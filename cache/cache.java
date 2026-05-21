import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.function.Supplier;

/**
 * Caches a single Key Encryption Key (KEK) with bounded retry semantics.
 *
 * <h2>Lifecycle</h2>
 * <pre>
 *  HEALTHY   ── refresh fails ──→ RETRYING ── N consecutive fails ──→ FAILED
 *     ↑                              │                                   │
 *     └──── refresh succeeds ────────┴───────────────────────────────────┘
 * </pre>
 *
 * <ul>
 *   <li><b>HEALTHY</b>  — Latest value cached, refreshing every {@code normalTtl}.</li>
 *   <li><b>RETRYING</b> — Last refresh failed. Cached value still served; scheduler
 *       retries every {@code retryInterval}.</li>
 *   <li><b>FAILED</b>   — {@code maxConsecutiveFailures} reached. {@link #get()} throws
 *       {@link KekRefreshException}. The scheduler continues retrying — a successful
 *       refresh transitions back to HEALTHY.</li>
 * </ul>
 *
 * <h2>Design</h2>
 * <ul>
 *   <li><b>Caffeine</b> stores the value and coalesces concurrent cold-start callers.
 *       Used as a plain {@code AsyncLoadingCache} — no expiry, no refresh policy.</li>
 *   <li><b>Scheduler</b> is the sole writer to all mutable state. Both refresh logic
 *       (from scheduled tasks) and cold-start bookkeeping (from {@code asyncLoad}
 *       handing off via {@code scheduler.execute}) run on this single thread.</li>
 *   <li><b>{@code failures}</b> is the only field read across threads: written on the
 *       scheduler thread, read by {@code get()} from caller threads. Marked
 *       {@code volatile} for visibility. The read-modify-write {@code failures++} is
 *       safe because all writes serialise onto the scheduler thread.</li>
 *   <li><b>{@code pending}</b> is read and written only on the scheduler thread, so
 *       it requires no volatile. Happens-before is provided by the executor.</li>
 * </ul>
 *
 * <h2>Approximate threshold detection</h2>
 * {@code failures} is updated by the scheduler thread, but reads from {@code get()}
 * are not synchronised with those updates. As a result the threshold check has two
 * forms of imprecision:
 *
 * <ol>
 *   <li><b>One-cycle lag near the threshold.</b> A caller may read {@code failures}
 *       just before the scheduler increments past the threshold, pass the check, and
 *       receive the stale value once more. With {@code retryInterval} on the order of
 *       minutes or hours this is negligible.</li>
 *   <li><b>Cold-start writes are eventually consistent.</b> When {@code asyncLoad}
 *       finishes (success or failure) it hands off to the scheduler via
 *       {@code scheduler.execute}. The {@code failures} update therefore happens
 *       <em>after</em> the load result has already been returned to the calling
 *       thread. Concurrent cold-start callers may each fire a fresh load before the
 *       counter catches up. In practice the scheduler retry interval dominates the
 *       call rate, so the loader is invoked rarely after the initial period.</li>
 * </ol>
 *
 * Once {@code failures >= maxConsecutiveFailures} is observed, {@code get()}
 * short-circuits with {@link KekRefreshException} and the loader is no longer
 * invoked from the {@code get()} path. The scheduler continues retrying in the
 * background — this is what allows recovery.
 *
 * <h2>Shutdown</h2>
 * {@link #close()} requests an orderly shutdown and waits up to 5 seconds for any
 * in-flight load to complete. After the timeout {@link ExecutorService#shutdownNow()}
 * is invoked, which interrupts the executing loader thread. Loaders that do not
 * respect interrupts may continue running until their own I/O completes.
 */
public class KekCache<V> implements AutoCloseable {

    private static final String KEY = "kek";

    private final Supplier<V>                  loader;
    private final long                         normalTtlMs;
    private final long                         retryIntervalMs;
    private final int                          maxConsecutiveFailures;
    private final AsyncLoadingCache<String, V> cache;
    private final ScheduledExecutorService     scheduler;

    /**
     * Written exclusively on the scheduler thread (whether from {@link #refresh()}
     * or from the cold-start hand-off via {@code scheduler.execute}). Read from any
     * thread via {@link #get()}; {@code volatile} guarantees visibility.
     *
     * <p>The read-modify-write {@code failures++} is safe because the scheduler is
     * single-threaded — no two increments ever overlap.
     */
    private volatile int failures = 0;

    /**
     * Scheduler-thread-only. Tracks the next scheduled refresh so it can be cancelled
     * before scheduling a replacement. Needs no {@code volatile}: all accesses are on
     * the scheduler thread, and the executor provides happens-before between submissions.
     */
    private ScheduledFuture<?> pending;

    // -------------------------------------------------------------------------

    private KekCache(Builder<V> b) {
        this.loader                 = b.loader;
        this.normalTtlMs            = b.normalTtl.toMillis();
        this.retryIntervalMs        = b.retryInterval.toMillis();
        this.maxConsecutiveFailures = b.maxConsecutiveFailures;

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "kek-refresh");
            t.setDaemon(true);
            return t;
        });

        this.cache = Caffeine.newBuilder()
            .maximumSize(1)
            // asyncLoad: thin initial fetch. On either outcome we hand off to the
            // scheduler thread, which then owns the refresh cycle exclusively.
            .buildAsync((key, executor) ->
                CompletableFuture.supplyAsync(() -> {
                    Throwable failure = null;
                    V v = null;
                    try { v = loader.get(); }
                    catch (Exception e) { failure = e; }

                    final boolean ok = (v != null);

                    // Hand off to the scheduler — this is the ONLY place the refresh
                    // cycle begins. Note: this happens asynchronously with respect to
                    // the caller; see "Approximate threshold detection" in the class docs.
                    scheduler.execute(() -> {
                        if (ok) {
                            failures = 0;
                            scheduleNext(normalTtlMs);
                        } else {
                            failures++;
                            scheduleNext(retryIntervalMs);
                        }
                    });

                    if (failure != null) throw new CompletionException(failure);
                    return v;
                }, executor)
            );
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Returns the cached KEK.
     *
     * <p>Blocks only on the very first call (cold start). Concurrent cold callers are
     * coalesced by Caffeine onto a single load. Subsequent calls return instantly.
     *
     * <p><b>Behaviour during outages:</b>
     * <ul>
     *   <li>Initial load failure → {@code KekRefreshException} wrapping the cause.</li>
     *   <li>HEALTHY/RETRYING → cached value returned. The scheduler may not yet have
     *       observed the latest refresh outcome (see class docs).</li>
     *   <li>FAILED ({@code failures >= maxConsecutiveFailures}) → {@code KekRefreshException}.
     *       No stale value is returned; callers must treat this as an outage.</li>
     * </ul>
     *
     * @throws KekRefreshException if the cache is in FAILED state, or if the
     *         cold-start load fails. Carries no value.
     */
    public V get() {
        int snapshot = failures;
        if (snapshot >= maxConsecutiveFailures) {
            throw new KekRefreshException(
                "KEK refresh has failed " + snapshot + " consecutive times");
        }
        try {
            return cache.get(KEY).join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new KekRefreshException("Initial KEK load failed", cause);
        }
    }

    /**
     * Requests an orderly shutdown of the refresh scheduler.
     *
     * <p>Waits up to 5 seconds for any in-flight load to complete. After the timeout
     * the scheduler is interrupted via {@code shutdownNow()}. Loaders that do not
     * respect interrupts may continue running until their I/O completes.
     */
    @Override
    public void close() throws InterruptedException {
        scheduler.shutdown();
        if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
            scheduler.shutdownNow();
        }
    }

    // -------------------------------------------------------------------------
    // Scheduler thread — sole writer
    // -------------------------------------------------------------------------

    /** Cancel any pending refresh and schedule a new one. Runs on the scheduler thread. */
    private void scheduleNext(long delayMs) {
        if (pending != null) pending.cancel(false);
        try {
            pending = scheduler.schedule(this::refresh, delayMs, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ignored) {
            pending = null;   // scheduler shut down — let the cycle end naturally
        }
    }

    /**
     * Runs on the scheduler thread.
     *
     * <p>The {@code finally} block structurally guarantees the refresh cycle never
     * dies silently: every exit path reschedules, regardless of whether the loader
     * returned normally, returned null, or threw.
     */
    private void refresh() {
        boolean succeeded = false;
        try {
            V fresh = loader.get();
            if (fresh != null) {
                cache.synchronous().put(KEY, fresh);
                failures  = 0;
                succeeded = true;
            } else {
                failures++;
            }
        } catch (Exception e) {
            failures++;
        } finally {
            scheduleNext(succeeded ? normalTtlMs : retryIntervalMs);
        }
    }

    // -------------------------------------------------------------------------
    // Exception
    // -------------------------------------------------------------------------

    /**
     * Thrown by {@link #get()} when the cache is in FAILED state or when the
     * cold-start load fails. Carries no value — callers must treat this as an
     * outage condition requiring operator attention.
     */
    public static class KekRefreshException extends RuntimeException {
        public KekRefreshException(String message)              { super(message); }
        public KekRefreshException(String message, Throwable c) { super(message, c); }
    }

    // -------------------------------------------------------------------------
    // Builder
    // -------------------------------------------------------------------------

    public static <V> Builder<V> builder() { return new Builder<>(); }

    public static final class Builder<V> {
        private Supplier<V> loader;
        private Duration    normalTtl              = Duration.ofDays(30);
        private Duration    retryInterval          = Duration.ofHours(1);
        private int         maxConsecutiveFailures = 3;

        public Builder<V> loader(Supplier<V> l)             { this.loader = l; return this; }
        public Builder<V> normalTtl(Duration d)             { this.normalTtl = d; return this; }
        public Builder<V> retryInterval(Duration d)         { this.retryInterval = d; return this; }
        public Builder<V> maxConsecutiveFailures(int n)     { this.maxConsecutiveFailures = n; return this; }

        public KekCache<V> build() {
            if (loader == null)
                throw new IllegalStateException("loader is required");
            if (normalTtl.toMillis() < 1 || retryInterval.toMillis() < 1)
                throw new IllegalArgumentException("durations must be at least 1ms");
            if (!retryInterval.minus(normalTtl).isNegative())
                throw new IllegalArgumentException("retryInterval should be shorter than normalTtl");
            if (maxConsecutiveFailures < 1)
                throw new IllegalArgumentException("maxConsecutiveFailures must be >= 1");
            return new KekCache<>(this);
        }
    }
}
