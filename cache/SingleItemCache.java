import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Single-item cache with fast reads and a single writer thread.
 *
 * <h3>Design</h3>
 * <ul>
 *   <li><b>Hot path</b> — one volatile read of an immutable {@link Snapshot}. No locks,
 *       no threads, no allocations. ~1µs on modern hardware.</li>
 *   <li><b>Cold path</b> — callers submit a load task to the single-thread scheduler and
 *       wait on a shared Future. Only one load ever runs at a time.</li>
 *   <li><b>Writes</b> — exclusively on the scheduler thread. Plain field access,
 *       no synchronization needed.</li>
 * </ul>
 *
 * <h3>TTL state machine</h3>
 * <pre>
 *  [NORMAL TTL] --1st refresh fail--> [DEGRADED TTL] --Nth fail--> [INVALIDATED + backoff]
 *       ^                                    |                             |
 *       |_______________refresh succeeds_____|_____________________________|
 *                                      (caller-driven via cold path)
 * </pre>
 *
 * <h3>Stampede protection</h3>
 * <ul>
 *   <li>Cold-start: {@code loadFuture} AtomicReference ensures only one load task is
 *       submitted; all concurrent callers join the same Future.</li>
 *   <li>Post-invalidation: {@code retryAfter} timestamp is set on every failure so
 *       callers return immediately without triggering a load until the backoff elapses.</li>
 *   <li>Background refresh: {@code pendingRefresh} ScheduledFuture is cancelled when
 *       max failures are reached, stopping the refresh cycle until a caller re-enters
 *       the cold path after the backoff window.</li>
 * </ul>
 */
public class SingleItemCache<V> {

    // Immutable — one volatile write makes all fields atomically visible to readers
    record Snapshot<V>(V value, long expiresAt, long retryAfter, int failures) {
        static <V> Snapshot<V> empty() { return new Snapshot<>(null, 0, 0, 0); }
        boolean isWarm()    { return value != null && System.currentTimeMillis() < expiresAt; }
        boolean inBackoff() { return System.currentTimeMillis() < retryAfter; }
        long remainingMs()  { return value != null ? Math.max(0, expiresAt - System.currentTimeMillis()) : 0; }
    }

    private final Supplier<V>                loader;
    private final long                        normalTtlMs, degradedTtlMs, retryBackoffMs;
    private final int                         maxConsecutiveFailures;
    private volatile Snapshot<V>             snapshot    = Snapshot.empty();
    private final AtomicReference<Future<?>> loadFuture  = new AtomicReference<>(null);
    private ScheduledFuture<?>               pendingRefresh; // scheduler thread only

    private final ScheduledExecutorService scheduler =
        Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cache-refresh");
            t.setDaemon(true);
            return t;
        });

    private SingleItemCache(Builder<V> b) {
        this.loader                 = b.loader;
        this.normalTtlMs            = b.normalTtl.toMillis();
        this.degradedTtlMs          = b.degradedTtl.toMillis();
        this.retryBackoffMs         = b.retryBackoff.toMillis();
        this.maxConsecutiveFailures = b.maxConsecutiveFailures;
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Hot path: one volatile read — returns instantly when the entry is live.
     * Cold path: submits a load to the scheduler and waits — only on first call
     * or after the entry has expired and been invalidated.
     */
    public Optional<V> get() throws ExecutionException, InterruptedException {
        Snapshot<V> s = snapshot;
        if (s.isWarm()) return Optional.of(s.value());  // ← ~1µs, no threads
        return coldGet(s);
    }

    public Optional<V> peek()        { return Optional.ofNullable(snapshot.value()); }
    public boolean     isStale()     { Snapshot<V> s = snapshot; return s.failures() > 0 && s.value() != null; }
    public boolean     isNormal()    { return snapshot.failures() == 0; }
    public int         failures()    { return snapshot.failures(); }
    public long        remainingMs() { return snapshot.remainingMs(); }

    /** Evicts immediately. Next get() triggers a fresh cold load after any backoff. */
    public void invalidate() {
        snapshot = Snapshot.empty();
        scheduler.execute(() -> {
            if (pendingRefresh != null) { pendingRefresh.cancel(false); pendingRefresh = null; }
            loadFuture.set(null);
        });
    }

    public void close() { scheduler.shutdownNow(); }

    // -------------------------------------------------------------------------
    // Cold path
    // -------------------------------------------------------------------------

    private Optional<V> coldGet(Snapshot<V> s) throws ExecutionException, InterruptedException {
        if (s.inBackoff()) return Optional.ofNullable(s.value());

        // Submit a load only if none is already in flight
        Future<?> existing = loadFuture.get();
        if (existing == null) {
            Future<?> submitted = scheduler.submit(this::doLoad);
            if (!loadFuture.compareAndSet(null, submitted))
                submitted.cancel(false);  // lost the race — another thread won
        }

        Future<?> f = loadFuture.get();
        if (f != null) f.get();           // wait for the in-flight load to finish
        return Optional.ofNullable(snapshot.value());
    }

    // -------------------------------------------------------------------------
    // Scheduler thread only — no synchronization needed
    // -------------------------------------------------------------------------

    private void doLoad() {
        V fresh;
        try   { fresh = loader.get(); }
        catch (Exception e) { fresh = null; }

        long now  = System.currentTimeMillis();
        int  prev = snapshot.failures();

        if (fresh != null) {
            snapshot       = new Snapshot<>(fresh, now + normalTtlMs, 0, 0);
            loadFuture.set(null);
            pendingRefresh = scheduler.schedule(this::doLoad, normalTtlMs, TimeUnit.MILLISECONDS);
        } else {
            int f = prev + 1;
            if (f >= maxConsecutiveFailures) {
                // Stop the refresh cycle — next attempt is caller-driven after retryAfter
                snapshot       = new Snapshot<>(null, 0, now + retryBackoffMs, 0);
                loadFuture.set(null);
                pendingRefresh = null;
            } else {
                snapshot       = new Snapshot<>(snapshot.value(), now + degradedTtlMs, now + retryBackoffMs, f);
                loadFuture.set(null);
                pendingRefresh = scheduler.schedule(this::doLoad, degradedTtlMs, TimeUnit.MILLISECONDS);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Builder
    // -------------------------------------------------------------------------

    public enum TtlMode { NORMAL, DEGRADED }

    public static <V> Builder<V> builder() { return new Builder<>(); }

    public static final class Builder<V> {
        private Supplier<V> loader;
        private Duration    normalTtl             = Duration.ofMinutes(5);
        private Duration    degradedTtl           = Duration.ofMinutes(1);
        private Duration    retryBackoff          = Duration.ofSeconds(5);
        private int         maxConsecutiveFailures = 3;

        public Builder<V> loader(Supplier<V> loader)            { this.loader = loader; return this; }
        public Builder<V> normalTtl(Duration d)                 { this.normalTtl = d; return this; }
        public Builder<V> degradedTtl(Duration d)               { this.degradedTtl = d; return this; }
        public Builder<V> retryBackoff(Duration d)              { this.retryBackoff = d; return this; }
        public Builder<V> maxConsecutiveFailures(int n)         { this.maxConsecutiveFailures = n; return this; }

        public SingleItemCache<V> build() {
            if (loader == null) throw new IllegalStateException("loader is required");
            if (!degradedTtl.minus(normalTtl).isNegative())
                throw new IllegalArgumentException("degradedTtl must be shorter than normalTtl");
            return new SingleItemCache<>(this);
        }
    }
}
