import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Single-item cache with synchronous first load, async-only subsequent refreshes,
 * dual TTL (normal / degraded), and full stampede protection.
 *
 * <h3>TTL state machine</h3>
 * <pre>
 *  [NORMAL TTL] --1st refresh fail--> [DEGRADED TTL] --Nth fail (limit)--> [INVALIDATED]
 *       ^                                    |                                    |
 *       |________________refresh succeeds____|____________________________________|
 * </pre>
 *
 * <h3>Stampede / thundering-herd protection</h3>
 * <ul>
 *   <li><b>Cold-start coalescing</b> — an {@link AtomicReference} gate holds the
 *       in-flight {@link CompletableFuture} for the duration of a cold load. All
 *       concurrent callers join the same future instead of firing their own load.</li>
 *   <li><b>Failed-load backoff gate</b> — when a cold load fails (service down), the
 *       gate is held for {@code retryBackoff} before being cleared. This prevents a
 *       burst of requests from immediately fanning out into N serial retries against
 *       a service that is already unhealthy.</li>
 *   <li><b>Reload deduplication</b> — a flag prevents a second async reload from
 *       queuing up while one is already in flight.</li>
 *   <li><b>Isolated refresh executor</b> — a dedicated single-thread executor is used
 *       for all async reloads, keeping them off {@code ForkJoinPool.commonPool()} and
 *       preventing refresh starvation under application load.</li>
 * </ul>
 *
 * @param <V> the type of the cached value
 */
public class SingleItemCaffeine<V> {

    private static final String UNIT = "__unit__";

    private final int         maxConsecutiveFailures;
    private final long        normalTtlNanos;
    private final long        degradedTtlNanos;
    private final long        retryBackoffMs;
    private final Supplier<V> refresher;

    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);

    /**
     * Cold-start coalescing gate.
     *
     * Lifecycle:
     *   null             → no load in progress, gate is open
     *   non-null future  → a load is in progress; other callers join this future
     *
     * On success:  cleared immediately so the next expiry triggers a fresh load.
     * On failure:  held for {@code retryBackoffMs} via the refresh executor,
     *              so a burst of callers all get the empty result from the same
     *              future rather than each firing a new load attempt.
     */
    private final AtomicReference<CompletableFuture<V>> loadGate = new AtomicReference<>(null);

    /** Prevents a second asyncReload from queuing while one is already running. */
    private volatile boolean reloadInFlight = false;

    private final ScheduledExecutorService refreshExecutor;
    private final AsyncLoadingCache<String, V> cache;

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    private SingleItemCaffeine(Builder<V> b, Supplier<V> refresher) {
        this.maxConsecutiveFailures = b.maxConsecutiveFailures;
        this.normalTtlNanos         = b.normalTtl.toNanos();
        this.degradedTtlNanos       = b.degradedTtl.toNanos();
        this.retryBackoffMs         = b.retryBackoff.toMillis();
        this.refresher              = refresher;
        this.refreshExecutor        = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cache-refresh");
            t.setDaemon(true);
            return t;
        });

        this.cache = Caffeine.newBuilder()
            .maximumSize(1)
            .expireAfter(new Expiry<String, V>() {
                @Override
                public long expireAfterCreate(String k, V v, long now) {
                    return currentTtlNanos();
                }
                @Override
                public long expireAfterUpdate(String k, V v, long now, long currentDuration) {
                    // Called after every asyncReload — reads failure count to pick TTL.
                    return currentTtlNanos();
                }
                @Override
                public long expireAfterRead(String k, V v, long now, long currentDuration) {
                    return currentDuration;  // reads never affect expiry
                }
            })
            .refreshAfterWrite(b.normalTtl)
            .executor(refreshExecutor)
            .buildAsync(new AsyncCacheLoader<>() {

                /**
                 * Cold-start load. Coalesces concurrent callers onto a single future.
                 * On failure, holds the gate for retryBackoff to throttle retries.
                 */
                @Override
                public CompletableFuture<V> asyncLoad(String key, Executor ex) {
                    // Check if a load is already in flight
                    CompletableFuture<V> existing = loadGate.get();
                    if (existing != null) return existing;

                    CompletableFuture<V> myFuture = new CompletableFuture<>();
                    if (!loadGate.compareAndSet(null, myFuture)) {
                        return loadGate.get();  // another thread won the CAS
                    }

                    ex.execute(() -> {
                        try {
                            V v = safeLoad();
                            if (v != null) {
                                consecutiveFailures.set(0);
                                myFuture.complete(v);
                                loadGate.set(null);  // success — open gate immediately
                            } else {
                                myFuture.complete(null);
                                // Hold gate for retryBackoff — throttles stampede on failure
                                ((ScheduledExecutorService) ex)
                                    .schedule(() -> loadGate.set(null),
                                              retryBackoffMs, TimeUnit.MILLISECONDS);
                            }
                        } catch (Throwable t) {
                            myFuture.completeExceptionally(t);
                            ((ScheduledExecutorService) ex)
                                .schedule(() -> loadGate.set(null),
                                          retryBackoffMs, TimeUnit.MILLISECONDS);
                        }
                    });
                    return myFuture;
                }

                /**
                 * Async refresh — deduplicated and isolated to the refresh executor.
                 * Old value is served to all callers while this runs.
                 */
                @Override
                public CompletableFuture<V> asyncReload(String key, V oldValue, Executor ex) {
                    synchronized (SingleItemCaffeine.this) {
                        if (reloadInFlight) return CompletableFuture.completedFuture(oldValue);
                        reloadInFlight = true;
                    }

                    CompletableFuture<V> future = new CompletableFuture<>();
                    ex.execute(() -> {
                        try {
                            V fresh = safeLoad();
                            if (fresh != null) {
                                consecutiveFailures.set(0);
                                future.complete(fresh);
                            } else {
                                int f = consecutiveFailures.incrementAndGet();
                                if (f >= maxConsecutiveFailures) {
                                    consecutiveFailures.set(0);
                                    future.complete(oldValue);
                                    cache.synchronous().invalidate(UNIT);
                                } else {
                                    future.complete(oldValue);
                                }
                            }
                        } catch (Throwable t) {
                            future.complete(oldValue);
                        } finally {
                            synchronized (SingleItemCaffeine.this) {
                                reloadInFlight = false;
                            }
                        }
                    });
                    return future;
                }
            });
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Returns the cached value.
     * Blocks only on cold start. All subsequent calls return instantly.
     */
    public Optional<V> get() {
        try {
            return Optional.ofNullable(cache.get(UNIT).get());
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    /** Returns the cached value without triggering a load. */
    public Optional<V> peek() {
        CompletableFuture<V> f = cache.getIfPresent(UNIT);
        if (f == null || !f.isDone() || f.isCompletedExceptionally()) return Optional.empty();
        try { return Optional.ofNullable(f.getNow(null)); } catch (Exception e) { return Optional.empty(); }
    }

    /** Current TTL mode — NORMAL while healthy, DEGRADED after first refresh failure. */
    public TtlMode ttlMode() {
        return consecutiveFailures.get() == 0 ? TtlMode.NORMAL : TtlMode.DEGRADED;
    }

    /** Number of consecutive refresh failures since the last successful load. */
    public int consecutiveFailures() { return consecutiveFailures.get(); }

    /** {@code true} if serving a stale value (at least one refresh has failed). */
    public boolean isStale() { return consecutiveFailures.get() > 0 && peek().isPresent(); }

    /** Evicts immediately. Next {@link #get()} triggers a synchronous cold load. */
    public void invalidate() {
        cache.synchronous().invalidate(UNIT);
        consecutiveFailures.set(0);
        loadGate.set(null);
    }

    /** Forces an immediate async refresh outside the normal schedule. */
    public void refresh() { cache.synchronous().refresh(UNIT); }

    /** Shuts down the refresh executor. Call when the cache is no longer needed. */
    public void close() { refreshExecutor.shutdownNow(); }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private long currentTtlNanos() {
        return consecutiveFailures.get() == 0 ? normalTtlNanos : degradedTtlNanos;
    }

    private V safeLoad() {
        try { return refresher.get(); } catch (Exception ignored) { return null; }
    }

    // -------------------------------------------------------------------------
    // Types
    // -------------------------------------------------------------------------

    public enum TtlMode { NORMAL, DEGRADED }

    // -------------------------------------------------------------------------
    // Builder
    // -------------------------------------------------------------------------

    public static <V> Builder<V> builder() { return new Builder<>(); }

    public static final class Builder<V> {
        private Duration normalTtl             = Duration.ofMinutes(5);
        private Duration degradedTtl           = Duration.ofMinutes(1);
        private Duration retryBackoff          = Duration.ofSeconds(5);
        private int      maxConsecutiveFailures = 3;

        /** TTL applied while refreshes succeed. */
        public Builder<V> normalTtl(Duration ttl) {
            if (ttl == null || ttl.isNegative() || ttl.isZero())
                throw new IllegalArgumentException("normalTtl must be positive");
            this.normalTtl = ttl;
            return this;
        }

        /**
         * Shorter TTL applied from the first refresh failure.
         * Must be strictly less than {@code normalTtl}.
         */
        public Builder<V> degradedTtl(Duration ttl) {
            if (ttl == null || ttl.isNegative() || ttl.isZero())
                throw new IllegalArgumentException("degradedTtl must be positive");
            this.degradedTtl = ttl;
            return this;
        }

        /**
         * How long to hold the cold-start gate after a failed load before allowing
         * a new attempt. Prevents a burst of callers from fanning out into serial
         * retries against a down service. Defaults to 5 seconds.
         */
        public Builder<V> retryBackoff(Duration backoff) {
            if (backoff == null || backoff.isNegative() || backoff.isZero())
                throw new IllegalArgumentException("retryBackoff must be positive");
            this.retryBackoff = backoff;
            return this;
        }

        /** How many consecutive failures before the entry is invalidated. Must be >= 1. */
        public Builder<V> maxConsecutiveFailures(int max) {
            if (max < 1) throw new IllegalArgumentException("maxConsecutiveFailures must be >= 1");
            this.maxConsecutiveFailures = max;
            return this;
        }

        public SingleItemCaffeine<V> build(Supplier<V> refresher) {
            if (!degradedTtl.minus(normalTtl).isNegative())
                throw new IllegalArgumentException(
                    "degradedTtl (" + degradedTtl + ") must be shorter than normalTtl (" + normalTtl + ")");
            return new SingleItemCaffeine<>(this, refresher);
        }
    }
}
