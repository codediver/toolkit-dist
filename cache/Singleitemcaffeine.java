import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Single-item cache with:
 * <ul>
 *   <li>Synchronous first load, async-only subsequent refreshes.</li>
 *   <li>Normal TTL while refreshes succeed.</li>
 *   <li>Shortened TTL from the first refresh failure — so a degraded entry
 *       expires sooner and forces a fresh cold-start sooner.</li>
 *   <li>Automatic invalidation after {@code maxConsecutiveFailures}.</li>
 * </ul>
 *
 * <h3>TTL state machine</h3>
 * <pre>
 *  [NORMAL TTL] --refresh fails--> [SHORT TTL] --refresh fails (limit hit)--> [INVALIDATED]
 *                                       ^                                           |
 *                                       |___________refresh succeeds_______________|
 *                                       (also resets from any state back to NORMAL)
 * </pre>
 *
 * <h3>How variable TTL is implemented</h3>
 * Caffeine's {@link Expiry} interface is used so each entry carries its own expiry.
 * {@code expireAfterCreate} and {@code expireAfterUpdate} read the current
 * {@code consecutiveFailures} counter to decide which TTL to return:
 * <ul>
 *   <li>0 failures → {@code normalTtl}</li>
 *   <li>≥ 1 failure → {@code degradedTtl}</li>
 * </ul>
 * {@code expireAfterRead} always returns {@code currentDurationNanos} unchanged —
 * reads never affect the TTL.
 *
 * <h3>Example</h3>
 * <pre>{@code
 * SingleItemCaffeine<Config> cache = SingleItemCaffeine.<Config>builder()
 *     .normalTtl(Duration.ofMinutes(10))
 *     .degradedTtl(Duration.ofMinutes(2))   // shorter window while service is flaky
 *     .maxConsecutiveFailures(3)
 *     .build(() -> configService.load());
 *
 * Config cfg = cache.get().orElseThrow();   // blocks only on cold start
 * }</pre>
 *
 * @param <V> the type of the cached value
 */
public class SingleItemCaffeine<V> {

    private static final String UNIT = "__unit__";

    private final int             maxConsecutiveFailures;
    private final Supplier<V>     refresher;
    private final AtomicInteger   consecutiveFailures = new AtomicInteger(0);

    // Both TTLs stored so the Expiry callbacks can read them
    private final long normalTtlNanos;
    private final long degradedTtlNanos;

    private final AsyncLoadingCache<String, V> cache;

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    private SingleItemCaffeine(Builder<V> b, Supplier<V> refresher) {
        this.maxConsecutiveFailures = b.maxConsecutiveFailures;
        this.refresher              = refresher;
        this.normalTtlNanos         = b.normalTtl.toNanos();
        this.degradedTtlNanos       = b.degradedTtl.toNanos();

        this.cache = Caffeine.newBuilder()
            .maximumSize(1)
            /*
             * Variable expiry: returns normalTtl when healthy, degradedTtl once
             * the first refresh failure has been recorded.
             *
             * expireAfterCreate  — first population (cold start)
             * expireAfterUpdate  — called after every asyncReload() completes,
             *                      whether the value changed or not. This is the
             *                      hook that switches between normal and degraded TTL.
             * expireAfterRead    — plain get(); never alters expiry.
             */
            .expireAfter(new Expiry<String, V>() {
                @Override
                public long expireAfterCreate(String k, V v, long now) {
                    return currentTtlNanos();
                }
                @Override
                public long expireAfterUpdate(String k, V v, long now, long currentDuration) {
                    return currentTtlNanos();
                }
                @Override
                public long expireAfterRead(String k, V v, long now, long currentDuration) {
                    return currentDuration;   // reads are invisible to expiry
                }
            })
            /*
             * refreshAfterWrite drives the async reload loop.
             * Must be <= normalTtl so Caffeine attempts a refresh before the entry
             * would naturally expire. We use normalTtl so the refresh fires exactly
             * when the healthy window closes.
             */
            .refreshAfterWrite(b.normalTtl)
            .executor(b.executor)
            .buildAsync(new AsyncCacheLoader<>() {

                /** Initial synchronous-style load on cache miss. */
                @Override
                public CompletableFuture<V> asyncLoad(String key, Executor ex) {
                    return CompletableFuture.supplyAsync(() -> {
                        V v = safeLoad();
                        if (v != null) consecutiveFailures.set(0);
                        return v;
                    }, ex);
                }

                /**
                 * Async refresh — called by Caffeine after refreshAfterWrite elapses.
                 * Old value is served to callers while this runs.
                 *
                 * On success  → reset failure counter (Expiry switches back to normalTtl)
                 * On failure  → increment counter    (Expiry switches to degradedTtl)
                 *               If limit reached, invalidate immediately.
                 */
                @Override
                public CompletableFuture<V> asyncReload(String key, V oldValue, Executor ex) {
                    return CompletableFuture.supplyAsync(() -> {
                        V fresh = safeLoad();

                        if (fresh != null) {
                            consecutiveFailures.set(0);   // healthy — normalTtl applied by expireAfterUpdate
                            return fresh;
                        }

                        int f = consecutiveFailures.incrementAndGet();
                        // expireAfterUpdate will now see f >= 1 and return degradedTtl

                        if (f >= maxConsecutiveFailures) {
                            consecutiveFailures.set(0);
                            cache.synchronous().invalidate(UNIT);
                        }

                        return oldValue;   // keep serving stale while we still can
                    }, ex);
                }
            });
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Returns the cached value.
     * Blocks only on the very first call (cold start). All subsequent calls return
     * instantly from the last good value while Caffeine refreshes in the background.
     */
    public Optional<V> get() {
        try {
            return Optional.ofNullable(cache.get(UNIT).get());
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    /**
     * Returns the cached value without triggering a load.
     */
    public Optional<V> peek() {
        CompletableFuture<V> f = cache.getIfPresent(UNIT);
        if (f == null || !f.isDone() || f.isCompletedExceptionally()) return Optional.empty();
        try { return Optional.ofNullable(f.getNow(null)); } catch (Exception e) { return Optional.empty(); }
    }

    /** Current TTL mode — NORMAL when healthy, DEGRADED after the first refresh failure. */
    public TtlMode ttlMode() {
        return consecutiveFailures.get() == 0 ? TtlMode.NORMAL : TtlMode.DEGRADED;
    }

    /** Number of consecutive refresh failures since the last successful load. */
    public int consecutiveFailures() { return consecutiveFailures.get(); }

    /** {@code true} if the entry exists but at least one refresh has failed. */
    public boolean isStale() { return consecutiveFailures.get() > 0 && peek().isPresent(); }

    /** Evicts immediately; next {@link #get()} triggers a synchronous cold load. */
    public void invalidate() {
        cache.synchronous().invalidate(UNIT);
        consecutiveFailures.set(0);
    }

    /** Forces an immediate async refresh outside the normal schedule. */
    public void refresh() { cache.synchronous().refresh(UNIT); }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Reads current failure count to decide which TTL the Expiry callbacks return. */
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
        private Duration normalTtl              = Duration.ofMinutes(5);
        private Duration degradedTtl            = Duration.ofMinutes(1);
        private int      maxConsecutiveFailures  = 3;
        private Executor executor               = ForkJoinPool.commonPool();

        /** TTL applied while refreshes are succeeding. */
        public Builder<V> normalTtl(Duration ttl) {
            if (ttl == null || ttl.isNegative() || ttl.isZero())
                throw new IllegalArgumentException("normalTtl must be positive");
            this.normalTtl = ttl;
            return this;
        }

        /**
         * Shorter TTL applied from the first refresh failure onward.
         * Must be less than {@code normalTtl} — enforced at build time.
         * A shorter window means a degraded entry expires sooner, prompting
         * an earlier cold-start retry.
         */
        public Builder<V> degradedTtl(Duration ttl) {
            if (ttl == null || ttl.isNegative() || ttl.isZero())
                throw new IllegalArgumentException("degradedTtl must be positive");
            this.degradedTtl = ttl;
            return this;
        }

        /**
         * How many consecutive background refresh failures to tolerate before
         * invalidating the entry. Must be >= 1.
         */
        public Builder<V> maxConsecutiveFailures(int max) {
            if (max < 1) throw new IllegalArgumentException("maxConsecutiveFailures must be >= 1");
            this.maxConsecutiveFailures = max;
            return this;
        }

        /**
         * Executor for async refreshes. Supply a dedicated I/O executor if
         * the refresher does blocking work.
         */
        public Builder<V> executor(Executor executor) {
            this.executor = executor;
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
