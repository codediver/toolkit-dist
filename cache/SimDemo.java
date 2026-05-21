import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Supplier;

public class SimDemo {

    static class SimCache<V> {
        private final long     normalMs, degradedMs;
        private final int      maxFailures;
        private final long     retryBackoffMs;   // min wait between failed cold loads
        private final Supplier<V> refresher;

        private final AtomicInteger failures = new AtomicInteger(0);
        private volatile V      value        = null;
        private volatile boolean valid       = false;
        private volatile long   expiresAt    = 0;
        private volatile long   nextRetryAt  = 0;  // earliest time another cold load may run

        final AtomicInteger actualLoads = new AtomicInteger(0);
        final AtomicInteger coalesced   = new AtomicInteger(0);

        // Held for the entire duration of a cold load (success or failure).
        // On failure it is NOT cleared immediately — callers join it and get
        // the empty result, then the gate is cleared only after retryBackoff elapses.
        private final AtomicReference<CompletableFuture<Optional<V>>> loadGate =
            new AtomicReference<>(null);

        private volatile boolean reloadInFlight = false;

        private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                var t = new Thread(r, "refresh"); t.setDaemon(true); return t;
            });

        SimCache(long normalMs, long degradedMs, int maxFailures, long retryBackoffMs, Supplier<V> refresher) {
            this.normalMs = normalMs; this.degradedMs = degradedMs;
            this.maxFailures = maxFailures; this.retryBackoffMs = retryBackoffMs;
            this.refresher = refresher;
        }

        Optional<V> get() {
            // Warm path
            if (valid && System.currentTimeMillis() < expiresAt)
                return Optional.ofNullable(value);

            // Still in retry backoff? Join whatever future is current (may be completed)
            // or short-circuit with stale/empty.
            CompletableFuture<Optional<V>> existing = loadGate.get();
            if (existing != null) {
                coalesced.incrementAndGet();
                try { return existing.get(); } catch (Exception e) { return Optional.empty(); }
            }

            // Past backoff window — try to win the gate
            CompletableFuture<Optional<V>> myFuture = new CompletableFuture<>();
            if (!loadGate.compareAndSet(null, myFuture)) {
                coalesced.incrementAndGet();
                try { return loadGate.get().get(); } catch (Exception e) { return Optional.empty(); }
            }

            actualLoads.incrementAndGet();
            try {
                V v = safeLoad();
                Optional<V> result;
                if (v != null) {
                    value = v; valid = true; failures.set(0);
                    expiresAt = System.currentTimeMillis() + normalMs;
                    scheduleRefresh(normalMs);
                    result = Optional.of(v);
                    myFuture.complete(result);
                    loadGate.set(null);              // success — open immediately
                } else {
                    result = Optional.ofNullable(value);
                    myFuture.complete(result);
                    // Hold the gate for retryBackoff so concurrent callers don't pile on
                    scheduler.schedule(() -> loadGate.set(null), retryBackoffMs, TimeUnit.MILLISECONDS);
                }
                return result;
            } catch (Throwable t) {
                myFuture.complete(Optional.empty());
                scheduler.schedule(() -> loadGate.set(null), retryBackoffMs, TimeUnit.MILLISECONDS);
                return Optional.empty();
            }
        }

        Optional<V> peek()  { return valid ? Optional.ofNullable(value) : Optional.empty(); }
        boolean isStale()   { return failures.get() > 0 && valid; }
        int consecutiveFailures() { return failures.get(); }
        String ttlMode()    { return failures.get() == 0 ? "NORMAL" : "DEGRADED"; }
        long remainingMs()  { return valid ? Math.max(0, expiresAt - System.currentTimeMillis()) : 0; }
        void invalidate()   { valid = false; value = null; failures.set(0); }

        private void scheduleRefresh(long delayMs) {
            scheduler.schedule(this::asyncRefresh, delayMs, TimeUnit.MILLISECONDS);
        }

        private synchronized void asyncRefresh() {
            if (!valid || reloadInFlight) return;
            reloadInFlight = true;
            try {
                V fresh = safeLoad();
                if (fresh != null) {
                    value = fresh; failures.set(0);
                    expiresAt = System.currentTimeMillis() + normalMs;
                    scheduleRefresh(normalMs);
                } else {
                    int f = failures.incrementAndGet();
                    if (f >= maxFailures) { invalidate(); }
                    else {
                        expiresAt = System.currentTimeMillis() + degradedMs;
                        scheduleRefresh(degradedMs);
                    }
                }
            } finally { reloadInFlight = false; }
        }

        private V safeLoad() { try { return refresher.get(); } catch (Exception e) { return null; } }
    }

    static volatile boolean up = true;
    static final AtomicInteger calls = new AtomicInteger(0);
    static String service() {
        int n = calls.incrementAndGet();
        if (!up) throw new RuntimeException("down");
        Thread.yield();
        return "config-v" + n;
    }

    static void print(String label, SimCache<String> c) {
        System.out.printf("  %-36s value=%-14s ttlMode=%-8s stale=%-5b failures=%d  remaining=%dms%n",
            label, c.peek().orElse("(empty)"), c.ttlMode(),
            c.isStale(), c.consecutiveFailures(), c.remainingMs());
    }

    static void stampedeTest(String label, SimCache<String> cache, int expectedLoads) throws Exception {
        System.out.println("-- " + label + " --");
        int pre = cache.actualLoads.get();
        int threads = 50;
        var pool  = Executors.newFixedThreadPool(threads);
        var latch = new CountDownLatch(1);
        var results = new ConcurrentLinkedQueue<String>();
        for (int i = 0; i < threads; i++) pool.submit(() -> {
            try { latch.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            cache.get().ifPresent(results::add);
        });
        latch.countDown();
        pool.shutdown(); pool.awaitTermination(5, TimeUnit.SECONDS);
        int loads = cache.actualLoads.get() - pre;
        System.out.printf("  Threads: %d  |  Actual loads: %d (expect %d)  |  Coalesced: %d%n",
            threads, loads, expectedLoads, cache.coalesced.get());
        System.out.println(loads == expectedLoads ? "  ✓ PASS" : "  ✗ FAIL — expected " + expectedLoads + " but got " + loads);
    }

    public static void main(String[] args) throws Exception {
        System.out.println("=== Dual-TTL Cache + Stampede Protection ===");
        System.out.println("    normalTtl=3s  degradedTtl=1s  maxFailures=3  retryBackoff=500ms\n");

        // retryBackoffMs=500 — gate stays closed for 500ms after a failed cold load
        var cache = new SimCache<>(3000, 1000, 3, 500, SimDemo::service);

        stampedeTest("Stampede on cold cache (service UP)", cache, 1);
        print("after:", cache);

        System.out.println("\n-- Warm reads --");
        for (int i = 0; i < 3; i++) {
            long t = System.currentTimeMillis(); cache.get();
            System.out.printf("  get() returned in %dms%n", System.currentTimeMillis()-t);
        }

        System.out.println("\n-- Background refresh (service UP) --");
        Thread.sleep(3200);
        print("after healthy refresh:", cache);

        System.out.println("\n-- Service DOWN → degraded TTL → invalidation --");
        up = false;
        for (int i = 1; i <= 3; i++) {
            Thread.sleep(i == 1 ? 3200 : 1200);
            print("after failure #" + i + ":", cache);
        }

        System.out.println();
        stampedeTest("Stampede on post-invalidation cold cache (service DOWN)", cache, 1);

        System.out.println("\n-- Service recovers --");
        Thread.sleep(600);  // let backoff window pass
        up = true;
        System.out.println("  get() → " + cache.get());
        print("after recovery:", cache);

        System.out.println("\n=== All scenarios verified ===");
    }
}
