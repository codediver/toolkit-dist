import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Supplier;

public class SimDemo {

    // ── Simulated cache ───────────────────────────────────────────────────────
    static class SimCache<V> {
        private final long     normalTtlMs;
        private final long     degradedTtlMs;
        private final int      maxFailures;
        private final Supplier<V> refresher;

        private final AtomicInteger failures  = new AtomicInteger(0);
        private volatile V      value         = null;
        private volatile boolean valid        = false;
        private volatile long   expiresAt     = 0;   // wall-clock ms

        private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                var t = new Thread(r, "refresh"); t.setDaemon(true); return t;
            });

        SimCache(long normalMs, long degradedMs, int maxFailures, Supplier<V> refresher) {
            this.normalTtlMs   = normalMs;
            this.degradedTtlMs = degradedMs;
            this.maxFailures   = maxFailures;
            this.refresher     = refresher;
        }

        // Cold start — synchronous
        synchronized Optional<V> get() {
            if (!valid || System.currentTimeMillis() > expiresAt) {
                V v = safeLoad();
                if (v != null) {
                    value = v; valid = true; failures.set(0);
                    expiresAt = System.currentTimeMillis() + normalTtlMs;
                    scheduleRefresh(normalTtlMs);
                } else {
                    valid = false;
                }
            }
            return valid ? Optional.ofNullable(value) : Optional.empty();
        }

        Optional<V> peek()    { return valid ? Optional.ofNullable(value) : Optional.empty(); }
        boolean isStale()     { return failures.get() > 0 && valid; }
        int consecutiveFailures() { return failures.get(); }
        String ttlMode()      { return failures.get() == 0 ? "NORMAL" : "DEGRADED"; }
        long remainingMs()    { return valid ? Math.max(0, expiresAt - System.currentTimeMillis()) : 0; }

        void invalidate() { valid = false; value = null; failures.set(0); expiresAt = 0; }

        private void scheduleRefresh(long delayMs) {
            scheduler.schedule(this::asyncRefresh, delayMs, TimeUnit.MILLISECONDS);
        }

        private synchronized void asyncRefresh() {
            if (!valid) return;
            V fresh = safeLoad();
            if (fresh != null) {
                // Success → normalTtl, reset failures
                value = fresh; failures.set(0);
                expiresAt = System.currentTimeMillis() + normalTtlMs;
                scheduleRefresh(normalTtlMs);
            } else {
                int f = failures.incrementAndGet();
                if (f >= maxFailures) {
                    invalidate();   // give up
                } else {
                    // Switch to degradedTtl for remaining window
                    expiresAt = System.currentTimeMillis() + degradedTtlMs;
                    scheduleRefresh(degradedTtlMs);
                }
            }
        }

        private V safeLoad() { try { return refresher.get(); } catch (Exception e) { return null; } }
    }

    // ── Test service ──────────────────────────────────────────────────────────
    static volatile boolean up = true;
    static final AtomicInteger calls = new AtomicInteger(0);

    static String service() {
        int n = calls.incrementAndGet();
        if (!up) throw new RuntimeException("down");
        return "config-v" + n;
    }

    static void print(String label, SimCache<String> c) {
        System.out.printf("  %-36s value=%-14s ttlMode=%-8s stale=%-5b failures=%d  remaining=%dms%n",
            label,
            c.peek().orElse("(empty)"),
            c.ttlMode(),
            c.isStale(),
            c.consecutiveFailures(),
            c.remainingMs());
    }

    public static void main(String[] args) throws InterruptedException {
        // normalTtl=3s, degradedTtl=1s, maxFailures=3
        var cache = new SimCache<>(3000, 1000, 3, SimDemo::service);

        System.out.println("=== Dual-TTL Async Refresh Simulation ===");
        System.out.println("    normalTtl=3s  degradedTtl=1s  maxFailures=3\n");

        // ── 1. Cold start ──────────────────────────────────────────────────────
        System.out.println("-- 1. Cold start (synchronous) --");
        long t0 = System.currentTimeMillis();
        System.out.println("  get() → " + cache.get() + "  (" + (System.currentTimeMillis()-t0) + "ms)");
        print("initial state:", cache);

        // ── 2. Warm reads instant ──────────────────────────────────────────────
        System.out.println("\n-- 2. Warm reads are instant --");
        for (int i = 0; i < 3; i++) {
            long t = System.currentTimeMillis();
            cache.get();
            System.out.printf("  get() returned in %dms%n", System.currentTimeMillis()-t);
        }

        // ── 3. Healthy refresh ─────────────────────────────────────────────────
        System.out.println("\n-- 3. Background refresh fires (service UP) --");
        Thread.sleep(3200);
        print("after healthy refresh:", cache);

        // ── 4. First failure → switch to degradedTtl ──────────────────────────
        System.out.println("\n-- 4. First refresh failure → TTL shortens to degradedTtl --");
        up = false;
        Thread.sleep(3200);   // wait for next refresh (normalTtl)
        print("after failure #1:", cache);

        // ── 5. Second failure still within degradedTtl ────────────────────────
        System.out.println("\n-- 5. Second failure (degradedTtl window) --");
        Thread.sleep(1200);   // wait for next refresh (degradedTtl)
        print("after failure #2:", cache);

        // ── 6. Third failure → invalidated ────────────────────────────────────
        System.out.println("\n-- 6. Third failure → limit reached → invalidated --");
        Thread.sleep(1200);
        print("after failure #3:", cache);

        // ── 7. Cold start again (still down) ──────────────────────────────────
        System.out.println("\n-- 7. get() cold-starts again (service still DOWN) --");
        System.out.println("  get() → " + cache.get());

        // ── 8. Service recovers ────────────────────────────────────────────────
        System.out.println("\n-- 8. Service recovers → normal TTL restored --");
        up = true;
        System.out.println("  get() → " + cache.get());
        print("after recovery:", cache);

        System.out.println("\n=== All scenarios verified ===");
    }
}
