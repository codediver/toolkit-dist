import kotlinx.coroutines.*
import kotlinx.coroutines.test.*
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.*

/**
 * Tests for SingleItemCache.
 * Uses kotlinx-coroutines-test for virtual time — no real sleeps needed.
 *
 * Dependencies (Gradle):
 *   implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.0")
 *   testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.8.0")
 *   testImplementation("org.jetbrains.kotlin:kotlin-test")
 */
class SingleItemCacheTest {

    // ── 1. Cold start ─────────────────────────────────────────────────────────

    @Test
    fun `cold start loads value synchronously`() = runTest {
        val cache = SingleItemCache.build<String>(
            loader      = { "hello" },
            scope       = this,
            normalTtl   = Duration.ofSeconds(10),
            degradedTtl = Duration.ofSeconds(2),
        )
        assertEquals("hello", cache.get())
        assertEquals(SingleItemCache.TtlMode.NORMAL, cache.ttlMode)
        assertEquals(0, cache.failures)
    }

    // ── 2. Warm reads are instant (no loader call) ────────────────────────────

    @Test
    fun `warm reads do not call loader`() = runTest {
        val calls = AtomicInteger(0)
        val cache = SingleItemCache.build<String>(
            loader      = { calls.incrementAndGet(); "v1" },
            scope       = this,
            normalTtl   = Duration.ofSeconds(10),
            degradedTtl = Duration.ofSeconds(2),
        )
        cache.get()
        repeat(10) { cache.get() }
        assertEquals(1, calls.get(), "loader should be called exactly once")
    }

    // ── 3. Stampede — concurrent cold-start callers fire one load ─────────────

    @Test
    fun `concurrent cold-start callers coalesce into one load`() = runTest {
        val loadCount = AtomicInteger(0)
        val cache = SingleItemCache.build<String>(
            loader      = { loadCount.incrementAndGet(); delay(50); "v1" },
            scope       = this,
            normalTtl   = Duration.ofSeconds(10),
            degradedTtl = Duration.ofSeconds(2),
        )
        // Launch 50 coroutines simultaneously
        val results = (1..50).map {
            async { cache.get() }
        }.awaitAll()

        assertEquals(1, loadCount.get(), "only one load should fire")
        assertTrue(results.all { it == "v1" }, "all callers should get the same value")
    }

    // ── 4. Background refresh on normal TTL ───────────────────────────────────

    @Test
    fun `background refresh fires after normalTtl`() = runTest {
        var callCount = 0
        val cache = SingleItemCache.build<String>(
            loader      = { "v${++callCount}" },
            scope       = this,
            normalTtl   = Duration.ofSeconds(5),
            degradedTtl = Duration.ofSeconds(2),
        )
        cache.get()                      // cold start → "v1"
        advanceTimeBy(5_001)             // past normalTtl
        assertEquals("v2", cache.get()) // background refresh fired
        assertEquals(2, callCount)
    }

    // ── 5. First failure → degradedTtl, isStale = true ───────────────────────

    @Test
    fun `first refresh failure switches to degradedTtl`() = runTest {
        var fail = false
        val cache = SingleItemCache.build<String>(
            loader             = { if (fail) null else "v1" },
            scope              = this,
            normalTtl          = Duration.ofSeconds(5),
            degradedTtl        = Duration.ofSeconds(2),
            maxConsecutiveFailures = 3,
        )
        cache.get()          // seed
        fail = true
        advanceTimeBy(5_001) // trigger background refresh → fails

        assertEquals("v1", cache.get())  // stale value still served
        assertTrue(cache.isStale)
        assertEquals(1, cache.failures)
        assertEquals(SingleItemCache.TtlMode.DEGRADED, cache.ttlMode)
    }

    // ── 6. Recovery resets to normalTtl ──────────────────────────────────────

    @Test
    fun `successful refresh after failures resets to normalTtl`() = runTest {
        var fail = false
        var callCount = 0
        val cache = SingleItemCache.build<String>(
            loader             = { if (fail) null else "v${++callCount}" },
            scope              = this,
            normalTtl          = Duration.ofSeconds(5),
            degradedTtl        = Duration.ofSeconds(2),
            maxConsecutiveFailures = 3,
        )
        cache.get()           // seed → "v1"
        fail = true
        advanceTimeBy(5_001)  // failure #1

        fail = false
        advanceTimeBy(2_001)  // degradedTtl refresh → success

        assertEquals("v2", cache.get())
        assertFalse(cache.isStale)
        assertEquals(0, cache.failures)
        assertEquals(SingleItemCache.TtlMode.NORMAL, cache.ttlMode)
    }

    // ── 7. maxConsecutiveFailures → invalidation ──────────────────────────────

    @Test
    fun `hitting maxConsecutiveFailures invalidates entry`() = runTest {
        var fail = false
        val cache = SingleItemCache.build<String>(
            loader             = { if (fail) null else "v1" },
            scope              = this,
            normalTtl          = Duration.ofSeconds(5),
            degradedTtl        = Duration.ofSeconds(2),
            retryBackoff       = Duration.ofSeconds(1),
            maxConsecutiveFailures = 3,
        )
        cache.get()           // seed
        fail = true
        advanceTimeBy(5_001)  // failure 1
        advanceTimeBy(2_001)  // failure 2
        advanceTimeBy(2_001)  // failure 3 → invalidated

        assertNull(cache.get())         // entry gone
        assertEquals(0, cache.failures) // counter reset
        assertFalse(cache.isStale)
    }

    // ── 8. retryBackoff suppresses post-invalidation stampede ────────────────

    @Test
    fun `retryBackoff prevents stampede after invalidation`() = runTest {
        var fail = true
        val loadCount = AtomicInteger(0)
        val cache = SingleItemCache.build<String>(
            loader             = { loadCount.incrementAndGet(); if (fail) null else "v1" },
            scope              = this,
            normalTtl          = Duration.ofSeconds(5),
            degradedTtl        = Duration.ofSeconds(2),
            retryBackoff       = Duration.ofSeconds(3),
            maxConsecutiveFailures = 1,
        )

        // First cold load fails → retryAfter set
        cache.get()
        val loadsAfterFirstFail = loadCount.get()

        // 50 concurrent callers within the backoff window — should all be suppressed
        (1..50).map { async { cache.get() } }.awaitAll()
        assertEquals(loadsAfterFirstFail, loadCount.get(), "no loads should fire within backoff")

        // After backoff elapses, one load fires
        fail = false
        advanceTimeBy(3_001)
        cache.get()
        assertEquals(loadsAfterFirstFail + 1, loadCount.get(), "exactly one load after backoff")
    }

    // ── 9. invalidate() clears entry and cancels refresh ─────────────────────

    @Test
    fun `invalidate clears entry and next get cold-starts`() = runTest {
        var callCount = 0
        val cache = SingleItemCache.build<String>(
            loader      = { "v${++callCount}" },
            scope       = this,
            normalTtl   = Duration.ofSeconds(10),
            degradedTtl = Duration.ofSeconds(2),
        )
        cache.get()         // "v1"
        cache.invalidate()
        assertEquals("v2", cache.get()) // fresh cold load
        assertEquals(2, callCount)
    }
}
