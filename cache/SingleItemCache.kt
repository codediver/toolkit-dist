import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.time.Instant
import java.time.Duration

/**
 * Single-item coroutine cache with:
 *
 *  - Lock-free warm reads  — a @Volatile snapshot field; no suspension on the hot path.
 *  - Stampede protection   — Mutex coalesces concurrent cold-start callers; they suspend
 *                            (no thread blocked) while the single in-flight load runs.
 *  - Async background refresh — a coroutine re-schedules itself after each successful load;
 *                            callers always see the last good value instantly.
 *  - Dual TTL              — normalTtl while healthy; degradedTtl after the first failure.
 *  - Bounded retries       — entry dropped after maxConsecutiveFailures; a retryAfter
 *                            window prevents a post-invalidation stampede.
 *
 * @param loader   Suspending function that fetches the value. Throw or return null on failure.
 * @param scope    CoroutineScope that owns the background refresh coroutine.
 */
class SingleItemCache<V : Any>(
    private val loader: suspend () -> V?,
    private val normalTtl: Duration,
    private val degradedTtl: Duration,
    private val retryBackoff: Duration,
    private val maxConsecutiveFailures: Int,
    private val scope: CoroutineScope,
) {
    // ---------------------------------------------------------------------------
    // Snapshot — one @Volatile write makes all fields atomically visible
    // ---------------------------------------------------------------------------

    private data class Snapshot<V>(
        val value: V?,
        val expiresAt: Instant,
        val retryAfter: Instant,
        val failures: Int,
    ) {
        fun isWarm()    = value != null && Instant.now().isBefore(expiresAt)
        fun inBackoff() = Instant.now().isBefore(retryAfter)
    }

    @Volatile private var snapshot = Snapshot<V>(
        value      = null,
        expiresAt  = Instant.MIN,
        retryAfter = Instant.MIN,
        failures   = 0,
    )

    // Serialises cold-start loads and post-expiry refreshes.
    // Callers that lose the lock suspend — no thread is blocked.
    private val mutex = Mutex()

    private var refreshJob: Job? = null

    // ---------------------------------------------------------------------------
    // Public API
    // ---------------------------------------------------------------------------

    /**
     * Returns the cached value.
     *
     * Hot path  (entry alive): reads @Volatile snapshot — no suspension, no lock.
     * Cold path (expired / empty): suspends until the single in-flight load finishes.
     */
    suspend fun get(): V? {
        // Hot path — lock-free volatile read
        snapshot.takeIf { it.isWarm() }?.let { return it.value }

        // Cold path — all concurrent callers funnel through the mutex
        return mutex.withLock {
            // Re-check after acquiring — another coroutine may have loaded while we waited
            snapshot.takeIf { it.isWarm() }?.value ?: doLoad()
        }
    }

    /** Returns whatever is currently cached without triggering a load. */
    val value: V? get() = snapshot.value

    /** True while serving a stale value (at least one refresh has failed). */
    val isStale: Boolean get() = snapshot.let { it.failures > 0 && it.value != null }

    /** Current TTL mode. */
    val ttlMode: TtlMode get() = if (snapshot.failures == 0) TtlMode.NORMAL else TtlMode.DEGRADED

    /** Consecutive failures since the last successful load. */
    val failures: Int get() = snapshot.failures

    /**
     * Evicts the entry immediately.
     * The next [get] triggers a fresh synchronous load after any retryAfter window.
     */
    suspend fun invalidate() = mutex.withLock {
        refreshJob?.cancel()
        snapshot = Snapshot(null, Instant.MIN, Instant.MIN, 0)
    }

    // ---------------------------------------------------------------------------
    // Internal — all called under the mutex
    // ---------------------------------------------------------------------------

    /** Performs a load. Caller holds the mutex. */
    private suspend fun doLoad(): V? {
        if (snapshot.inBackoff()) return snapshot.value

        val fresh = runCatching { loader() }.getOrNull()

        return if (fresh != null) {
            snapshot = Snapshot(
                value      = fresh,
                expiresAt  = Instant.now() + normalTtl,
                retryAfter = Instant.MIN,
                failures   = 0,
            )
            scheduleRefresh(normalTtl)
            fresh
        } else {
            val f = snapshot.failures + 1
            if (f >= maxConsecutiveFailures) {
                // Give up — next attempt is caller-driven after retryAfter elapses
                refreshJob?.cancel()
                snapshot = Snapshot(
                    value      = null,
                    expiresAt  = Instant.MIN,
                    retryAfter = Instant.now() + retryBackoff,
                    failures   = 0,
                )
            } else {
                snapshot = Snapshot(
                    value      = snapshot.value,
                    expiresAt  = Instant.now() + degradedTtl,
                    retryAfter = Instant.now() + retryBackoff,
                    failures   = f,
                )
                scheduleRefresh(degradedTtl)
            }
            snapshot.value
        }
    }

    /** Cancels any previous refresh job and schedules a new one after [delay]. */
    private fun scheduleRefresh(delay: Duration) {
        refreshJob?.cancel()
        refreshJob = scope.launch {
            delay(delay.toMillis())
            mutex.withLock { doLoad() }
        }
    }

    // ---------------------------------------------------------------------------

    enum class TtlMode { NORMAL, DEGRADED }

    // ---------------------------------------------------------------------------
    // Builder-style companion
    // ---------------------------------------------------------------------------

    companion object {
        fun <V : Any> build(
            loader: suspend () -> V?,
            scope: CoroutineScope = CoroutineScope(Dispatchers.IO + SupervisorJob()),
            normalTtl: Duration = Duration.ofMinutes(5),
            degradedTtl: Duration = Duration.ofMinutes(1),
            retryBackoff: Duration = Duration.ofSeconds(5),
            maxConsecutiveFailures: Int = 3,
        ): SingleItemCache<V> {
            require(degradedTtl < normalTtl) { "degradedTtl must be shorter than normalTtl" }
            require(maxConsecutiveFailures >= 1) { "maxConsecutiveFailures must be >= 1" }
            return SingleItemCache(loader, normalTtl, degradedTtl, retryBackoff, maxConsecutiveFailures, scope)
        }
    }
}
