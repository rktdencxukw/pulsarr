package ai.platon.pulsar.common.collect

import ai.platon.pulsar.common.Priority13
import ai.platon.pulsar.common.collect.FetchCatchManager.Companion.REAL_TIME_PRIORITY
import ai.platon.pulsar.common.config.ImmutableConfig
import ai.platon.pulsar.common.urls.UrlAware
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicBoolean

open class DelayUrl(
    val url: UrlAware,
    val delay: Duration
) : Delayed {
    val startTime = Instant.now() + delay

    override fun compareTo(other: Delayed): Int {
        return Duration.between(startTime, (other as DelayUrl).startTime).toMillis().toInt()
    }

    override fun getDelay(unit: TimeUnit): Long {
        val delay = Duration.between(startTime, Instant.now())
        return unit.convert(delay.toMillis(), TimeUnit.MILLISECONDS)
    }
}

/**
 * The fetch catch manager
 * */
interface FetchCatchManager {
    companion object {
        val REAL_TIME_PRIORITY = Priority13.HIGHEST.value * 2
    }

    /**
     * The priority fetch caches
     * */
    val caches: MutableMap<Int, FetchCache>
    val unorderedCaches: MutableList<FetchCache>
    val totalItems: Int

    val lowestCache: FetchCache
    val lower5Cache: FetchCache
    val lower4Cache: FetchCache
    val lower3Cache: FetchCache
    val lower2Cache: FetchCache
    val lowerCache: FetchCache
    val normalCache: FetchCache
    val higherCache: FetchCache
    val higher2Cache: FetchCache
    val higher3Cache: FetchCache
    val higher4Cache: FetchCache
    val higher5Cache: FetchCache
    val highestCache: FetchCache

    val realTimeCache: FetchCache
    val delayCache: DelayQueue<DelayUrl>

    fun initialize()
    fun removeDeceased()
}

/**
 * The abstract fetch catch manager
 * */
abstract class AbstractFetchCatchManager(val conf: ImmutableConfig) : FetchCatchManager {
    protected val initialized = AtomicBoolean()
    override val totalItems get() = ensureInitialized().caches.values.sumOf { it.size }

    override val lowestCache: FetchCache get() = ensureInitialized().caches[Priority13.LOWEST.value]!!
    override val lower5Cache: FetchCache get() = ensureInitialized().caches[Priority13.LOWER5.value]!!
    override val lower4Cache: FetchCache get() = ensureInitialized().caches[Priority13.LOWER4.value]!!
    override val lower3Cache: FetchCache get() = ensureInitialized().caches[Priority13.LOWER3.value]!!
    override val lower2Cache: FetchCache get() = ensureInitialized().caches[Priority13.LOWER2.value]!!
    override val lowerCache: FetchCache get() = ensureInitialized().caches[Priority13.LOWER.value]!!
    override val normalCache: FetchCache get() = ensureInitialized().caches[Priority13.NORMAL.value]!!
    override val higherCache: FetchCache get() = ensureInitialized().caches[Priority13.HIGHER.value]!!
    override val higher2Cache: FetchCache get() = ensureInitialized().caches[Priority13.HIGHER2.value]!!
    override val higher3Cache: FetchCache get() = ensureInitialized().caches[Priority13.HIGHER3.value]!!
    override val higher4Cache: FetchCache get() = ensureInitialized().caches[Priority13.HIGHER4.value]!!
    override val higher5Cache: FetchCache get() = ensureInitialized().caches[Priority13.HIGHER5.value]!!
    override val highestCache: FetchCache get() = ensureInitialized().caches[Priority13.HIGHEST.value]!!

    override fun removeDeceased() {
        ensureInitialized()
        caches.values.forEach { it.removeDeceased() }
        unorderedCaches.forEach { it.removeDeceased() }
        val now = Instant.now()
        delayCache.removeIf { it.url.deadTime > now }
    }

    private fun ensureInitialized(): AbstractFetchCatchManager {
        if (initialized.compareAndSet(false, true)) {
            initialize()
        }
        return this
    }
}

/**
 * The global cache
 * */
open class ConcurrentFetchCatchManager(conf: ImmutableConfig) : AbstractFetchCatchManager(conf) {
    /**
     * The priority fetch caches
     * */
    override val caches = ConcurrentSkipListMap<Int, FetchCache>()

    override val unorderedCaches: MutableList<FetchCache> = Collections.synchronizedList(mutableListOf())

    /**
     * The real time fetch cache
     * */
    override val realTimeCache: FetchCache = ConcurrentFetchCache("realtime")

    /**
     * The delayed fetch cache
     * */
    override val delayCache = DelayQueue<DelayUrl>()

    override fun initialize() {
        if (initialized.compareAndSet(false, true)) {
            Priority13.values().forEach { caches[it.value] = ConcurrentFetchCache(it.name) }
        }
    }
}

class LoadingFetchCatchManager(
    val urlLoader: ExternalUrlLoader,
    val capacity: Int = 10_000,
    conf: ImmutableConfig
) : ConcurrentFetchCatchManager(conf) {
    /**
     * The real time fetch cache
     * */
    override val realTimeCache: FetchCache = LoadingFetchCache("realtime", urlLoader, REAL_TIME_PRIORITY, capacity)

    override fun initialize() {
        if (initialized.compareAndSet(false, true)) {
            Priority13.values().forEach {
                caches[it.value] = LoadingFetchCache(it.name, urlLoader, it.value, capacity)
            }
        }
    }
}
