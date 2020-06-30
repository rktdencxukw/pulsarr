package ai.platon.pulsar.crawl

import ai.platon.pulsar.PulsarContext
import ai.platon.pulsar.PulsarSession
import ai.platon.pulsar.common.FlowState
import ai.platon.pulsar.common.IllegalApplicationContextStateException
import ai.platon.pulsar.common.Strings
import ai.platon.pulsar.common.config.AppConstants
import ai.platon.pulsar.common.config.CapabilityTypes.BROWSER_MAX_ACTIVE_TABS
import ai.platon.pulsar.common.config.CapabilityTypes.PRIVACY_CONTEXT_NUMBER
import ai.platon.pulsar.common.options.LoadOptions
import ai.platon.pulsar.common.prependReadableClassName
import ai.platon.pulsar.common.proxy.ProxyVendorUntrustedException
import ai.platon.pulsar.persist.WebPage
import com.codahale.metrics.Gauge
import com.codahale.metrics.SharedMetricRegistries
import kotlinx.coroutines.*
import org.h2tools.dev.util.ConcurrentLinkedList
import oshi.SystemInfo
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.abs

open class StreamingCrawler(
        private val urls: Sequence<String>,
        private val options: LoadOptions = LoadOptions.create(),
        val pageCollector: ConcurrentLinkedList<WebPage>? = null,
        session: PulsarSession = PulsarContext.createSession(),
        autoClose: Boolean = true
): Crawler(session, autoClose) {
    companion object {
        private val metricRegistry = SharedMetricRegistries.getOrCreate("pulsar")
        private val numRunningTasks = AtomicInteger()
        private val illegalState = AtomicBoolean()

        init {
            metricRegistry.register(prependReadableClassName(this,"runningTasks"), object: Gauge<Int> {
                override fun getValue(): Int = numRunningTasks.get()
            })
        }
    }

    private val conf = session.sessionConfig
    private val numPrivacyContexts get() = conf.getInt(PRIVACY_CONTEXT_NUMBER, 2)
    private val numMaxActiveTabs get() = conf.getInt(BROWSER_MAX_ACTIVE_TABS, AppConstants.NCPU)
    private val fetchConcurrency get() = numPrivacyContexts * numMaxActiveTabs
    private val idleTimeout = Duration.ofMinutes(10)
    private var lastActiveTime = Instant.now()
    private val idleTime get() = Duration.between(lastActiveTime, Instant.now())
    private val isIdle get() = idleTime > idleTimeout
    private val isAppActive get() = isActive && !isIdle && !illegalState.get()
    private val systemInfo = SystemInfo()
    // OSHI cached the value, so it's fast and safe to be called frequently
    private val availableMemory get() = systemInfo.hardware.memory.available
    private val requiredMemory = 500 * 1024 * 1024L // 500 MiB
    private val memoryRemaining get() = availableMemory - requiredMemory
    private val numTasks = AtomicInteger()
    private val taskTimeout = Duration.ofMinutes(6)
    private var flowState = FlowState.CONTINUE

    var onLoadComplete: (WebPage) -> Unit = {}

    open suspend fun run() {
        supervisorScope {
            urls.forEachIndexed { j, url ->
                val state = load(j, url, this)
                if (state != FlowState.CONTINUE) {
                    return@supervisorScope
                }
            }
        }

        log.info("Total {} tasks are loaded in session {}", numTasks, session)
    }

    open suspend fun run(scope: CoroutineScope) {
        urls.forEachIndexed { j, url ->
            val state = load(j, url, scope)
            if (state != FlowState.CONTINUE) {
                return
            }
        }

        log.info("Total {} tasks are loaded in session {}", numTasks, session)
    }

    private suspend fun load(j: Int, url: String, scope: CoroutineScope): FlowState {
        lastActiveTime = Instant.now()
        numTasks.incrementAndGet()

        while (isAppActive && numRunningTasks.get() > fetchConcurrency) {
            delay(1000)
        }

        while (isAppActive && memoryRemaining < 0) {
            if (j % 20 == 0) {
                handleMemoryShortage(j)
            }
            delay(1000)
        }

        if (!isAppActive) {
            return FlowState.BREAK
        }

        var page: WebPage?
        numRunningTasks.incrementAndGet()
        val context = Dispatchers.Default + CoroutineName("w")
        scope.launch(context) {
            page = withTimeoutOrNull(taskTimeout.toMillis()) { load(url) }
            page?.let(onLoadComplete)
            numRunningTasks.decrementAndGet()
            lastActiveTime = Instant.now()
        }

        return flowState
    }

    private suspend fun load(url: String): WebPage? {
        return session.runCatching { loadDeferred(url, options) }
                .onFailure { flowState = handleException(url, it) }
                .getOrNull()
                ?.also { pageCollector?.add(it) }
    }

    private fun handleException(url: String, e: Throwable): FlowState {
        when (e) {
            is IllegalApplicationContextStateException -> {
                if (illegalState.compareAndSet(false, true)) {
                    log.info("Illegal context, quit streaming crawler")
                }
                return FlowState.BREAK
            }
            is ProxyVendorUntrustedException -> log.error(e.message?:"Unexpected error").let { return FlowState.BREAK }
            is TimeoutCancellationException -> log.warn("Timeout cancellation: {} | {}", Strings.simplifyException(e), url)
            else -> log.error("Unexpected exception", e)
        }
        return FlowState.CONTINUE
    }

    private fun handleMemoryShortage(j: Int) {
        log.info("$j.\tnumRunning: {}, availableMemory: {}, requiredMemory: {}, shortage: {}",
                numRunningTasks,
                Strings.readableBytes(availableMemory),
                Strings.readableBytes(requiredMemory),
                Strings.readableBytes(abs(memoryRemaining))
        )
        session.context.clearCaches()
        System.gc()
    }
}
