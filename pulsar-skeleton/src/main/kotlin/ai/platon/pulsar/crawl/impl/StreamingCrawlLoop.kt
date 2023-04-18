package ai.platon.pulsar.crawl.impl

import ai.platon.pulsar.common.collect.UrlFeeder
import ai.platon.pulsar.common.config.CapabilityTypes.CRAWL_ENABLE_DEFAULT_DATA_COLLECTORS
import ai.platon.pulsar.common.config.ImmutableConfig
import ai.platon.pulsar.common.urls.UrlAware
import osp.leobert.android.diagram.notation.GenerateUMLDiagram
import ai.platon.pulsar.context.PulsarContexts
import ai.platon.pulsar.crawl.Crawler
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean

@GenerateUMLDiagram("StreamingCrawler")
open class StreamingCrawlLoop(
    /**
     * The unmodified configuration load from file
     * */
    unmodifiedConfig: ImmutableConfig,
    /**
     * The loop name
     * */
    name: String = "StreamingCrawlLoop"
) : AbstractCrawlLoop(name, unmodifiedConfig) {
    private val logger = LoggerFactory.getLogger(StreamingCrawlLoop::class.java)

    private val running = AtomicBoolean()
    private val scope = CoroutineScope(Dispatchers.Default) + CoroutineName("sc")
    private var crawlJob: Job? = null
    private val started = CountDownLatch(1)

    val isRunning get() = running.get()

    override val urlFeeder by lazy { createUrlFeeder() }

    private lateinit var _crawler: StreamingCrawler
    override val crawler: Crawler get() = _crawler

    private val context get() = PulsarContexts.create()

    init {
        logger.info("Crawl loop is created | @{}", hashCode())
    }

    @Synchronized
    override fun start() {
        if (isRunning) {
            // issue a warning for debug
            logger.warn("Crawl loop #{} is already running", id)
        }

        if (running.compareAndSet(false, true)) {
            start0()
        }
    }

    @Synchronized
    override fun stop() {
        if (running.compareAndSet(true, false)) {
            _crawler.close()
            runBlocking {
                crawlJob?.cancelAndJoin()
                crawlJob = null

                logger.info("Crawl loop #{} is stopped", id)
            }
        }
    }

    override fun await() {
        started.await()
        crawler.await()
    }

    private fun start0() {
        logger.info("Registered {} link collectors | loop#{} @{}", urlFeeder.collectors.size, id, hashCode())

        val urls = urlFeeder.asSequence()
        // kcread 启动入口 0.01 创建 StreamingCrawler, 代表一个循环线程
        _crawler = StreamingCrawler(urls, context.createSession(), noProxy = false)

        crawlJob = scope.launch {
            supervisorScope {
                started.countDown()
                _crawler.run(this)
            }
        }
    }

    private fun createUrlFeeder(): UrlFeeder {
        val enableDefaults = config.getBoolean(CRAWL_ENABLE_DEFAULT_DATA_COLLECTORS, true)
        return UrlFeeder(context.crawlPool, enableDefaults = enableDefaults)
    }
}
