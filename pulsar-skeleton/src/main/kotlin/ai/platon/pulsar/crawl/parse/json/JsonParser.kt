package ai.platon.pulsar.crawl.parse.json

import ai.platon.pulsar.common.PulsarParams
import ai.platon.pulsar.common.persist.ext.loadEvent
import ai.platon.pulsar.crawl.parse.ParseResult
import ai.platon.pulsar.crawl.parse.Parser
import ai.platon.pulsar.crawl.parse.html.PrimerHtmlParser
import ai.platon.pulsar.dom.FeaturedDocument
import ai.platon.pulsar.persist.WebPage
import ai.platon.pulsar.persist.metadata.ParseStatusCodes
import org.slf4j.LoggerFactory
import java.net.MalformedURLException
import java.util.concurrent.atomic.AtomicInteger

class JsonParser(
) : Parser {
    companion object {
        val numHtmlParses = AtomicInteger()
        val numHtmlParsed = AtomicInteger()
    }
    private val logger = LoggerFactory.getLogger(this::class.java)
    override fun parse(page: WebPage): ParseResult {
        return try {
            // The base url is set by protocol, it might be different from the page url
            // if the request redirects.
            onWillParseHTMLDocument(page)
            require(page.hasVar(PulsarParams.VAR_IS_SCRAPE))
            onHTMLDocumentParsed(page, FeaturedDocument.NIL ) // 这里会调用executeQuery，然后装填 resultset. FeatureDocument没用上
            ParseResult()
        } catch (e: MalformedURLException) {
            ParseResult.failed(ParseStatusCodes.FAILED_MALFORMED_URL, e.message)
        } catch (e: Exception) {
            ParseResult.failed(ParseStatusCodes.FAILED_INVALID_FORMAT, e.message)
        }
    }
    private fun onWillParseHTMLDocument(page: WebPage) {
        numHtmlParses.incrementAndGet()

        try {
            page.loadEvent?.onWillParseHTMLDocument?.invoke(page)
        } catch (e: Throwable) {
            logger.warn("Failed to invoke onWillParseHTMLDocument | ${page.configuredUrl}", e)
        }
    }
    private fun onHTMLDocumentParsed(page: WebPage, doc: FeaturedDocument){
        try {// kcread 完成回调 4.9
            page.loadEvent?.onHTMLDocumentParsed?.invoke(page, doc)
        } catch (e: Throwable) {
            logger.warn("Failed to invoke onHTMLDocumentParsed | ${page.configuredUrl}", e)
        } finally {
            PrimerHtmlParser.numHtmlParsed.incrementAndGet()
        }
    }
}
