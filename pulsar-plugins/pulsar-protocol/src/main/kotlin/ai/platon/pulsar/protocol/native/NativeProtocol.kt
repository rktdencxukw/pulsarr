package ai.platon.pulsar.protocol.native

import ai.platon.pulsar.common.persist.ext.options
import ai.platon.pulsar.common.urls.UrlUtils
import ai.platon.pulsar.crawl.fetch.driver.AbstractBrowser
import ai.platon.pulsar.crawl.protocol.ForwardingResponse
import ai.platon.pulsar.crawl.protocol.Response
import ai.platon.pulsar.crawl.protocol.http.AbstractNativeHttpProtocol
import ai.platon.pulsar.crawl.protocol.http.ProtocolStatusTranslator
import ai.platon.pulsar.crawl.signature.HttpsUrlValidator
import ai.platon.pulsar.persist.WebPage
import ai.platon.pulsar.protocol.browser.driver.SessionLostException
import ai.platon.pulsar.protocol.browser.emulator.BrowserResponseEvents
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.jsoup.Connection
import org.jsoup.Jsoup
import java.nio.charset.StandardCharsets
import java.time.Duration

class NativeProtocol : AbstractNativeHttpProtocol() {
    private val jsoupCreateDestroyMonitor = Any()
    private var jsoupSession: Connection? = null
    override suspend fun getResponseDeferred(page: WebPage, followRedirects: Boolean): Response? {
        require(page.isNotInternal) { "Unexpected internal page ${page.url}" }
        require(page.isResource) { "Unexpected non resource page ${page.url}" }

        synchronized(jsoupCreateDestroyMonitor) {
            if (jsoupSession == null) {
//                val (headers, cookies) = getHeadersAndCookies()
//                jsoupSession = newSession(headers, cookies)
                jsoupSession = newSession(emptyMap(), emptyList(), page.options.proxyServer)
            }
        }

        val url = page.url
        HttpsUrlValidator.retrieveResponseFromServer(url);
        val response = withContext(Dispatchers.IO) {
            jsoupSession?.newRequest()?.url(url)?.execute()
        }?: return ForwardingResponse.failed(page, SessionLostException("null response"))
        val protocolStatus = ProtocolStatusTranslator.translateHttpCode(response.statusCode())
        val pageSource = response.body()
        var pageDatum = PageDatum.also {
            it.protocolStatus = protocolStatus
            it.headers.putAll(response.headers())
            it.contentType = response.contentType()
            it.content = navigateTask.pageSource.toByteArray(StandardCharsets.UTF_8)
            it.originalContentLength = it.content?.size ?: 0
        }

        responseHandler.emit(BrowserResponseEvents.willCreateResponse)
        return createResponseWithDatum(navigateTask, navigateTask.pageDatum).also {
            responseHandler.emit(BrowserResponseEvents.responseCreated)

        return response
    }

    override fun getResponse(url: String, page: WebPage, followRedirects: Boolean): Response {
        require(page.isNotInternal) { "Unexpected internal page ${page.url}" }
        require(page.isResource) { "Unexpected non resource page ${page.url}" }

        synchronized(jsoupCreateDestroyMonitor) {
            if (jsoupSession == null) {
//                val (headers, cookies) = getHeadersAndCookies()
//                jsoupSession = newSession(headers, cookies)
                jsoupSession = newSession(emptyMap(), emptyList(), page.options.proxyServer)
            }
        }

        HttpsUrlValidator.retrieveResponseFromServer(url);
        val response = withContext(Dispatchers.IO) {
            jsoupSession?.newRequest()?.url(url)?.execute()
        }

        return response
    }


    private fun newSession(headers: Map<String, String>, cookies: List<Map<String, String>>, proxyServer: String): Connection {
        // TODO: use the same user agent as this browser
//        val userAgent = browser.userAgent ?: (browser as AbstractBrowser).browserSettings.userAgent.getRandomUserAgent()
        val userAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36"

        val httpTimeout = Duration.ofSeconds(20)
        val session = Jsoup.newSession()
            .timeout(httpTimeout.toMillis().toInt())
            .userAgent(userAgent)
            .headers(headers)
            .ignoreContentType(true)
            .ignoreHttpErrors(true)

        if (cookies.isNotEmpty()) {
            session.cookies(cookies.first())
        }

        // Since the browser uses the system proxy (by default),
        // so the http connection should also use the system proxy
        val proxy = proxyServer ?: System.getenv("http_proxy")
        if (proxy != null && UrlUtils.isStandard(proxy)) {
            val u = UrlUtils.getURLOrNull(proxy)
            if (u != null) {
                session.proxy(u.host, u.port)
            }
        } else {
            throw IllegalStateException("Invalid proxy: $proxy")
        }

        return session
    }

    override fun getResponse(page: WebPage, followRedirects: Boolean): Response? {
        TODO("Not yet implemented")
    }

}