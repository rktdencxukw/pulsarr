package ai.platon.pulsar.protocol.native

import ai.platon.pulsar.common.HttpHeaders
import ai.platon.pulsar.common.persist.ext.options
import ai.platon.pulsar.common.urls.UrlUtils
import ai.platon.pulsar.crawl.protocol.ForwardingResponse
import ai.platon.pulsar.crawl.protocol.Response
import ai.platon.pulsar.crawl.protocol.http.AbstractHttpProtocol
import ai.platon.pulsar.crawl.protocol.http.ProtocolStatusTranslator
import ai.platon.pulsar.persist.PageDatum
import ai.platon.pulsar.persist.WebPage
import ai.platon.pulsar.protocol.browser.driver.SessionLostException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.jsoup.Connection
import org.jsoup.Jsoup
import java.nio.charset.StandardCharsets
import java.time.Duration

class NativeProtocol : AbstractHttpProtocol() {
    private val jsoupCreateDestroyMonitor = Any()
    private var jsoupSession: Connection? = null
    override suspend fun getResponseDeferred(page: WebPage, followRedirects: Boolean): Response? {
        require(page.isNotInternal) { "Unexpected internal page ${page.url}" }
        require(page.isResource) { "Unexpected non resource page ${page.url}" }

        synchronized(jsoupCreateDestroyMonitor) {
            jsoupSession = this.newSession(emptyMap(), emptyList(), page.options.proxyServer)
//            if (jsoupSession == null) {
//                val (headers, cookies) = getHeadersAndCookies()
//                jsoupSession = newSession(headers, cookies)
//                jsoupSession = this.newSession(emptyMap(), emptyList(), page.options.proxyServer)
//            }
        }

        val url = page.url
//        HttpsUrlValidator.retrieveResponseFromServer(url);
        val response = withContext(Dispatchers.IO) {
            jsoupSession?.newRequest()?.url(url)?.execute()
        }?: return ForwardingResponse.failed(page, SessionLostException("null response"))
        val protocolStatus = ProtocolStatusTranslator.translateHttpCode(response.statusCode())
        val pageSource = response.body()
        var pageDatum = PageDatum(page).also {
            it.protocolStatus = protocolStatus
            it.headers.putAll(response.headers())
            it.contentType = response.contentType()
            it.content = pageSource.toByteArray(StandardCharsets.UTF_8)
            it.originalContentLength = it.content?.size ?: 0
        }

        val headers = pageDatum.headers

        // The page content's encoding is already converted to UTF-8 by Web driver
        val utf8 = StandardCharsets.UTF_8.name()
        require(utf8 == "UTF-8") { "UTF-8 is expected" }

        headers.put(HttpHeaders.CONTENT_ENCODING, utf8)
        headers.put(HttpHeaders.Q_TRUSTED_CONTENT_ENCODING, utf8)
        headers.put(HttpHeaders.Q_RESPONSE_TIME, System.currentTimeMillis().toString())

        val urls = pageDatum.activeDOMUrls
        if (urls != null) {
            pageDatum.location = urls.location
            if (pageDatum.url != pageDatum.location) {
                // in-browser redirection
                // messageWriter?.debugRedirects(pageDatum.url, urls)
            }
        }

        return ForwardingResponse(page, pageDatum)
    }

//    override fun getResponse(url: String, page: WebPage, followRedirects: Boolean): Response {
//        TODO("Not yet implemented")
//    }

    override fun getResponse(page: WebPage, followRedirects: Boolean): Response? {
        return runBlocking { getResponseDeferred(page, followRedirects) }
    }



    fun newSession(headers: Map<String, String>, cookies: List<Map<String, String>>, proxyServer: String): Connection {
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


}