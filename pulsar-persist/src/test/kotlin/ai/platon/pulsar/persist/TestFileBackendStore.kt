package ai.platon.pulsar.persist

import ai.platon.pulsar.common.urls.UrlUtils
import ai.platon.pulsar.persist.gora.FileBackendPageStore
import org.junit.Before
import org.junit.Test
import java.nio.file.Files
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class TestFileBackendStore {
    private val url = "https://www.amazon.com/dp/B082P8J28M"
    private val persistDirectory = Files.createTempDirectory("pulsar-test")
    private val store = FileBackendPageStore(persistDirectory)
    private lateinit var page: WebPage

    @Before
    fun setup() {
        page = WebPageExt.newTestWebPage(url)
    }

    @Test
    fun whenWritePage_ThenAvroFileExists() {
        val path = store.getPersistPath(url, ".avro")
        store.writeAvro(page)
        assertTrue { Files.exists(path) }

        val key = UrlUtils.reverseUrl(url)
        val loadedPage = store.readAvro(key)
        assertNotNull(loadedPage)
    }

    @Test
    fun whenWritePage_ThenReadSuccess() {
        store.writeAvro(page)

        val key = UrlUtils.reverseUrl(url)
        val loadedPage = store.readAvro(key)
        assertNotNull(loadedPage)
    }
}
