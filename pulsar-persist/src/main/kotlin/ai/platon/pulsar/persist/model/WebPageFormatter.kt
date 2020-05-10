/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.platon.pulsar.persist.model

import ai.platon.pulsar.persist.HypeLink
import ai.platon.pulsar.persist.WebPage
import ai.platon.pulsar.persist.gora.generated.GFieldGroup
import ai.platon.pulsar.persist.gora.generated.GHypeLink
import com.google.gson.GsonBuilder
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.util.Bytes
import org.jsoup.nodes.Document
import org.jsoup.nodes.Element
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.*
import java.util.function.Consumer
import java.util.stream.Collectors

class WebPageFormatter(page: WebPage) {
    private var withText = false
    private var withContent = false
    private var withLinks = false
    private var withFields = false
    private var withEntities = false
    private val page: WebPage
    private val zoneId: ZoneId
    fun withText(withText: Boolean): WebPageFormatter {
        this.withText = withText
        return this
    }

    fun withText(): WebPageFormatter {
        withText = true
        return this
    }

    fun withContent(withContent: Boolean): WebPageFormatter {
        this.withContent = withContent
        return this
    }

    fun withContent(): WebPageFormatter {
        withContent = true
        return this
    }

    fun withLinks(withLinks: Boolean): WebPageFormatter {
        this.withLinks = withLinks
        return this
    }

    fun withLinks(): WebPageFormatter {
        withLinks = true
        return this
    }

    fun withFields(withFields: Boolean): WebPageFormatter {
        this.withFields = withFields
        return this
    }

    fun withFields(): WebPageFormatter {
        withFields = true
        return this
    }

    fun withEntities(withEntities: Boolean): WebPageFormatter {
        this.withEntities = withEntities
        return this
    }

    fun withEntities(): WebPageFormatter {
        withEntities = true
        return this
    }

    fun toMap(): Map<String, Any> {
        val fields: MutableMap<String, Any> = LinkedHashMap()
        /* General */fields["key"] = page.key
        fields["url"] = page.url
        fields["options"] = page.options
        fields["isSeed"] = page.isSeed
        fields["createTime"] = format(page.createTime)
        fields["distance"] = page.distance
        /* Fetch */fields["crawlStatus"] = page.crawlStatus.toString()
        fields["protocolStatus"] = page.protocolStatus.name
        fields["protocolStatusMessage"] = page.protocolStatus.toString()
        if (page.content != null) {
            fields["contentLength"] = page.content!!.array().size
        }
        fields["fetchCount"] = page.fetchCount
        fields["fetchPriority"] = page.fetchPriority
        fields["fetchInterval"] = page.fetchInterval.toString()
        fields["retriesSinceFetch"] = page.fetchRetries
        fields["prevFetchTime"] = format(page.prevFetchTime)
        fields["fetchTime"] = format(page.fetchTime)
        fields["prevModifiedTime"] = format(page.prevModifiedTime)
        fields["modifiedTime"] = format(page.modifiedTime)
        fields["baseUrl"] = page.location
        fields["reprUrl"] = page.reprUrl
        fields["batchId"] = page.batchId
        /* Parse */fields["parseStatus"] = page.parseStatus.name
        fields["parseStatusMessage"] = page.parseStatus.toString()
        fields["encoding"] = page.encoding?:""
        fields["prevSignature"] = page.prevSignatureAsString
        fields["signature"] = page.signatureAsString
        fields["pageCategory"] = page.pageCategory.name
        fields["prevContentPublishTime"] = format(page.prevContentPublishTime)
        fields["contentPublishTime"] = format(page.contentPublishTime)
        fields["prevContentModifiedTime"] = format(page.prevContentModifiedTime)
        fields["contentModifiedTime"] = format(page.contentModifiedTime)
        fields["prevRefContentPublishTime"] = format(page.prevRefContentPublishTime)
        fields["refContentPublishTime"] = format(page.refContentPublishTime)
        // May be too long
// fields.put("inlinkAnchors", page.getInlinkAnchors());
        fields["pageTitle"] = page.pageTitle
        fields["contentTitle"] = page.contentTitle
        fields["inlinkAnchor"] = page.anchor
        fields["title"] = page.sniffTitle()
        /* Score */fields["contentScore"] = page.contentScore.toString()
        fields["score"] = page.score.toString()
        fields["cash"] = page.cash.toString()
        fields["marks"] = page.marks.asStringMap()
        fields["pageCounters"] = page.pageCounters.asStringMap()
        fields["metadata"] = page.metadata.asStringMap()
        fields["headers"] = page.headers.asStringMap()
        fields["linkCount"] = page.links.size
        fields["vividLinkCount"] = page.vividLinks.size
        fields["liveLinkCount"] = page.liveLinks.size
        fields["deadLinkCount"] = page.deadLinks.size
        fields["inlinkCount"] = page.inlinks.size
        fields["linksMessage"] = ("Total "
                + page.links.size + " links, "
                + page.vividLinks.size + " vivid links, "
                + page.liveLinks.size + " live links, "
                + page.deadLinks.size + " dead links, "
                + page.inlinks.size + " inlinks")
        if (withLinks) {
            fields["links"] = page.links.stream().map { obj: CharSequence -> obj.toString() }.collect(Collectors.toList())
            fields["vividLinks"] = page.vividLinks.values.stream().map { obj: CharSequence -> obj.toString() }.collect(Collectors.toList())
            fields["liveLinks"] = page.liveLinks.values.stream().map { l: GHypeLink? -> HypeLink.box(l!!).toString() }.collect(Collectors.toList())
            fields["deadLinks"] = page.deadLinks.stream().map { obj: CharSequence -> obj.toString() }.collect(Collectors.toList())
            fields["inlinks"] = page.inlinks.entries.stream()
                    .map { il: Map.Entry<CharSequence, CharSequence> -> il.key.toString() + "\t" + il.value }.collect(Collectors.joining("\n"))
        }
        if (withText) {
            fields["contentText"] = page.contentText
            fields["pageText"] = page.pageText
        }
        if (withContent && page.content != null) {
            fields["content"] = page.contentAsString
        }
        if (withEntities) {
            val pageEntities = page.pageModel.unbox().stream()
                    .map { fg: GFieldGroup? -> FieldGroupFormatter(fg!!).fields }
                    .collect(Collectors.toList())
            fields["pageEntities"] = pageEntities
        }
        return fields
    }

    /**
     * TODO: Optimization
     */
    fun toMap(fields: Set<String>): Map<String, Any> {
        return toMap().entries.filter { fields.contains(it.key) }.associate { it.key to it.value }
    }

    fun format(): String {
        val sb = StringBuilder()
        sb.append("url:\t" + page.url + "\n")
                .append("baseUrl:\t" + page.location + "\n")
                .append("batchId:\t" + page.batchId + "\n")
                .append("crawlStatus:\t" + page.crawlStatus + "\n")
                .append("protocolStatus:\t" + page.protocolStatus + "\n")
                .append("depth:\t" + page.distance + "\n")
                .append("pageCategory:\t" + page.pageCategory + "\n")
                .append("fetchCount:\t" + page.fetchCount + "\n")
                .append("fetchPriority:\t" + page.fetchPriority + "\n")
                .append("fetchInterval:\t" + page.fetchInterval + "\n")
                .append("retriesSinceFetch:\t" + page.fetchRetries + "\n")
        sb.append("\n")
                .append("options:\t" + page.options + "\n")
        sb.append("\n")
                .append("createTime:\t" + format(page.createTime) + "\n")
                .append("prevFetchTime:\t" + format(page.prevFetchTime) + "\n")
                .append("fetchTime:\t" + format(page.fetchTime) + "\n")
                .append("prevModifiedTime:\t" + format(page.prevModifiedTime) + "\n")
                .append("modifiedTime:\t" + format(page.modifiedTime) + "\n")
                .append("prevContentModifiedTime:\t" + format(page.prevContentModifiedTime) + "\n")
                .append("contentModifiedTime:\t" + format(page.contentModifiedTime) + "\n")
                .append("prevContentPublishTime:\t" + format(page.prevContentPublishTime) + "\n")
                .append("contentPublishTime:\t" + format(page.contentPublishTime) + "\n")
        sb.append("\n")
                .append("prevRefContentPublishTime:\t" + format(page.prevRefContentPublishTime) + "\n")
                .append("refContentPublishTime:\t" + format(page.refContentPublishTime) + "\n")
        sb.append("\n")
                .append("pageTitle:\t" + page.pageTitle + "\n")
                .append("contentTitle:\t" + page.contentTitle + "\n")
                .append("anchor:\t" + page.anchor + "\n")
                .append("title:\t" + page.sniffTitle() + "\n")
        sb.append("\n")
                .append("parseStatus:\t" + page.parseStatus.toString() + "\n")
                .append("prevSignature:\t" + page.prevSignatureAsString + "\n")
                .append("signature:\t" + page.signatureAsString + "\n")
                .append("contentScore:\t" + page.contentScore + "\n")
                .append("score:\t" + page.score + "\n")
                .append("cash:\t" + page.cash + "\n")
        if (page.reprUrl != null) {
            sb.append("\n\n").append("reprUrl:\t" + page.reprUrl + "\n")
        }
        val crawlMarks = page.marks
        if (!crawlMarks.unbox().isEmpty()) {
            sb.append("\n")
            crawlMarks.unbox().forEach { (key: CharSequence, value: CharSequence) -> sb.append("mark $key:\t$value\n") }
        }
        if (!page.pageCounters.unbox().isEmpty()) {
            sb.append("\n")
            page.pageCounters.unbox().forEach { (key: CharSequence, value: Int) -> sb.append("counter $key : $value\n") }
        }
        val metadata = page.metadata.asStringMap()
        if (!metadata.isEmpty()) {
            sb.append("\n")
            metadata.entries.stream().filter { e: Map.Entry<String, String> -> !e.value.startsWith("meta_") }
                    .forEach { e: Map.Entry<String, String> -> sb.append("metadata " + e.key + ":\t" + e.value + "\n") }
            metadata.entries.stream().filter { e: Map.Entry<String, String> -> e.value.startsWith("meta_") }
                    .forEach { e: Map.Entry<String, String> -> sb.append("metadata " + e.key + ":\t" + e.value + "\n") }
        }
        val headers = page.headers.unbox()
        if (headers != null && !headers.isEmpty()) {
            sb.append("\n")
            headers.forEach { (key: CharSequence, value: CharSequence) -> sb.append("header $key:\t$value\n") }
        }
        sb.append("\n")
        sb.append("Total " + page.links.size + " links, ")
                .append(page.vividLinks.size.toString() + " vivid links, ")
                .append(page.liveLinks.size.toString() + " live links, ")
                .append(page.deadLinks.size.toString() + " dead links, ")
                .append(page.inlinks.size.toString() + " inlinks\n")
        if (withLinks) {
            sb.append("\n")
            sb.append("links:\n")
            page.links.forEach(Consumer { l: CharSequence -> sb.append("links:\t$l\n") })
            sb.append("vividLinks:\n")
            page.vividLinks.forEach { (k: CharSequence, v: CharSequence) -> sb.append("liveLinks:\t$k\t$v\n") }
            sb.append("liveLinks:\n")
            page.liveLinks.values.forEach(Consumer { e: GHypeLink -> sb.append("liveLinks:\t" + e.url + "\t" + e.anchor + "\n") })
            sb.append("deadLinks:\n")
            page.deadLinks.forEach(Consumer { l: CharSequence -> sb.append("deadLinks:\t$l\n") })
            sb.append("inlinks:\n")
            page.inlinks.entries.forEach(Consumer<Map.Entry<CharSequence, CharSequence>> { e: Map.Entry<CharSequence, CharSequence> -> sb.append("inlink:\t" + e.key + "\t" + e.value + "\n") })
        }
        if (withContent) {
            val content = page.content
            if (content != null) {
                sb.append("\n")
                sb.append("contentType:\t" + page.contentType + "\n")
                        .append("content:START>>>\n")
                        .append(Bytes.toString(content.array()))
                        .append("\n<<<END:content\n")
            }
        }
        if (withText) {
            if (page.contentText.length > 0) {
                sb.append("\n")
                sb.append("contentText:START>>>\n")
                        .append(page.contentText)
                        .append("\n<<<END:contentText\n")
            }
            if (page.pageText.length > 0) {
                sb.append("pageText:START>>>\n")
                        .append(page.pageText)
                        .append("\n<<<END:pageText\n")
            }
        }
        if (withEntities) {
            sb.append("\n")
                    .append("entityField:START>>>\n")
            page.pageModel.unbox().stream()
                    .map { fg: GFieldGroup? -> FieldGroupFormatter(fg!!).fields }
                    .flatMap { m: Map<String, Any> -> m.entries.stream() }
                    .forEach { e: Map.Entry<String, Any> -> sb.append(e.key + ": " + e.value) }
            sb.append("\n<<<END:pageText\n")
        }
        sb.append("\n")
        return sb.toString()
    }

    fun createDocument(): Document {
        val doc = Document.createShell(page.location)
        doc.head().appendElement("title").appendText(page.pageTitle)
        doc.body().appendElement("h1").appendText(page.contentTitle)
        doc.body().appendElement("div")
                .attr("class", "content")
                .append(page.contentText)
        createLinksElement(doc.body())
        return doc
    }

    fun createLinksElement(parent: Element) {
        val links = parent.appendElement("div")
                .attr("class", "links")
                .appendElement("ul")
        var i = 0
        for (l in page.liveLinks.values) {
            ++i
            val text = if (StringUtils.isBlank(l.anchor)) i.toString() else l.anchor.toString()
            links.appendElement("li")
                    .appendElement("a").attr("href", l.url.toString()).appendText(text)
        }
    }

    fun toJson(): String {
        val gson = GsonBuilder().setPrettyPrinting().create()
        return gson.toJson(toMap())
    }

    private fun format(instant: Instant): String {
        return LocalDateTime.ofInstant(instant, zoneId).toString()
    }

    override fun toString(): String {
        return format()
    }

    init {
        Objects.requireNonNull(page)
        this.page = page
        zoneId = page.zoneId
    }
}