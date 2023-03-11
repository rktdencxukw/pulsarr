/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.platon.pulsar.crawl.protocol

import ai.platon.pulsar.persist.PageDatum
import ai.platon.pulsar.persist.ProtocolStatus
import ai.platon.pulsar.persist.RetryScope
import ai.platon.pulsar.persist.WebPage
import ai.platon.pulsar.persist.metadata.MultiMetadata

/**
 * Forward a response.
 */
open class ForwardingResponse(
        page: WebPage,
        pageDatum: PageDatum
) : Response(page, pageDatum) {
    constructor(status: ProtocolStatus, page: WebPage) : this("", status, MultiMetadata(), page)

    constructor(retryScope: RetryScope, reason: String, page: WebPage):
            this("", ProtocolStatus.retry(retryScope, reason), MultiMetadata(), page)

    constructor(retryScope: RetryScope, retryReason: Exception, page: WebPage):
            this("", ProtocolStatus.retry(retryScope, retryReason), MultiMetadata(), page)

    constructor(e: Throwable?, page: WebPage) : this("", ProtocolStatus.failed(e), MultiMetadata(), page)

    constructor(content: String, status: ProtocolStatus, headers: MultiMetadata, page: WebPage)
            : this(page, PageDatum(page.url, page.location, status, content.toByteArray(), headers = headers)) {
    }

    companion object {
        fun unfetched(page: WebPage) = ForwardingResponse(ProtocolStatus.STATUS_NOTFETCHED, page)
        fun unchanged(page: WebPage) = ForwardingResponse(page.protocolStatus, page)

        // TODO: define the difference between a canceled task and a retry task
        fun canceled(page: WebPage) = ForwardingResponse(ProtocolStatus.STATUS_CANCELED, page)
        fun canceled(page: WebPage, reason: String) = ForwardingResponse(ProtocolStatus.cancel(reason), page)

        fun retry(page: WebPage, retryScope: RetryScope, reason: String) = ForwardingResponse(retryScope, reason, page)
        fun retry(page: WebPage, retryScope: RetryScope, retryReason: Exception) = ForwardingResponse(retryScope, retryReason, page)

        fun privacyRetry(page: WebPage, reason: String) = retry(page, RetryScope.PRIVACY, reason)
        fun privacyRetry(page: WebPage, reason: Exception) = retry(page, RetryScope.PRIVACY, reason)

        fun crawlRetry(page: WebPage, reason: String) = retry(page, RetryScope.CRAWL, reason)
        fun crawlRetry(page: WebPage, reason: Exception) = retry(page, RetryScope.CRAWL, reason)

        fun failed(page: WebPage, e: Throwable?) = ForwardingResponse(e, page)
    }
}
