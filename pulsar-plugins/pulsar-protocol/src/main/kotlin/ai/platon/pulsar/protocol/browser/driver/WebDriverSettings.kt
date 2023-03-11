/*
 * Copyright (c) 2014 - 2022 platon.ai. <ivincent.zhang@gmail.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package ai.platon.pulsar.protocol.browser.driver

import ai.platon.pulsar.browser.common.BrowserSettings
import ai.platon.pulsar.browser.driver.chrome.common.ChromeOptions
import ai.platon.pulsar.common.config.AppConstants
import ai.platon.pulsar.common.config.AppConstants.FETCH_TASK_TIMEOUT_DEFAULT
import ai.platon.pulsar.common.config.CapabilityTypes.FETCH_TASK_TIMEOUT
import ai.platon.pulsar.common.config.ImmutableConfig
import java.time.Duration

/**
 * A general chrome option set:
 * Capabilities {
        acceptInsecureCerts: false,
        acceptSslCerts: false,
        applicationCacheEnabled: false,
        browserConnectionEnabled: false,
        browserName: chrome,
        chrome: {
            chromedriverVersion: 2.44.609551 (5d576e9a44fe4c...,
            userDataDir: /tmp/.org.chromium.Chromium...
        },
        cssSelectorsEnabled: true,
        databaseEnabled: false,
        goog:chromeOptions: {debuggerAddress: localhost:43001},
        handlesAlerts: true,
        hasTouchScreen: false,
        javascriptEnabled: true,
        locationContextEnabled: true,
        mobileEmulationEnabled: false,
        nativeEvents: true,
        networkConnectionEnabled: false,
        pageLoadStrategy: none,
        platform: LINUX,
        platformName: LINUX,
        rotatable: false,
        setWindowRect: true,
        takesHeapSnapshot: true,
        takesScreenshot: true,
        unexpectedAlertBehaviour: ignore,
        unhandledPromptBehavior: ignore,
        version: 69.0.3497.100,
        webStorageEnabled: true
    }
 * */
open class WebDriverSettings(conf: ImmutableConfig): BrowserSettings(conf) {

    companion object {
        val POLLING_DRIVER_TIMEOUT = "polling.driver.timeout"
        val POLLING_DRIVER_TIMEOUT_DEFAULT = Duration.ofSeconds(60)
    }

    val fetchTaskTimeout get() = conf.getDuration(FETCH_TASK_TIMEOUT, FETCH_TASK_TIMEOUT_DEFAULT)
    val pollingDriverTimeout get() = conf.getDuration(POLLING_DRIVER_TIMEOUT, POLLING_DRIVER_TIMEOUT_DEFAULT)

    // Special
    // var mobileEmulationEnabled = true

    open fun formatViewPort(delimiter: String = ","): String {
        return "${screenViewport.width}$delimiter${screenViewport.height}"
    }

    open fun createGeneralOptions(): MutableMap<String, Any> {
        val generalOptions = mutableMapOf<String, Any>()

        // generalOptions.setCapability("browserLanguage", "zh_CN")
        // generalOptions.setCapability("resolution", "${viewPort.width}x${viewPort.height}")

        return generalOptions
    }

    open fun createChromeOptions(generalOptions: Map<String, Any>): ChromeOptions {
        val chromeOptions = ChromeOptions()
        chromeOptions.merge(generalOptions)

        // rewrite proxy argument
        chromeOptions.removeArgument("proxy")
        chromeOptions.proxyServer = generalOptions["proxy"]?.toString()

        chromeOptions.headless = isHeadless
        chromeOptions.noSandbox = noSandbox

        // TODO: window-size seems not working
        chromeOptions.addArgument("window-size", formatViewPort())
            .addArgument("pageLoadStrategy", pageLoadStrategy)
            .addArgument("throwExceptionOnScriptError", "true")

        return chromeOptions
    }
}
