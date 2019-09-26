/*
  Copyright (C) 2013-2019 Expedia Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */
package com.hotels.styx.logging

import ch.qos.logback.classic.Level.INFO
import com.hotels.styx.StyxConfig
import com.hotels.styx.StyxServer
import com.hotels.styx.api.HttpHeaderNames
import com.hotels.styx.api.HttpRequest
import com.hotels.styx.client.StyxHttpClient
import com.hotels.styx.startup.StyxServerComponents
import com.hotels.styx.support.matchers.LoggingTestSupport
import io.kotlintest.Spec
import io.kotlintest.specs.FeatureSpec
import reactor.core.publisher.toMono

class HttpMessageLoggingSpec : FeatureSpec() {

    init {
        feature("Styx request/response logging") {

            scenario("Logger should hide cookies and headers") {
                val proxyHost = "${styxServer.proxyHttpAddress().hostName}:${styxServer.proxyHttpAddress().port}"

                client.send(HttpRequest.get("/a/path")
                        .header(HttpHeaderNames.HOST, proxyHost)
                        .header("header1", "h1")
                        .header("header2", "h2")
                        .header("cookie", "cookie1=c1;cookie2=c2")
                        .build())
                        .toMono()
                        .block()!!

                val expectedRequest = Regex("requestId=[-a-z0-9]+, secure=false, origin=null, "
                    + "request=\\{version=HTTP/1.1, method=GET, uri=/a/path, headers=\\[Host=localhost:[0-9]+, header1=\\*\\*\\*\\*, header2=h2, cookie=cookie1=\\*\\*\\*\\*;cookie2=c2\\], id=[-a-z0-9]+\\}")

                val expectedResponse = Regex("requestId=[-a-z0-9]+, secure=false, "
                    + "response=\\{version=HTTP/1.1, status=200 OK, headers=\\[header1=\\*\\*\\*\\*, header2=h2, cookie=cookie1=\\*\\*\\*\\*;cookie2=c2, Via=1.1 styx\\]\\}")

                logger.log().shouldContain(INFO, expectedRequest)
                logger.log().shouldContain(INFO, expectedResponse)
            }
        }
    }

    val logger = LoggingTestSupport("com.hotels.styx.http-messages.inbound")

    val client: StyxHttpClient = StyxHttpClient.Builder().build()

    val yamlText = """
        proxy:
          connectors:
            http:
              port: 0

            https:
              port: 0
              sslProvider: JDK
              sessionTimeoutMillis: 300000
              sessionCacheSize: 20000
              
        request-logging:
          inbound:
            enabled: true
            longFormat: true
          hideCookies:
            - cookie1
          hideHeaders:
            - header1

        admin:
          connectors:
            http:
              port: 0

        routingObjects:
          root:
            type: StaticResponseHandler
            config:
              status: 200
              content: ""
              headers:
                - name: "header1"
                  value: "h1"
                - name: "header2"
                  value: "h2" 
                - name: "cookie"
                  value: "cookie1=c1;cookie2=c2"

        httpPipeline: root
      """.trimIndent()

    val styxServer = StyxServer(StyxServerComponents.Builder()
            .styxConfig(StyxConfig.fromYaml(yamlText))
            .build())

    override fun beforeSpec(spec: Spec) {
        styxServer.startAsync().awaitRunning()
    }

    override fun afterSpec(spec: Spec) {
        styxServer.stopAsync().awaitTerminated()
    }

}