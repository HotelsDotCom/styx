/*
  Copyright (C) 2013-2021 Expedia Inc.

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
package com.hotels.styx.proxy

import com.google.common.base.Charsets
import com.google.common.base.Charsets._
import com.hotels.styx._
import com.hotels.styx.api.HttpRequest.get
import com.hotels.styx.api.HttpResponseStatus._
import com.hotels.styx.api.extension.Origin
import com.hotels.styx.support.configuration.{HttpBackend, Origins}
import com.hotels.styx.support.{NettyOrigins, TestClientSupport}
import com.hotels.styx.utils.HttpTestClient
import io.micrometer.core.instrument.Tags
import io.netty.buffer.Unpooled
import io.netty.buffer.Unpooled._
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.http.HttpHeaders.Names._
import io.netty.handler.codec.http.HttpHeaders.Values._
import io.netty.handler.codec.http.HttpMethod.GET
import io.netty.handler.codec.http.HttpVersion._
import io.netty.handler.codec.http.LastHttpContent.EMPTY_LAST_CONTENT
import io.netty.handler.codec.http._
import org.scalatest.FunSpec
import org.scalatest.concurrent.Eventually

import scala.concurrent.duration.{Duration, _}


class ChunkedDownloadSpec extends FunSpec
  with StyxProxySpec
  with DefaultStyxConfiguration
  with NettyOrigins
  with TestClientSupport
  with Eventually {

  val (originOne, originOneServer) = originAndCustomResponseWebServer("NettyOrigin-01")
  val (originTwo, originTwoServer) = originAndCustomResponseWebServer("NettyOrigin-02")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    styxServer.setBackends(
      "/chunkedDownloadSpec/a/" -> HttpBackend("appOne", Origins(originOneServer)),
      "/chunkedDownloadSpec/b/" -> HttpBackend("appTwo", Origins(originTwoServer))
    )
  }

  override protected def afterAll(): Unit = {
    originOneServer.stopAsync().awaitTerminated()
    super.afterAll()
  }

  describe("Downloads of chunked data.") {

    it("Proxies a response with chunked HTTP content.") {
      originRespondingWith(response200OkWithThreeChunks("a" * 10, "b" * 20, "c" * 30))

      val request = get(styxServer.routerURL("/chunkedDownloadSpec/a/1")).build()
      val response = decodedRequest(request)

      response.status() should be(OK)
      response.contentLength().isPresent should be(false)

      assert(response.bodyAs(UTF_8) == "a" * 10 + "b" * 20 + "c" * 30, s"\nReceived incorrect content: ${response.bodyAs(UTF_8)}")
    }

    it("Proxies long lasting HTTP Chunked downloads without triggering gateway read timeout.") {
      val messageBody = "Foo bar 0123456789012345678901234567890123456789\\n" * 100
      originRespondingWith(response200OkWithSlowChunkedMessageBody(messageBody))

      val request = get(styxServer.routerURL("/chunkedDownloadSpec/a/2")).build()
      val response = decodedRequest(request)

      response.status() should be(OK)

      assert(response.bodyAs(UTF_8).hashCode() == messageBody.hashCode, s"\nReceived incorrect content: ${response.bodyAs(UTF_8)}, \nexpected: $messageBody")
    }

    it("Cancels the HTTP download request when browser closes the connection.") {
      assert(busyConnections(originTwo) == 0, "Connection remains busy.")
      assert(noAvailableConnectionsInPool(originTwo), "Connection was not closed.")

      val messageBody = "Foo bar 0123456789012345678901234567890123456789\\n" * 100
      originRespondingWith(response200OkWithSlowChunkedMessageBody(messageBody))

      val request: DefaultFullHttpRequest = nettyGetRequest("/chunkedDownloadSpec/b/3")

      val client = newTestClientInstance("localhost", styxServer.httpPort)
      client.write(request)
      ensureResponseDidNotArrive(client)

      client.disconnect()

      eventually(timeout(5 seconds)) {
        assert(busyConnections(originTwo) == 0, "Connection remains busy.")
        assert(closedConnections(originTwo) == 1)
        styxServer.meterRegistry().counter("proxy.request.cancelled", "errorCause", "responseWriteError").count() should be(1.0)
      }
    }
  }

  def busyConnections(origin: Origin) = {
    meterRegistry.find("connectionpool.busyConnections").tags(meterTags(origin)).gauge().value()
  }

  def closedConnections(origin: Origin) = {
    meterRegistry.find("connectionpool.connectionsClosed").tags(meterTags(origin)).gauge().value()
  }

  def noAvailableConnectionsInPool(origin: Origin) = {
    meterRegistry.find("connectionpool.availableConnections").tags(meterTags(origin)).gauge().value() == 0.0
  }

  def meterTags(origin: Origin): Tags = {
    Tags.of("appId", "appTwo", "originId", s"localhost:${origin.port}")
  }

  def ensureResponseDidNotArrive(client: HttpTestClient) = {
    val response = client.waitForResponse(500, MILLISECONDS).asInstanceOf[FullHttpResponse]
    assert(response == null, "Response should not have arrived yet.")
  }

  def newTestClientInstance(host: String, port: Int): HttpTestClient = {
    val client = aggregatingTestClient(host, port)
    client.connect()
    client
  }

  def nettyGetRequest(url: String): DefaultFullHttpRequest = {
    val request = new DefaultFullHttpRequest(HTTP_1_1, GET, url)
    request.headers().add(HOST, styxServer.proxyHost)
    request
  }

  def response200OkWithSlowChunkedMessageBody(messageBody: String): (ChannelHandlerContext, Any) => Unit = {
    (ctx: ChannelHandlerContext, msg: scala.Any) => {
      if (msg.isInstanceOf[LastHttpContent]) {
        val response = new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.OK)
        response.headers().set(TRANSFER_ENCODING, CHUNKED)
        ctx.writeAndFlush(response)
        sendContentInChunks(ctx, messageBody, 100 millis)
      }
    }
  }

  def response200OkWithThreeChunks(chunk1: String, chunk2: String, chunk3: String): (ChannelHandlerContext, Any) => Any = {
    (ctx: ChannelHandlerContext, msg: scala.Any) => {
      if (msg.isInstanceOf[LastHttpContent]) {
        val response = new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.OK)
        response.headers().set(TRANSFER_ENCODING, CHUNKED)
        ctx.writeAndFlush(response)
        ctx.writeAndFlush(new DefaultHttpContent(copiedBuffer(chunk1, UTF_8)))
        ctx.writeAndFlush(new DefaultHttpContent(copiedBuffer(chunk2, UTF_8)))
        ctx.writeAndFlush(new DefaultLastHttpContent(copiedBuffer(chunk3, UTF_8)))
      }
    }
  }

  def sendContentInChunks(ctx: ChannelHandlerContext, data: String, delay: Duration): Unit = {
    val chunkData = data.take(100)
    if (chunkData.length == 0) {
      ctx.writeAndFlush(EMPTY_LAST_CONTENT)
      return
    }
    ctx.writeAndFlush(new DefaultHttpContent(Unpooled.copiedBuffer(chunkData, Charsets.UTF_8)))
    Thread.sleep(delay.toMillis)
    sendContentInChunks(ctx, data.drop(100), delay)
  }
}
