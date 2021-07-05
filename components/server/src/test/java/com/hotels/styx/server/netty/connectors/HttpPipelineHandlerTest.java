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
package com.hotels.styx.server.netty.connectors;

import com.hotels.styx.api.ContentOverflowException;
import com.hotels.styx.api.Eventual;
import com.hotels.styx.api.HttpHandler;
import com.hotels.styx.api.HttpInterceptor;
import com.hotels.styx.api.HttpResponse;
import com.hotels.styx.api.LiveHttpRequest;
import com.hotels.styx.api.LiveHttpResponse;
import com.hotels.styx.client.StyxClientException;
import com.hotels.styx.server.BadRequestException;
import com.hotels.styx.server.HttpErrorStatusListener;
import com.hotels.styx.server.RequestStatsCollector;
import com.hotels.styx.server.RequestTimeoutException;
import com.hotels.styx.server.netty.codec.NettyToStyxRequestDecoder;
import com.hotels.styx.server.netty.connectors.HttpPipelineHandler.HttpResponseWriterFactory;
import com.hotels.styx.server.netty.connectors.HttpPipelineHandler.State;
import com.hotels.styx.support.JustATestException;
import com.hotels.styx.support.matchers.LoggingTestSupport;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.net.ssl.SSLHandshakeException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static ch.qos.logback.classic.Level.INFO;
import static ch.qos.logback.classic.Level.WARN;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.toArray;
import static com.hotels.styx.api.HttpHeaderNames.CONNECTION;
import static com.hotels.styx.api.HttpHeaderNames.CONTENT_LENGTH;
import static com.hotels.styx.api.HttpHeaderValues.CLOSE;
import static com.hotels.styx.api.HttpResponseStatus.BAD_GATEWAY;
import static com.hotels.styx.api.HttpResponseStatus.BAD_REQUEST;
import static com.hotels.styx.api.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static com.hotels.styx.api.HttpResponseStatus.OK;
import static com.hotels.styx.api.HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE;
import static com.hotels.styx.api.HttpResponseStatus.REQUEST_TIMEOUT;
import static com.hotels.styx.api.LiveHttpRequest.get;
import static com.hotels.styx.api.LiveHttpResponse.response;
import static com.hotels.styx.api.Metrics.name;
import static com.hotels.styx.server.RequestStatsCollector.REQUEST_OUTSTANDING;
import static com.hotels.styx.server.netty.connectors.HttpPipelineHandler.State.ACCEPTING_REQUESTS;
import static com.hotels.styx.server.netty.connectors.HttpPipelineHandler.State.SENDING_RESPONSE;
import static com.hotels.styx.server.netty.connectors.HttpPipelineHandler.State.SENDING_RESPONSE_CLIENT_CLOSED;
import static com.hotels.styx.server.netty.connectors.HttpPipelineHandler.State.TERMINATED;
import static com.hotels.styx.server.netty.connectors.HttpPipelineHandler.State.WAITING_FOR_RESPONSE;
import static com.hotels.styx.server.netty.connectors.ResponseEnhancer.DO_NOT_MODIFY_RESPONSE;
import static com.hotels.styx.support.matchers.LoggingEventMatcher.loggingEvent;
import static com.hotels.styx.support.netty.HttpMessageSupport.httpMessageToBytes;
import static com.hotels.styx.support.netty.HttpMessageSupport.httpRequest;
import static com.hotels.styx.support.netty.HttpMessageSupport.httpRequestAsBuf;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class HttpPipelineHandlerTest {
    private final HttpHandler respondingHandler = (request, context) -> Eventual.of(response(OK).build());
    private final HttpHandler doNotRespondHandler = (request, context) -> new Eventual<>(Mono.never());

    private HttpErrorStatusListener errorListener;
    private MeterRegistry metrics;

    // Cannot use lambda expression below, because spy() does not understand them.
    private final HttpHandler respondingPipeline = spy(new HttpHandler() {
        @Override
        public Eventual<LiveHttpResponse> handle(LiveHttpRequest request, HttpInterceptor.Context context) {
            return respondingHandler.handle(request, context);
        }
    });

    private ChannelHandlerContext ctx;
    private EmitterProcessor<LiveHttpResponse> responseObservable;
    private EmitterProcessor<LiveHttpResponse> responseObservable2;
    private CompletableFuture<Void> writerFuture;
    private HttpResponseWriter responseWriter;
    private HttpPipelineHandler handler;
    private HttpHandler pipeline;
    private HttpResponseWriterFactory responseWriterFactory;
    private RequestStatsCollector statsCollector;
    private LiveHttpRequest request;
    private LiveHttpRequest request2;
    private LiveHttpResponse response;
    private LiveHttpResponse response2;
    private AtomicBoolean responseUnsubscribed;
    private AtomicBoolean responseUnsubscribed2;

    private ResponseEnhancer responseEnhancer;

    private LoggingTestSupport logger;

    @BeforeEach
    public void setUp() throws Exception {
        logger = new LoggingTestSupport(HttpPipelineHandler.class);

        statsCollector = mock(RequestStatsCollector.class);
        errorListener = mock(HttpErrorStatusListener.class);
        ctx = mockCtx();
        responseObservable = EmitterProcessor.create();
        responseUnsubscribed = new AtomicBoolean(false);

        writerFuture = new CompletableFuture<>();

        responseWriter = mock(HttpResponseWriter.class);
        when(responseWriter.write(nullable(LiveHttpResponse.class))).thenReturn(writerFuture);

        responseWriterFactory = mock(HttpResponseWriterFactory.class);
        when(responseWriterFactory.create(nullable(ChannelHandlerContext.class)))
                .thenReturn(responseWriter);

        pipeline = mock(HttpHandler.class);
        when(pipeline.handle(nullable(LiveHttpRequest.class), nullable(HttpInterceptor.Context.class))).thenReturn(new Eventual<>(Flux.from(responseObservable.doOnCancel(() -> responseUnsubscribed.set(true)))));

        request = get("/foo").id("REQUEST-1-ID").build();
        response = response().build();

        responseEnhancer = mock(ResponseEnhancer.class);
        when(responseEnhancer.enhance(nullable(LiveHttpResponse.Transformer.class), nullable(LiveHttpRequest.class))).thenAnswer(invocationOnMock -> invocationOnMock.getArguments()[0]);
        when(responseEnhancer.enhance(nullable(LiveHttpResponse.class), nullable(LiveHttpRequest.class))).thenAnswer(invocationOnMock -> invocationOnMock.getArguments()[0]);

        setupHandlerTo(ACCEPTING_REQUESTS);
    }

    private void setUpFor2Requests() throws Exception {
        responseObservable2 = EmitterProcessor.create();
        responseUnsubscribed2 = new AtomicBoolean(false);
        response2 = response().build();

        CompletableFuture<Void> writerFuture2 = new CompletableFuture<>();

        HttpResponseWriter responseWriter2 = mock(HttpResponseWriter.class);
        when(responseWriter2.write(any(LiveHttpResponse.class))).thenReturn(writerFuture2);

        responseWriterFactory = mock(HttpResponseWriterFactory.class);
        when(responseWriterFactory.create(anyObject()))
                .thenReturn(responseWriter)
                .thenReturn(responseWriter2);

        pipeline = mock(HttpHandler.class);
        when(pipeline.handle(anyObject(), any(HttpInterceptor.Context.class)))
                .thenReturn(new Eventual<>(responseObservable.doOnCancel(() -> responseUnsubscribed.set(true))))
                .thenReturn(new Eventual<>(responseObservable2.doOnCancel(() -> responseUnsubscribed2.set(true))));

        request2 = get("/bar").id("REQUEST-2-ID").build();

        setupHandlerTo(ACCEPTING_REQUESTS);
    }

    @AfterEach
    public void tearDown() {
        logger.stop();
    }

    @Test
    public void mapsWrappedBadRequestExceptionToBadRequest400ResponseCode() {
        EmbeddedChannel channel = buildEmbeddedChannel(handlerWithMocks());

        String badUri = "/no5_such3_file7.pl?\"><script>alert(73541);</script>56519<script>alert(1)</script>0e134";

        channel.writeInbound(httpMessageToBytes(httpRequest(GET, badUri)));
        DefaultHttpResponse response = (DefaultHttpResponse) channel.readOutbound();

        assertThat(response.getStatus(), is(io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST));
        verify(responseEnhancer).enhance(nullable(LiveHttpResponse.Transformer.class), eq(null));
        verify(errorListener, only()).proxyErrorOccurred(eq(BAD_REQUEST), nullable(DecoderException.class));
    }

    @Test
    public void mapsUnrecoverableInternalErrorsToInternalServerError500ResponseCode() {
        HttpHandler handler = (request, context) -> {
            throw new RuntimeException("Forced exception for testing");
        };
        EmbeddedChannel channel = buildEmbeddedChannel(handlerWithMocks(handler));

        channel.writeInbound(httpRequestAsBuf(GET, "http://foo.com/"));
        DefaultHttpResponse response = (DefaultHttpResponse) channel.readOutbound();

        assertThat(response.status(), is(io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR));
        verify(responseEnhancer).enhance(any(LiveHttpResponse.Transformer.class), any(LiveHttpRequest.class));
        verify(errorListener, only()).proxyErrorOccurred(any(LiveHttpRequest.class), any(InetSocketAddress.class), eq(INTERNAL_SERVER_ERROR), any(RuntimeException.class));
    }

    @Test
    public void ignoresConnectionResetException() {
        HttpPipelineHandler pipelineHandler = handlerWithMocks(respondingPipeline).buildForIoExceptionTest();
        EmbeddedChannel channel = buildEmbeddedChannel(pipelineHandler);

        channel.writeInbound(httpRequestAsBuf(GET, "http://foo.com/"));
        assertThat(channel.outboundMessages(), is(empty()));
        verifyZeroInteractions(errorListener);
    }

    @Test
    public void updatesRequestsOngoingCountOnChannelReadEvent() throws Exception {

        MeterRegistry registry = new SimpleMeterRegistry();
        HttpPipelineHandler pipelineHandler = handlerWithMocks(doNotRespondHandler)
                .responseEnhancer(DO_NOT_MODIFY_RESPONSE)
                .progressListener(new RequestStatsCollector(registry, "test"))
                .build();

        ChannelHandlerContext ctx = mockCtx();
        pipelineHandler.channelActive(ctx);
        pipelineHandler.channelRead0(ctx, get("/foo").build());

        assertThat(requestOutstandingValue(registry), is(1.0));
    }

    @Test
    public void decrementsRequestsOngoingCountOnResponseCompleted() throws Exception {
        CompletableFuture<Void> future = new CompletableFuture<>();

        HttpPipelineHandler adapter = handlerWithMocks(respondingPipeline)
                .responseEnhancer(DO_NOT_MODIFY_RESPONSE)
                .responseWriterFactory(responseWriterFactory(future))
                .build();

        adapter.channelActive(ctx);
        adapter.channelRead0(ctx, request);
        future.complete(null);

        verify(statsCollector).onComplete(eq(request.id()), eq(200));
    }

    @Test
    public void decrementsRequestsOngoingCountOnChannelInactiveWhenRequestIsOngoing() throws Exception {
        MeterRegistry registry = new SimpleMeterRegistry();
        HttpPipelineHandler adapter = handlerWithMocks(doNotRespondHandler)
                .responseEnhancer(DO_NOT_MODIFY_RESPONSE)
                .progressListener(new RequestStatsCollector(registry, "test"))
                .build();
        ChannelHandlerContext ctx = mockCtx();

        adapter.channelActive(ctx);
        adapter.channelRead0(ctx, get("/foo").build());
        assertThat(requestOutstandingValue(registry), is(1.0));

        adapter.channelInactive(ctx);
        assertThat(requestOutstandingValue(registry), is(0.0));
    }

    @Test
    public void allowsResponseObservableToCompleteAfterAfterDisconnect() throws Exception {
        handler.channelActive(ctx);
        handler.channelRead0(ctx, request);
        verify(statsCollector).onRequest(eq(request.id()));

        responseObservable.onNext(response);

        // When a remote peer (client) has received a response in full,
        // and it has closed the TCP connection:
        handler.channelInactive(ctx);

        // ... only after that the response observable completes:
        responseObservable.onComplete();

        // ... then treat it like a successfully sent response:
        writerFuture.complete(null);
        verify(statsCollector, never()).onTerminate(eq(request.id()));
        verify(statsCollector).onComplete(eq(request.id()), eq(200));
    }

    @Test
    public void responseFailureInSendingResponseClientConnectedState() throws Exception {
        RuntimeException cause = new JustATestException();

        handler.channelActive(ctx);
        handler.channelRead0(ctx, request);
        verify(statsCollector).onRequest(eq(request.id()));

        responseObservable.onNext(response);

        // Remote peer (client) has closed the connection for whatever reason:
        handler.channelInactive(ctx);

        // ... the PipelineHandler is now in SENDING_RESPONSE_CLIENT_DISCONNECTED state,
        // and response writer indicates a failure:
        writerFuture.completeExceptionally(cause);
        verify(statsCollector).onTerminate(eq(request.id()));
        verify(statsCollector, never()).onComplete(eq(request.id()), eq(200));
        assertThat(metrics.counter("test.request.cancelled", "errorCause", "responseWriteError").count(), is(1.0));

        assertThat(responseUnsubscribed.get(), is(true));
    }

    @Test
    public void channelExceptionAfterClientClosed() throws Exception {
        RuntimeException cause = new JustATestException();

        handler.channelActive(ctx);
        handler.channelRead0(ctx, request);
        verify(statsCollector).onRequest(eq(request.id()));

        responseObservable.onNext(response);

        // Remote peer (client) has closed the connection for whatever reason:
        handler.channelInactive(ctx);

        // ... but the channel exception occurs while response writer has finished:
        handler.exceptionCaught(ctx, cause);

        // Then, just log the error,
        verify(errorListener).proxyingFailure(eq(request), eq(response), eq(cause));

        // and allow response writer event (success/failure) to conclude the state machine cycle.
        assertThat(handler.state(), is(SENDING_RESPONSE_CLIENT_CLOSED));
    }

    @Test
    public void decrementsRequestsOngoingOnExceptionCaught() throws Exception {
        MeterRegistry registry = new SimpleMeterRegistry();
        HttpPipelineHandler adapter = handlerWithMocks(doNotRespondHandler)
                .progressListener(new RequestStatsCollector(registry, "test"))
                .build();

        ChannelHandlerContext ctx = mockCtx();
        adapter.channelActive(ctx);

        LiveHttpRequest request = get("/foo").build();
        adapter.channelRead0(ctx, request);
        assertThat(requestOutstandingValue(registry), is(1.0));

        adapter.exceptionCaught(ctx, new Throwable("Exception"));
        assertThat(requestOutstandingValue(registry), is(0.0));

        adapter.channelInactive(ctx);
        assertThat(requestOutstandingValue(registry), is(0.0));

        verify(responseEnhancer).enhance(any(LiveHttpResponse.Transformer.class), eq(request));
    }

    @Test
    public void proxiesRequestAndResponse() throws Exception {
        handler.channelRead0(ctx, request);
        assertThat(handler.state(), is(WAITING_FOR_RESPONSE));
        verify(statsCollector).onRequest(request.id());

        responseObservable.onNext(response);
        responseObservable.onComplete();
        assertThat(handler.state(), is(SENDING_RESPONSE));

        writerFuture.complete(null);
        assertThat(handler.state(), is(ACCEPTING_REQUESTS));
        verify(statsCollector).onComplete(request.id(), response.status().code());
        verify(ctx.channel(), never()).close();
        verify(ctx, never()).close();
    }

    @Test
    public void prematureRequestIsNotProxiedUntilPreviousResponseIsSuccessfullyWritten() throws Exception {
        setUpFor2Requests();

        // Receive first request
        handler.channelRead0(ctx, request);
        assertThat(handler.state(), is(WAITING_FOR_RESPONSE));
        verify(statsCollector).onRequest(request.id());

        // Response arrives, together with response onComplete event.
        // The response onComplete arrives *before* response has been written.
        responseObservable.onNext(response);
        responseObservable.onComplete();
        assertThat(handler.state(), is(SENDING_RESPONSE));

        // Receive second request while still sending previous response.
        handler.channelRead0(ctx, request2);

        // Ensure state machine remains SENDING_RESPONSE, and
        // the second request is NOT yet written.
        assertThat(handler.state(), is(SENDING_RESPONSE));
        verify(ctx.channel(), never()).close();
        verify(ctx, never()).close();
        requestProxiedOnlyOnce(pipeline, request);
    }

    private static void requestProxiedOnlyOnce(HttpHandler pipeline, LiveHttpRequest request) {
        ArgumentCaptor<LiveHttpRequest> captor = ArgumentCaptor.forClass(LiveHttpRequest.class);
        verify(pipeline).handle(captor.capture(), any(HttpInterceptor.Context.class));
        assertThat(captor.getValue().id(), is(request.id()));
    }

    @Test
    public void proxiesPrematureRequestAfterPreviousResponseIsSuccessfullyWritten() throws Exception {
        setUpFor2Requests();

        // Receive first request.
        handler.channelRead0(ctx, request);
        assertThat(handler.state(), is(WAITING_FOR_RESPONSE));
        verify(statsCollector).onRequest(request.id());

        // Response arrives, together with response onComplete event.
        // The response onComplete arrives *before* response has been written.
        responseObservable.onNext(response);
        responseObservable.onComplete();
        assertThat(handler.state(), is(SENDING_RESPONSE));
        verify(responseWriter).write(any(LiveHttpResponse.class));

        // Receive second request.
        handler.channelRead0(ctx, request2);
        assertThat(handler.state(), is(SENDING_RESPONSE));

        // Ensure that second request is proxied after the response for the
        // first is successfully completed.
        writerFuture.complete(null);
        requestProxiedTwice(pipeline, request, request2);
    }

    private static void requestProxiedTwice(HttpHandler pipeline, LiveHttpRequest request1, LiveHttpRequest request2) {
        ArgumentCaptor<LiveHttpRequest> captor = ArgumentCaptor.forClass(LiveHttpRequest.class);
        verify(pipeline, times(2)).handle(captor.capture(), any(HttpInterceptor.Context.class));
        assertThat(captor.getAllValues().get(0).id(), is(request1.id()));
        assertThat(captor.getAllValues().get(1).id(), is(request2.id()));
    }

    @Test
    public void closesConnectionWhenMultiplePrematureRequestsAreDetectedInSendingResponseState() throws Exception {
        setUpFor2Requests();

        // Receive request.
        handler.channelRead0(ctx, request);
        assertThat(handler.state(), is(WAITING_FOR_RESPONSE));
        verify(statsCollector).onRequest(request.id());

        // Response arrives.
        responseObservable.onNext(response);
        responseObservable.onComplete();
        assertThat(handler.state(), is(SENDING_RESPONSE));
        verify(responseWriter).write(nullable(LiveHttpResponse.class));

        // Receive second request, while still responding to the previous request.
        // This request will be queued.
        handler.channelRead0(ctx, request2);

        // Receive third request, while one pending request is already queued.
        handler.channelRead0(ctx, request2);

        // Assert that the third request triggers an error.
        assertThat(metrics.counter("test.request.cancelled", "errorCause", "spuriousRequest").count(), is(1.0));
        assertThat(writerFuture.isCancelled(), is(true));
        assertThat(responseUnsubscribed.get(), is(true));
        verify(statsCollector).onTerminate(request.id());

        assertThat(handler.state(), is(TERMINATED));
        verify(ctx).close();
    }

    @Test
    public void pluginRespondsImmediatelyWithoutProxying() throws Exception {
        // Plugin intercepts the request and generates a response. This scenario is different
        // from others because the response emitted "in-place" within the same call stack as
        // the channel read event is being processed.

        setupIdleHandlerWithPluginResponse();
        assertThat(handler.state(), is(ACCEPTING_REQUESTS));

        handler.channelRead0(ctx, request);
        assertThat(handler.state(), is(SENDING_RESPONSE));
        verify(statsCollector).onRequest(request.id());

        writerFuture.complete(null);
        assertThat(handler.state(), is(ACCEPTING_REQUESTS));
        verify(statsCollector).onComplete(request.id(), response.status().code());
        verify(ctx, never()).close();
        verify(ctx.channel(), never()).close();
    }

    private void setupIdleHandlerWithPluginResponse() throws Exception {
        pipeline = mock(HttpHandler.class);
        when(pipeline.handle(anyObject(), any(HttpInterceptor.Context.class))).thenReturn(Eventual.of(response));
        handler = createHandler(pipeline);
    }

    @Test
    public void closesTheConnectionAfterProxyingWhenConnectionHeaderHasValueClose() throws Exception {
        LiveHttpRequest oneShotRequest = get("/closeAfterThis").header(CONNECTION, CLOSE).build();

        handler.channelRead0(ctx, oneShotRequest);
        assertThat(handler.state(), is(WAITING_FOR_RESPONSE));
        verify(statsCollector).onRequest(oneShotRequest.id());

        responseObservable.onNext(response);
        responseObservable.onComplete();
        assertThat(handler.state(), is(SENDING_RESPONSE));

        writerFuture.complete(null);
        assertThat(handler.state(), is(TERMINATED));
        verify(statsCollector).onComplete(oneShotRequest.id(), response.status().code());
        verify(ctx).close();
    }

    @Test
    public void transitionsToTerminatedWhenChannelInactivatesInAcceptingRequestsState() throws Exception {
        // In IDLE state,
        // Inbound TCP gets closed
        assertThat(handler.state(), is(ACCEPTING_REQUESTS));

        handler.channelInactive(ctx);
        assertThat(handler.state(), is(TERMINATED));
    }

    @Test
    public void ioExceptionCaughtInAcceptingRequestsState() throws Exception {
        // In IDLE state,
        // An IO-exception bubbles up the Netty pipeline.
        assertThat(handler.state(), is(ACCEPTING_REQUESTS));

        handler.exceptionCaught(ctx, new IOException("Error occurred"));
        assertThat(handler.state(), is(TERMINATED));
    }

    @Test
    public void nonIoExceptionCaughtInAcceptingRequestsStateChannelRemainsActive() throws Exception {
        // In IDLE state,
        // A non-IO exception bubbles up the Netty pipeline.
        assertThat(handler.state(), is(ACCEPTING_REQUESTS));
        activateChannel(ctx);

        handler.exceptionCaught(ctx, new RuntimeException("Error occurred"));
        verify(errorListener).proxyErrorOccurred(eq(INTERNAL_SERVER_ERROR), anyObject());

        writerFuture.complete(null);
        verify(ctx).close();
        assertThat(handler.state(), is(TERMINATED));
    }

    @Test
    public void pluginPipelineThrowsAnExceptionInAcceptingRequestsState() throws Exception {
        Throwable cause = new RuntimeException("Simulated Styx plugin exception");

        pipeline = mock(HttpHandler.class);
        when(pipeline.handle(anyObject(), any(HttpInterceptor.Context.class))).thenThrow(cause);
        handler = createHandler(pipeline);
        assertThat(handler.state(), is(ACCEPTING_REQUESTS));

        handler.channelRead0(ctx, request);

        ArgumentCaptor<LiveHttpResponse> captor = ArgumentCaptor.forClass(LiveHttpResponse.class);
        verify(responseWriter).write(captor.capture());

        HttpResponse response = Mono.from(captor.getValue().aggregate(100)).block();

        assertThat(response.status(), is(INTERNAL_SERVER_ERROR));
        assertThat(response.header(CONNECTION), is(Optional.of("close")));
        assertThat(response.header(CONTENT_LENGTH), is(Optional.of("29")));
        assertThat(response.bodyAs(UTF_8), is("Site temporarily unavailable."));

        verify(responseEnhancer).enhance(any(LiveHttpResponse.Transformer.class), eq(request));
        verify(errorListener).proxyErrorOccurred(request, InetSocketAddress.createUnresolved("localhost", 2), INTERNAL_SERVER_ERROR, cause);
        verify(statsCollector).onTerminate(request.id());
        assertThat(handler.state(), is(TERMINATED));
    }

    @Test
    public void requestTimeoutExceptionOccursInAcceptingRequestsStateAndTcpConnectionRemainsActive() throws Exception {
        RuntimeException cause = new RequestTimeoutException("timeout occurred");
        handler.exceptionCaught(ctx, cause);

        verify(errorListener).proxyErrorOccurred(REQUEST_TIMEOUT, cause);
        verify(responseEnhancer).enhance(any(LiveHttpResponse.Transformer.class), eq(null));
        verify(responseWriter).write(response(REQUEST_TIMEOUT)
                .header(CONTENT_LENGTH, 15)
                .header(CONNECTION, "close")
                .build());
    }

    @Test
    public void tooLongFrameExceptionOccursInIdleStateAndTcpConnectionRemainsActive() throws Exception {
        setupHandlerTo(WAITING_FOR_RESPONSE);

        RuntimeException cause = new DecoderException("timeout occurred", new BadRequestException("bad request", new TooLongFrameException("too long frame")));
        handler.exceptionCaught(ctx, cause);

        verify(errorListener).proxyErrorOccurred(REQUEST_ENTITY_TOO_LARGE, cause);
        verify(responseEnhancer).enhance(any(LiveHttpResponse.Transformer.class), eq(request));
        verify(responseWriter).write(response(REQUEST_ENTITY_TOO_LARGE)
                .header(CONTENT_LENGTH, 24)
                .header(CONNECTION, "close")
                .build());
    }

    @Test
    public void badRequestExceptionExceptionOccursInIdleStateAndTcpConnectionRemainsActive() throws Exception {
        setupHandlerTo(WAITING_FOR_RESPONSE);

        RuntimeException cause = new DecoderException("timeout occurred", new BadRequestException("bad request", new RuntimeException("random bad request failure")));
        handler.exceptionCaught(ctx, cause);

        verify(errorListener).proxyErrorOccurred(BAD_REQUEST, cause);
        verify(responseEnhancer).enhance(any(LiveHttpResponse.Transformer.class), eq(request));
        verify(responseWriter).write(response(BAD_REQUEST)
                .header(CONTENT_LENGTH, 11)
                .header(CONNECTION, "close")
                .build());
    }

    @Test
    public void responseObservableEmitsErrorInWaitingForResponseState() throws Exception {
        // In Waiting For Response state,
        // Response observable emits error.
        setupHandlerTo(WAITING_FOR_RESPONSE);

        responseObservable.onError(new RuntimeException("Request Send Error"));

        assertThat(responseUnsubscribed.get(), is(true));
        writerFuture.complete(null);
        verify(statsCollector).onComplete(request.id(), 500);

        // NOTE: channel closure is not verified. This is because cannot mock channel future.
        assertThat(handler.state(), is(TERMINATED));
    }

    @Test
    public void responseObservableEmitsContentOverflowExceptionInWaitingForResponseState() throws Exception {
        // In Waiting For Response state,
        // When response observable emits ContentOverflowException.
        // Then respond with BAD_GATEWAY and close the channel
        setupHandlerTo(WAITING_FOR_RESPONSE);

        responseObservable.onError(new ContentOverflowException("Request Send Error"));

        assertThat(responseUnsubscribed.get(), is(true));

        ArgumentCaptor<LiveHttpResponse> captor = ArgumentCaptor.forClass(LiveHttpResponse.class);
        verify(responseWriter).write(captor.capture());

        HttpResponse response = Mono.from(captor.getValue().aggregate(100)).block();
        assertThat(response.status(), is(BAD_GATEWAY));
        assertThat(response.header(CONNECTION), is(Optional.of("close")));
        assertThat(response.header(CONTENT_LENGTH), is(Optional.of("29")));
        assertThat(response.bodyAs(UTF_8), is("Site temporarily unavailable."));

        verify(responseEnhancer).enhance(any(LiveHttpResponse.Transformer.class), eq(request));

        writerFuture.complete(null);
        verify(statsCollector).onComplete(request.id(), 502);
        verify(errorListener).proxyErrorOccurred(any(LiveHttpRequest.class), any(InetSocketAddress.class), eq(BAD_GATEWAY), any(RuntimeException.class));

        // NOTE: channel closure is not verified. This is because cannot mock channel future.
        verify(ctx).close();
        assertThat(handler.state(), is(TERMINATED));
    }

    @Test
    public void mapsStyxClientExceptionToInternalServerErrorInWaitingForResponseState() throws Exception {
        // In Waiting For Response state,
        // The response observable emits a StyxClientException.
        // Then, respond with INTERNAL_SERVER_ERROR and close the channel.
        setupHandlerTo(WAITING_FOR_RESPONSE);

        responseObservable.onError(new StyxClientException("Client error occurred", new JustATestException()));

        assertThat(responseUnsubscribed.get(), is(true));

        ArgumentCaptor<LiveHttpResponse> captor = ArgumentCaptor.forClass(LiveHttpResponse.class);
        verify(responseWriter).write(captor.capture());

        HttpResponse response = Mono.from(captor.getValue().aggregate(100)).block();
        assertThat(response.status(), is(INTERNAL_SERVER_ERROR));
        assertThat(response.header(CONNECTION), is(Optional.of("close")));
        assertThat(response.header(CONTENT_LENGTH), is(Optional.of("29")));
        assertThat(response.bodyAs(UTF_8), is("Site temporarily unavailable."));

        writerFuture.complete(null);
        verify(statsCollector).onComplete(request.id(), 500);
        verify(errorListener).proxyErrorOccurred(any(LiveHttpRequest.class), any(InetSocketAddress.class), eq(INTERNAL_SERVER_ERROR), any(RuntimeException.class));

        // NOTE: channel closure is not verified. This is because cannot mock channel future.
        verify(ctx).close();
        assertThat(handler.state(), is(TERMINATED));
    }

    @Test
    public void channelInactiveEventInWaitingForResponseState() throws Exception {
        // In Waiting For Response state,
        // Inbound TCP connection gets closed
        setupHandlerTo(WAITING_FOR_RESPONSE);

        handler.channelInactive(ctx);

        assertThat(responseUnsubscribed.get(), is(true));
        verify(statsCollector).onTerminate(request.id());
        assertThat(handler.state(), is(TERMINATED));
    }

    @Test
    public void ioExceptionInWaitingForResponseState() throws Exception {
        // In Waiting for Response state,
        // IO exception bubbles up the Netty pipeline.
        setupHandlerTo(WAITING_FOR_RESPONSE);

        handler.exceptionCaught(ctx, new IOException("TCP connection broke"));

        assertThat(responseUnsubscribed.get(), is(true));
        verify(statsCollector).onTerminate(request.id());
        assertThat(handler.state(), is(TERMINATED));
    }

    @Test
    public void nonIoExceptionInWaitingForResponseStateAndTcpConnectionIsActive() throws Exception {
        // In waiting for Response state,
        // A non-IO exception bubbles up the Netty pipeline.
        setupHandlerTo(WAITING_FOR_RESPONSE);

        RuntimeException cause = new JustATestException();
        handler.exceptionCaught(ctx, cause);

        assertThat(responseUnsubscribed.get(), is(true));
        verify(errorListener).proxyErrorOccurred(INTERNAL_SERVER_ERROR, cause);
        responseWriter.write(response(INTERNAL_SERVER_ERROR).build());
        verify(statsCollector).onTerminate(request.id());
        assertThat(handler.state(), is(TERMINATED));
    }

    @Test
    public void requestTimeoutExceptionOccursInWaitingForResponseStateAndTcpConnectionRemainsActive() throws Exception {
        setupHandlerTo(WAITING_FOR_RESPONSE);

        RuntimeException cause = new RequestTimeoutException("timeout occurred");
        handler.exceptionCaught(ctx, cause);

        verify(errorListener).proxyErrorOccurred(REQUEST_TIMEOUT, cause);
        responseWriter.write(response(REQUEST_TIMEOUT).build());
    }

    @Test
    public void tooLongFrameExceptionOccursInWaitingForResponseStateAndTcpConnectionRemainsActive() throws Exception {
        setupHandlerTo(WAITING_FOR_RESPONSE);

        RuntimeException cause = new DecoderException("timeout occurred", new BadRequestException("bad request", new TooLongFrameException("too long frame")));
        handler.exceptionCaught(ctx, cause);

        verify(errorListener).proxyErrorOccurred(REQUEST_ENTITY_TOO_LARGE, cause);
        responseWriter.write(response(REQUEST_ENTITY_TOO_LARGE).build());
    }

    @Test
    public void badRequestExceptionExceptionOccursInWaitingForResponseStateAndTcpConnectionRemainsActive() throws Exception {
        setupHandlerTo(WAITING_FOR_RESPONSE);

        RuntimeException cause = new DecoderException("timeout occurred", new BadRequestException("bad request", new RuntimeException("random bad request failure")));
        handler.exceptionCaught(ctx, cause);

        verify(errorListener).proxyErrorOccurred(BAD_REQUEST, cause);
        responseWriter.write(response(BAD_REQUEST).build());
    }

    @Test
    public void responseObservableEmitsErrorInSendingResponseState() throws Exception {
        // In Sending Response state,
        // The response observable emits a response, which is being written out.
        // Then, response observable emits an error.
        // Because we have started to write out the response, let's write out a log message, and pretend as if nothing happened.
        setupHandlerTo(SENDING_RESPONSE);

        responseObservable.onError(new RuntimeException("Spurious error occurred"));

        assertThat(handler.state(), is(SENDING_RESPONSE));

        verify(errorListener).proxyingFailure(any(LiveHttpRequest.class), any(LiveHttpResponse.class), any(Throwable.class));
    }

    @Test
    public void responseWriteFailsInSendingResponseState() throws Exception {
        // In Sending Response state,
        // The HttpResponseWriter future completes with an error.
        RuntimeException cause = new RuntimeException("Response write failed");
        setupHandlerTo(SENDING_RESPONSE);

        writerFuture.completeExceptionally(cause);

        assertThat(responseUnsubscribed.get(), is(true));
        verify(statsCollector).onTerminate(request.id());
        verify(errorListener).proxyWriteFailure(any(LiveHttpRequest.class), eq(response(OK).build()), any(RuntimeException.class));

        assertThat(handler.state(), is(TERMINATED));
    }

    @Test
    public void ioExceptionInSendingResponseState() throws Exception {
        // In Sending Response state,
        // An IO exception bubbles up the Netty pipeline
        setupHandlerTo(SENDING_RESPONSE);

        handler.exceptionCaught(ctx, new IOException(JustATestException.DEFAULT_MESSAGE));
        assertThat(responseUnsubscribed.get(), is(true));
        verify(statsCollector).onTerminate(request.id());
        assertThat(handler.state(), is(TERMINATED));
    }

    @Test
    public void anotherExceptionInSendingResponseState() throws Exception {
        // In Sending Response state,
        // A non-IO exception bubbles up the Netty pipeline
        setupHandlerTo(SENDING_RESPONSE);

        handler.exceptionCaught(ctx, new JustATestException());
        assertThat(responseUnsubscribed.get(), is(true));
        verify(statsCollector).onTerminate(request.id());
        assertThat(handler.state(), is(TERMINATED));
    }

    @Test
    public void discardsOnCompleteEventsForEarlierRequestsInWaitingForResponseState() throws Exception {
        setUpFor2Requests();

        handler.channelRead0(ctx, request);
        assertThat(handler.state(), is(WAITING_FOR_RESPONSE));

        responseObservable.onNext(response);
        assertThat(handler.state(), is(SENDING_RESPONSE));

        writerFuture.complete(null);
        assertThat(handler.state(), is(ACCEPTING_REQUESTS));

        handler.channelRead0(ctx, request2);
        assertThat(handler.state(), is(WAITING_FOR_RESPONSE));

        responseObservable.onComplete();
        assertThat(handler.state(), is(WAITING_FOR_RESPONSE));
    }

    @Test
    public void discardsOnCompleteEventsForEarlierRequestsInSendingResponseState() throws Exception {
        setUpFor2Requests();

        handler.channelRead0(ctx, request);
        assertThat(handler.state(), is(WAITING_FOR_RESPONSE));

        responseObservable.onNext(response);
        assertThat(handler.state(), is(SENDING_RESPONSE));

        writerFuture.complete(null);
        assertThat(handler.state(), is(ACCEPTING_REQUESTS));

        handler.channelRead0(ctx, request2);
        assertThat(handler.state(), is(WAITING_FOR_RESPONSE));

        responseObservable2.onNext(response2);
        assertThat(handler.state(), is(SENDING_RESPONSE));

        responseObservable.onComplete();
        assertThat(handler.state(), is(SENDING_RESPONSE));
    }

    @Test
    public void discardsOnErrorForEarlierRequestsInWaitingForResponseState() throws Exception {
        setUpFor2Requests();

        handler.channelRead0(ctx, request);
        assertThat(handler.state(), is(WAITING_FOR_RESPONSE));

        responseObservable.onNext(response);
        assertThat(handler.state(), is(SENDING_RESPONSE));

        writerFuture.complete(null);
        assertThat(handler.state(), is(ACCEPTING_REQUESTS));

        handler.channelRead0(ctx, request2);
        assertThat(handler.state(), is(WAITING_FOR_RESPONSE));

        responseObservable.onError(new JustATestException());
        assertThat(handler.state(), is(WAITING_FOR_RESPONSE));
    }

    @Test
    public void removesOngoingResponeFromLogMessages() throws Exception {
        setUpFor2Requests();

        handler.channelRead0(ctx, request);
        assertThat(handler.state(), is(WAITING_FOR_RESPONSE));

        responseObservable.onNext(response);
        assertThat(handler.state(), is(SENDING_RESPONSE));

        writerFuture.complete(null);
        assertThat(handler.state(), is(ACCEPTING_REQUESTS));

        handler.channelRead0(ctx, request);
        assertThat(handler.state(), is(WAITING_FOR_RESPONSE));

        responseObservable2.onError(new RuntimeException("error"));

        assertTrue(logger.lastMessage().getMessage().contains("ongoingResponse=null"));
    }

    @Test
    public void cancelsOngoingRequestWhenSpuriousRequestArrivesInWaitingForResponseState() throws Exception {
        // - writes EMPTY_LAST_CONTENT and closes the channel
        // - logs an error message
        // - cancels the ongoing request on the HTTP pipeline

        LiveHttpRequest spurious = get("/bar").build();
        setupHandlerTo(WAITING_FOR_RESPONSE);

        handler.channelRead0(ctx, spurious);

        verify(statsCollector).onTerminate(request.id());
        assertThat(handler.state(), is(TERMINATED));
        assertThat(logger.lastMessage(), is(
                loggingEvent(WARN, "message='Spurious request received while handling another request'.*")));
        assertThat(responseUnsubscribed.get(), is(true));
    }

    @Test
    public void logsSslHandshakeErrors() throws Exception {
        setupHandlerTo(ACCEPTING_REQUESTS);

        handler.exceptionCaught(ctx, new DecoderException(new SSLHandshakeException("Client requested protocol TLSv1.1 not enabled or not supported")));

        verify(ctx.channel()).close();
        assertThat(handler.state(), is(TERMINATED));

        assertThat(logger.lastMessage(), is(
                loggingEvent(INFO, "SSL handshake failure from incoming connection .*")));
    }

    private void setupHandlerTo(State targetState) throws Exception {
        handler = createHandler(pipeline);

        if (targetState == WAITING_FOR_RESPONSE) {
            handler.channelRead0(ctx, request);
            assertThat(handler.state(), is(WAITING_FOR_RESPONSE));
        } else if (targetState == SENDING_RESPONSE) {
            handler.channelRead0(ctx, request);
            assertThat(handler.state(), is(WAITING_FOR_RESPONSE));
            responseObservable.onNext(response);
            assertThat(handler.state(), is(SENDING_RESPONSE));
        }
    }

    private HttpPipelineHandler createHandler(HttpHandler pipeline) throws Exception {
        metrics = new SimpleMeterRegistry();
        HttpPipelineHandler handler = handlerWithMocks(pipeline)
                .responseWriterFactory(responseWriterFactory)
                .build();

        handler.channelActive(ctx);

        return handler;
    }

    private HttpPipelineHandler.Builder handlerWithMocks() {
        return handlerWithMocks(pipeline);
    }

    private HttpPipelineHandler.Builder handlerWithMocks(HttpHandler pipeline) {
        return new HttpPipelineHandler.Builder(pipeline)
                .errorStatusListener(errorListener)
                .responseEnhancer(responseEnhancer)
                .progressListener(statsCollector)
                .meterRegistry(metrics)
                .meterPrefix("test");
    }

    private static HttpResponseWriterFactory responseWriterFactory(CompletableFuture<Void> future) {
        HttpResponseWriterFactory writerFactory = mock(HttpResponseWriterFactory.class);
        HttpResponseWriter responseWriter = mock(HttpResponseWriter.class);
        when(writerFactory.create(any(ChannelHandlerContext.class))).thenReturn(responseWriter);
        when(responseWriter.write(any(LiveHttpResponse.class))).thenReturn(future);
        return writerFactory;
    }

    private static void activateChannel(ChannelHandlerContext ctx) {
        when(ctx.channel().isActive()).thenReturn(true);
    }

    private static ChannelHandlerContext mockCtx() {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        ChannelFuture future = channelFutureOk();
        Channel channel = channel();
        when(channel.writeAndFlush(nullable(Object.class))).thenReturn(future);
        when(ctx.writeAndFlush(nullable(Object.class))).thenReturn(future);
        when(ctx.channel()).thenReturn(channel);

        when(ctx.channel().localAddress()).thenReturn(InetSocketAddress.createUnresolved("localhost", 1));
        when(ctx.channel().remoteAddress()).thenReturn(InetSocketAddress.createUnresolved("localhost", 2));

        return ctx;
    }

    private static Channel channel() {
        Channel channel = mock(Channel.class);
        when(channel.isActive()).thenReturn(true);
        return channel;
    }

    private static ChannelFuture channelFutureOk() {
        ChannelFuture future = mock(ChannelFuture.class);
        when(future.isSuccess()).thenReturn(true);
        return future;
    }

    private static EmbeddedChannel buildEmbeddedChannel(HttpPipelineHandler.Builder builder) {
        return buildEmbeddedChannel(builder.build());
    }

    private static EmbeddedChannel buildEmbeddedChannel(ChannelHandler... lastHandlers) {
        Iterable<ChannelHandler> commonHandlers = asList(
                new HttpRequestDecoder(),
                new HttpObjectAggregator(6000),
                new NettyToStyxRequestDecoder.Builder().build());

        return new EmbeddedChannel(toArray(concat(commonHandlers, asList(lastHandlers)), ChannelHandler.class));
    }

    private double requestOutstandingValue(MeterRegistry registry) {
        return Optional.ofNullable(registry.find(name("test", REQUEST_OUTSTANDING)).gauge()).map(Gauge::value).orElse(0.0);
    }

}
