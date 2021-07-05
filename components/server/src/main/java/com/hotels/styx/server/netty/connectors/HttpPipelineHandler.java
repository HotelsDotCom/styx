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


import com.google.common.annotations.VisibleForTesting;
import com.hotels.styx.api.Buffer;
import com.hotels.styx.api.ByteStream;
import com.hotels.styx.api.ContentOverflowException;
import com.hotels.styx.api.Eventual;
import com.hotels.styx.api.HttpHandler;
import com.hotels.styx.api.HttpResponseStatus;
import com.hotels.styx.api.Id;
import com.hotels.styx.api.LiveHttpRequest;
import com.hotels.styx.api.LiveHttpResponse;
import com.hotels.styx.api.exceptions.NoAvailableHostsException;
import com.hotels.styx.api.exceptions.OriginUnreachableException;
import com.hotels.styx.api.exceptions.ResponseTimeoutException;
import com.hotels.styx.api.exceptions.StyxException;
import com.hotels.styx.api.exceptions.TransportLostException;
import com.hotels.styx.api.plugins.spi.PluginException;
import com.hotels.styx.client.BadHttpResponseException;
import com.hotels.styx.client.StyxClientException;
import com.hotels.styx.client.connectionpool.ResourceExhaustedException;
import com.hotels.styx.common.FsmEventProcessor;
import com.hotels.styx.common.QueueDrainingEventProcessor;
import com.hotels.styx.common.StateMachine;
import com.hotels.styx.common.content.ConsumerDisconnectedException;
import com.hotels.styx.server.BadRequestException;
import com.hotels.styx.server.HttpErrorStatusListener;
import com.hotels.styx.server.HttpInterceptorContext;
import com.hotels.styx.server.NoServiceConfiguredException;
import com.hotels.styx.server.RequestProgressListener;
import com.hotels.styx.server.RequestTimeoutException;
import com.hotels.styx.server.track.RequestTracker;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.TooLongFrameException;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;

import javax.net.ssl.SSLHandshakeException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.hotels.styx.api.HttpHeaderNames.CONNECTION;
import static com.hotels.styx.api.HttpHeaderNames.CONTENT_LENGTH;
import static com.hotels.styx.api.HttpResponseStatus.BAD_GATEWAY;
import static com.hotels.styx.api.HttpResponseStatus.BAD_REQUEST;
import static com.hotels.styx.api.HttpResponseStatus.GATEWAY_TIMEOUT;
import static com.hotels.styx.api.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static com.hotels.styx.api.HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE;
import static com.hotels.styx.api.HttpResponseStatus.REQUEST_TIMEOUT;
import static com.hotels.styx.api.HttpResponseStatus.SERVICE_UNAVAILABLE;
import static com.hotels.styx.api.HttpVersion.HTTP_1_1;
import static com.hotels.styx.api.LiveHttpResponse.response;
import static com.hotels.styx.api.Metrics.name;
import static com.hotels.styx.server.HttpErrorStatusListener.IGNORE_ERROR_STATUS;
import static com.hotels.styx.server.RequestProgressListener.IGNORE_REQUEST_PROGRESS;
import static com.hotels.styx.server.netty.connectors.HttpPipelineHandler.State.ACCEPTING_REQUESTS;
import static com.hotels.styx.server.netty.connectors.HttpPipelineHandler.State.SENDING_RESPONSE;
import static com.hotels.styx.server.netty.connectors.HttpPipelineHandler.State.SENDING_RESPONSE_CLIENT_CLOSED;
import static com.hotels.styx.server.netty.connectors.HttpPipelineHandler.State.TERMINATED;
import static com.hotels.styx.server.netty.connectors.HttpPipelineHandler.State.WAITING_FOR_RESPONSE;
import static com.hotels.styx.server.netty.connectors.ResponseEnhancer.DO_NOT_MODIFY_RESPONSE;
import static io.netty.channel.ChannelFutureListener.CLOSE;
import static io.netty.handler.codec.http.LastHttpContent.EMPTY_LAST_CONTENT;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Passes request to HTTP Pipeline.
 * If a response is successfully returned, it is written by a NettyHttpResponseWriter.
 * If an error occurs, an error response is generated.
 */
public class HttpPipelineHandler extends SimpleChannelInboundHandler<LiveHttpRequest> {
    private static final Logger LOGGER = getLogger(HttpPipelineHandler.class);

    private static final ExceptionStatusMapper EXCEPTION_STATUSES = new ExceptionStatusMapper.Builder()
            .add(REQUEST_TIMEOUT, RequestTimeoutException.class)
            .add(BAD_GATEWAY,
                    OriginUnreachableException.class,
                    NoAvailableHostsException.class,
                    NoServiceConfiguredException.class,
                    BadHttpResponseException.class,
                    ContentOverflowException.class
            )
            .add(SERVICE_UNAVAILABLE, ResourceExhaustedException.class)
            .add(GATEWAY_TIMEOUT, ResponseTimeoutException.class)
            .add(INTERNAL_SERVER_ERROR, StyxClientException.class)
            .build();

    private final HttpHandler httpPipeline;
    private final HttpErrorStatusListener httpErrorStatusListener;
    private final HttpResponseWriterFactory responseWriterFactory;

    private final RequestProgressListener statsSink;
    private final MeterRegistry meterRegistry;
    private final String meterPrefix;

    private final StateMachine<State> stateMachine;
    private final ResponseEnhancer responseEnhancer;
    private final boolean secure;
    private final CharSequence originsHeaderName;

    private volatile Subscription subscription;
    private volatile LiveHttpRequest ongoingRequest;
    private volatile LiveHttpResponse ongoingResponse;
    private volatile LiveHttpRequest prematureRequest;

    private volatile CompletableFuture<Void> future;
    private volatile QueueDrainingEventProcessor eventProcessor;

    private final RequestTracker tracker;

    private HttpPipelineHandler(Builder builder, RequestTracker tracker) {
        this.responseEnhancer = requireNonNull(builder.responseEnhancer);
        this.httpPipeline = requireNonNull(builder.httpPipeline);
        this.httpErrorStatusListener = requireNonNull(builder.httpErrorStatusListener);
        this.responseWriterFactory = requireNonNull(builder.responseWriterFactory);
        this.statsSink = requireNonNull(builder.progressListener);
        this.stateMachine = createStateMachine();
        this.meterRegistry = builder.meterRegistrySupplier.get();
        this.meterPrefix = builder.meterPrefix;
        this.secure = builder.secure;
        this.tracker = tracker;
        this.originsHeaderName = builder.originsHeaderName;
    }

    private StateMachine<State> createStateMachine() {
        return new StateMachine.Builder<State>()
                .initialState(ACCEPTING_REQUESTS)

                .transition(ACCEPTING_REQUESTS, RequestReceivedEvent.class, event -> onLegitimateRequest(event.request, event.ctx))
                .transition(ACCEPTING_REQUESTS, ChannelInactiveEvent.class, event -> TERMINATED)
                .transition(ACCEPTING_REQUESTS, ChannelExceptionEvent.class, event -> onChannelExceptionWhenAcceptingRequests(event.ctx, event.cause))
                .transition(ACCEPTING_REQUESTS, ResponseObservableCompletedEvent.class, event -> ACCEPTING_REQUESTS)

                .transition(WAITING_FOR_RESPONSE, ResponseReceivedEvent.class, event -> onResponseReceived(event.response, event.ctx))
                .transition(WAITING_FOR_RESPONSE, RequestReceivedEvent.class, event -> onSpuriousRequest(event.request, WAITING_FOR_RESPONSE))
                .transition(WAITING_FOR_RESPONSE, ChannelInactiveEvent.class, event -> onChannelInactive())
                .transition(WAITING_FOR_RESPONSE, ChannelExceptionEvent.class, event -> onChannelExceptionWhenWaitingForResponse(event.ctx, event.cause))
                .transition(WAITING_FOR_RESPONSE, ResponseObservableErrorEvent.class, event -> onResponseObservableError(event.ctx, event.cause, event.requestId))
                .transition(WAITING_FOR_RESPONSE, ResponseObservableCompletedEvent.class, event -> onResponseObservableCompletedTooSoon(event.ctx, event.requestId))

                .transition(SENDING_RESPONSE, ResponseSentEvent.class, event -> onResponseSent(event.ctx))
                .transition(SENDING_RESPONSE, ResponseWriteErrorEvent.class, event -> onResponseWriteError(event.ctx, event.cause))
                .transition(SENDING_RESPONSE, ChannelInactiveEvent.class, event -> SENDING_RESPONSE_CLIENT_CLOSED)
                .transition(SENDING_RESPONSE, ChannelExceptionEvent.class, event -> onChannelExceptionWhenSendingResponse(event.ctx, event.cause))
                .transition(SENDING_RESPONSE, ResponseObservableErrorEvent.class, event -> logError(SENDING_RESPONSE, event.cause))
                .transition(SENDING_RESPONSE, ResponseObservableCompletedEvent.class, event -> SENDING_RESPONSE)
                .transition(SENDING_RESPONSE, RequestReceivedEvent.class, event -> onPrematureRequest(event.request, event.ctx))

                .transition(SENDING_RESPONSE_CLIENT_CLOSED, ResponseSentEvent.class, event -> onResponseSentAfterClientClosed(event.ctx))
                .transition(SENDING_RESPONSE_CLIENT_CLOSED, ResponseWriteErrorEvent.class, event -> onResponseWriteError(event.ctx, event.cause))
                .transition(SENDING_RESPONSE_CLIENT_CLOSED, ChannelExceptionEvent.class, event -> logError(SENDING_RESPONSE_CLIENT_CLOSED, event.cause))
                .transition(SENDING_RESPONSE_CLIENT_CLOSED, ResponseObservableErrorEvent.class, event -> logError(SENDING_RESPONSE_CLIENT_CLOSED, event.cause))
                .transition(SENDING_RESPONSE_CLIENT_CLOSED, ResponseObservableCompletedEvent.class, event -> SENDING_RESPONSE_CLIENT_CLOSED)

                .transition(TERMINATED, ChannelInactiveEvent.class, event -> TERMINATED)

                .onInappropriateEvent((state, event) -> {
                    LOGGER.warn(warningMessage(event.getClass().getSimpleName()));
                    return state;
                })

                .build();
    }

    private State logError(State state, Throwable cause) {
        httpErrorStatusListener.proxyingFailure(ongoingRequest, ongoingResponse, cause);
        return state;
    }

    @VisibleForTesting
    State state() {
        return this.stateMachine.currentState();
    }

    enum State {
        ACCEPTING_REQUESTS,
        WAITING_FOR_RESPONSE,
        SENDING_RESPONSE,
        SENDING_RESPONSE_CLIENT_CLOSED,
        TERMINATED
    }


    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        String loggingPrefix = format("%s -> %s", ctx.channel().remoteAddress(), ctx.channel().localAddress());
        this.eventProcessor = new QueueDrainingEventProcessor(new FsmEventProcessor<>(stateMachine, (throwable, state) -> {
        }, loggingPrefix));
        super.channelActive(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, LiveHttpRequest request) throws Exception {
        eventProcessor.submit(new RequestReceivedEvent(request, ctx));
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        eventProcessor.submit(new ChannelInactiveEvent());

        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        eventProcessor.submit(new ChannelExceptionEvent(ctx, cause));
    }

    private State onSpuriousRequest(LiveHttpRequest request, State state) {
        LOGGER.warn(warningMessage("message='Spurious request received while handling another request', spuriousRequest=" + request));

        meterRegistry.counter(name(meterPrefix, "request.cancelled"),
                "errorCause", "spuriousRequest"
        ).increment();
        statsSink.onTerminate(ongoingRequest.id());
        tracker.endTrack(ongoingRequest);
        cancelSubscription();
        return TERMINATED;
    }

    private State onPrematureRequest(LiveHttpRequest request, ChannelHandlerContext ctx) {
        if (prematureRequest != null) {
            LOGGER.warn(warningMessage("message='Spurious request received while handling another request', spuriousRequest=%s" + request));

            meterRegistry.counter(name(meterPrefix, "request.cancelled"),
                    "errorCause", "spuriousRequest"
            ).increment();
            cancelSubscription();
            statsSink.onTerminate(ongoingRequest.id());
            tracker.endTrack(ongoingRequest);
            future.cancel(false);
            ctx.close();
            return TERMINATED;
        } else {
            prematureRequest = request;
            return this.state();
        }
    }

    private State onLegitimateRequest(LiveHttpRequest request, ChannelHandlerContext ctx) {
        statsSink.onRequest(request.id());
        LiveHttpRequest v11Request = request.newBuilder().version(HTTP_1_1).build();
        tracker.trackRequest(request, () -> this.state().toString());
        ongoingRequest = request;

        // Note, it is possible for onCompleted, onError, and onNext events to be emitted in
        // the same call stack as "onLegitimateRequest" handler. This happens when a plugin
        // generates a response.
        try {
            Eventual<LiveHttpResponse> responseEventual = httpPipeline.handle(
                    v11Request,
                    new HttpInterceptorContext(this.secure, remoteAddress(ctx), ctx.executor()));
            responseEventual.subscribe(new BaseSubscriber<LiveHttpResponse>() {
                @Override
                public void hookOnSubscribe(Subscription s) {
                    subscription = s;
                    s.request(1);
                }

                @Override
                public void hookOnComplete() {
                    eventProcessor.submit(new ResponseObservableCompletedEvent(ctx, request.id()));
                }

                @Override
                public void hookOnError(Throwable cause) {
                    eventProcessor.submit(new ResponseObservableErrorEvent(ctx, cause, request.id()));
                }

                @Override
                public void hookOnNext(LiveHttpResponse response) {
                    eventProcessor.submit(new ResponseReceivedEvent(response, ctx));
                }
            });

            return WAITING_FOR_RESPONSE;
        } catch (Throwable cause) {
            LiveHttpResponse response = exceptionToResponse(cause, request, originsHeaderName);
            httpErrorStatusListener.proxyErrorOccurred(request, remoteAddress(ctx), response.status(), cause);
            statsSink.onTerminate(request.id());
            tracker.endTrack(ongoingRequest);
            if (ctx.channel().isActive()) {
                respondAndClose(ctx, response);
            }
            return TERMINATED;
        }
    }

    private State onResponseReceived(LiveHttpResponse response, ChannelHandlerContext ctx) {
        ongoingResponse = response;
        HttpResponseWriter httpResponseWriter = responseWriterFactory.create(ctx);

        future = httpResponseWriter.write(responseEnhancer.enhance(ongoingResponse, ongoingRequest));
        future.handle((ignore, cause) -> {
            if (cause != null) {
                eventProcessor.submit(new ResponseWriteErrorEvent(ctx, cause));
            } else {
                eventProcessor.submit(new ResponseSentEvent(ctx));
            }
            return null;
        });

        return SENDING_RESPONSE;
    }

    private State onResponseSent(ChannelHandlerContext ctx) {
        statsSink.onComplete(ongoingRequest.id(), ongoingResponse.status().code());
        tracker.endTrack(ongoingRequest);
        if (ongoingRequest.keepAlive()) {
            ongoingRequest = null;
            ongoingResponse = null;

            if (prematureRequest != null) {
                eventProcessor.submit(new RequestReceivedEvent(prematureRequest, ctx));
                prematureRequest = null;
            }

            return ACCEPTING_REQUESTS;
        } else {
            ongoingRequest = null;
            ctx.close();
            return TERMINATED;
        }
    }

    private State onResponseSentAfterClientClosed(ChannelHandlerContext ctx) {
        statsSink.onComplete(ongoingRequest.id(), ongoingResponse.status().code());
        tracker.endTrack(ongoingRequest);
        ongoingRequest = null;
        ctx.close();
        return TERMINATED;
    }

    private State onResponseWriteError(ChannelHandlerContext ctx, Throwable cause) {
        meterRegistry.counter(name(meterPrefix, "request.cancelled"),
                "errorCause", "responseWriteError"
        ).increment();
        cancelSubscription();
        statsSink.onTerminate(ongoingRequest.id());
        tracker.endTrack(ongoingRequest);
        ctx.channel().writeAndFlush(EMPTY_LAST_CONTENT).addListener(CLOSE);

        httpErrorStatusListener.proxyWriteFailure(ongoingRequest, ongoingResponse, cause);

        return TERMINATED;
    }

    private State onChannelInactive() {
        meterRegistry.counter(name(meterPrefix, "request.cancelled"),
                "errorCause", "channelInactive"
        ).increment();
        if (future != null) {
            LOGGER.warn(warningMessage("message=onChannelInactive"));
            future.cancel(false);
        }
        cancelSubscription();
        statsSink.onTerminate(ongoingRequest.id());
        tracker.endTrack(ongoingRequest);
        return TERMINATED;
    }

    private State onChannelExceptionWhenSendingResponse(ChannelHandlerContext ctx, Throwable cause) {
        meterRegistry.counter(name(meterPrefix, "request.cancelled"),
                "errorCause", "channelExceptionWhileSendingResponse"
        ).increment();
        cancelSubscription();
        statsSink.onTerminate(ongoingRequest.id());
        tracker.endTrack(ongoingRequest);
        ctx.channel().writeAndFlush(EMPTY_LAST_CONTENT).addListener(CLOSE);
        httpErrorStatusListener.proxyErrorOccurred(cause);

        return TERMINATED;
    }

    private State onChannelExceptionWhenWaitingForResponse(ChannelHandlerContext ctx, Throwable cause) {
        meterRegistry.counter(name(meterPrefix, "request.cancelled"),
                "errorCause", "channelExceptionWhileWaitingForResponse"
        ).increment();
        statsSink.onTerminate(ongoingRequest.id());
        tracker.endTrack(ongoingRequest);
        cancelSubscription();
        return handleChannelException(ctx, cause);
    }

    private State onChannelExceptionWhenAcceptingRequests(ChannelHandlerContext ctx, Throwable cause) {
        // An exception might be caused by a bad request. Therefore handle
        // the exception as if a request had been received:
        return handleChannelException(ctx, cause);
    }

    private State handleChannelException(ChannelHandlerContext ctx, Throwable cause) {
        Throwable sslException = sslException(cause);
        if (sslException != null) {
            if (ctx.channel().isActive()) {
                ctx.channel().close();
            }

            LOGGER.info("SSL handshake failure from incoming connection "
                            + "cause=\"{}\", "
                            + "serverAddress={}, "
                            + "clientAddress={}",
                    sslException.getMessage(),
                    ctx.channel().localAddress(),
                    ctx.channel().remoteAddress());

            return TERMINATED;
        }

        if (!isIoException(cause)) {
            LiveHttpResponse response = exceptionToResponse(cause, ongoingRequest, originsHeaderName);
            httpErrorStatusListener.proxyErrorOccurred(response.status(), cause);
            if (ctx.channel().isActive()) {
                respondAndClose(ctx, response);
            }
        }
        return TERMINATED;
    }

    private static Throwable sslException(Throwable cause) {
        if (cause.getCause() != null && cause.getCause() instanceof SSLHandshakeException) {
            return cause.getCause();
        } else {
            return null;
        }
    }

    private void respondAndClose(ChannelHandlerContext ctx, LiveHttpResponse response) {
        HttpResponseWriter responseWriter = responseWriterFactory.create(ctx);
        CompletableFuture<Void> future = responseWriter.write(response);
        future.handle((ignore, reason) -> {
            if (future.isCompletedExceptionally()) {
                LOGGER.error(warningMessage("message='Unable to send error', response=" + reason));
            }
            ctx.close();
            return null;
        });
    }

    private State onResponseObservableError(ChannelHandlerContext ctx, Throwable cause, Object requestId) {
        if (!ongoingRequest.id().equals(requestId)) {
            return this.state();
        }

        meterRegistry.counter(name(meterPrefix, "request.cancelled"),
                "errorCause", "responseError"
        ).increment();
        cancelSubscription();

        LOGGER.error(warningMessage(format("message='Error proxying request', requestId=%s cause=%s", requestId, cause)));

        if (cause instanceof ConsumerDisconnectedException) {
            return TERMINATED;
        }

        LiveHttpResponse response = exceptionToResponse(cause, ongoingRequest, originsHeaderName);
        responseWriterFactory.create(ctx)
                .write(response)
                .handle((ignore, exception) -> {
                    if (exception != null) {
                        httpErrorStatusListener.proxyErrorOccurred(cause);
                        httpErrorStatusListener.proxyErrorOccurred(exception);
                    } else {
                        httpErrorStatusListener.proxyErrorOccurred(ongoingRequest, remoteAddress(ctx), response.status(), cause);
                        statsSink.onComplete(ongoingRequest.id(), response.status().code());
                        tracker.endTrack(ongoingRequest);
                    }
                    ctx.close();
                    return null;
                })
                .handle((ignore, exception) -> {
                    statsSink.onTerminate(ongoingRequest.id());
                    tracker.endTrack(ongoingRequest);
                    if (exception != null) {
                        LOGGER.error(warningMessage("message='Error during write completion handling'"), exception);
                    }
                    return null;
                });

        return TERMINATED;
    }

    private State onResponseObservableCompletedTooSoon(ChannelHandlerContext ctx, Object requestId) {
        meterRegistry.counter(name(meterPrefix, "request.cancelled"),
                "errorCause", "observableCompletedTooSoon"
        ).increment();

        if (!ongoingRequest.id().equals(requestId)) {
            return this.state();
        }

        cancelSubscription();
        statsSink.onTerminate(ongoingRequest.id());
        tracker.endTrack(ongoingRequest);
        responseWriterFactory.create(ctx).write(response(INTERNAL_SERVER_ERROR).build())
                .handle((dontCare, ignore) -> ctx.close());
        return TERMINATED;
    }

    private static boolean isIoException(Throwable throwable) {
        return throwable instanceof IOException;
    }

    private LiveHttpResponse exceptionToResponse(Throwable cause, LiveHttpRequest request, CharSequence originsHeaderName) {
        HttpResponseStatus status = status(cause instanceof PluginException
                ? cause.getCause()
                : cause);

        String message = status.code() >= 500 ? "Site temporarily unavailable." : status.description();

        LiveHttpResponse.Transformer builder = responseEnhancer.enhance(
                response(status)
                        .body(new ByteStream(Flux.just(new Buffer(message, UTF_8))))
                        .build()
                        .newBuilder(), request)
                .header(CONTENT_LENGTH, message.getBytes(UTF_8).length)
                .header(CONNECTION, "close");

        if (originsHeaderName != null && originFromException(cause) != null) {
            return builder.header(originsHeaderName, originFromException(cause))
                    .build();
        } else {
            return builder.build();
        }
    }

    private String originFromException(Throwable cause) {
        if (cause instanceof StyxException) {
            StyxException c = (StyxException) cause;
            return c.origin()
                    .map(Id::toString)
                    .orElse(Optional.ofNullable(c.application())
                            .map(Id::toString)
                            .orElse(null));

        } else {
            return null;
        }
    }

    private static HttpResponseStatus status(Throwable exception) {
        return EXCEPTION_STATUSES.statusFor(exception)
                .orElseGet(() -> {
                    if (exception instanceof DecoderException) {
                        Throwable cause = exception.getCause();

                        if (cause instanceof BadRequestException) {
                            if (cause.getCause() instanceof TooLongFrameException) {
                                return REQUEST_ENTITY_TOO_LARGE;
                            }

                            return BAD_REQUEST;
                        }
                    } else if (exception instanceof TransportLostException) {
                        return BAD_GATEWAY;
                    }

                    return INTERNAL_SERVER_ERROR;
                });
    }

    private String warningMessage(String msg) {
        return format("%s, state=%s, request=%s, ongoingResponse=%s, prematureRequest=%s",
                msg, state(), ongoingRequest, ongoingResponse, prematureRequest);
    }

    @FunctionalInterface
    interface HttpResponseWriterFactory {
        HttpResponseWriter create(ChannelHandlerContext ctx);
    }

    private static class RequestReceivedEvent {
        final LiveHttpRequest request;
        final ChannelHandlerContext ctx;

        RequestReceivedEvent(LiveHttpRequest request, ChannelHandlerContext ctx) {
            this.request = request;
            this.ctx = ctx;
        }
    }

    private static class ResponseReceivedEvent {
        private final LiveHttpResponse response;
        private final ChannelHandlerContext ctx;

        ResponseReceivedEvent(LiveHttpResponse response, ChannelHandlerContext ctx) {
            this.response = response;
            this.ctx = ctx;
        }
    }

    private static class ResponseSentEvent {
        private final ChannelHandlerContext ctx;

        ResponseSentEvent(ChannelHandlerContext ctx) {
            this.ctx = ctx;
        }
    }

    private static class ResponseWriteErrorEvent {
        private final Throwable cause;
        private final ChannelHandlerContext ctx;

        ResponseWriteErrorEvent(ChannelHandlerContext ctx, Throwable cause) {
            this.ctx = ctx;
            this.cause = cause;
        }
    }

    private static class ChannelInactiveEvent {
    }

    private static class ChannelExceptionEvent {
        ChannelHandlerContext ctx;
        Throwable cause;

        ChannelExceptionEvent(ChannelHandlerContext ctx, Throwable cause) {
            this.ctx = ctx;
            this.cause = cause;
        }
    }

    private static class ResponseObservableErrorEvent {
        private final ChannelHandlerContext ctx;
        private final Throwable cause;
        private final Object requestId;

        ResponseObservableErrorEvent(ChannelHandlerContext ctx, Throwable cause, Object requestId) {
            this.ctx = ctx;
            this.cause = cause;
            this.requestId = requestId;
        }
    }

    private static class ResponseObservableCompletedEvent {
        private final ChannelHandlerContext ctx;
        private final Object requestId;

        ResponseObservableCompletedEvent(ChannelHandlerContext ctx, Object requestId) {
            this.ctx = ctx;
            this.requestId = requestId;
        }
    }

    private void cancelSubscription() {
        if (subscription != null) {
            subscription.cancel();
        }
    }

    /**
     * Builds instances of HttpPipelineHandler.
     */
    public static class Builder {
        private final HttpHandler httpPipeline;

        private ResponseEnhancer responseEnhancer = DO_NOT_MODIFY_RESPONSE;
        private HttpErrorStatusListener httpErrorStatusListener = IGNORE_ERROR_STATUS;
        private RequestProgressListener progressListener = IGNORE_REQUEST_PROGRESS;
        private HttpResponseWriterFactory responseWriterFactory = HttpResponseWriter::new;
        private Supplier<MeterRegistry> meterRegistrySupplier = CompositeMeterRegistry::new;
        private String meterPrefix;
        private RequestTracker tracker = RequestTracker.NO_OP;
        private boolean secure;
        private CharSequence originsHeaderName;

        /**
         * Constructs a new builder.
         *
         * @param httpPipeline the HTTP pipeline
         */
        public Builder(HttpHandler httpPipeline) {
            this.httpPipeline = requireNonNull(httpPipeline);
        }

        /**
         * Sets the response enhancer. By default, the response will not be enhanced.
         *
         * @param responseEnhancer response enhancer.
         * @return this builder
         */
        public Builder responseEnhancer(ResponseEnhancer responseEnhancer) {
            this.responseEnhancer = requireNonNull(responseEnhancer);
            return this;
        }

        /**
         * Sets the HTTP error status listener. By default, the errors will ignored.
         *
         * @param httpErrorStatusListener the HTTP error status listener
         * @return this builder
         */
        public Builder errorStatusListener(HttpErrorStatusListener httpErrorStatusListener) {
            this.httpErrorStatusListener = requireNonNull(httpErrorStatusListener);
            return this;
        }

        /**
         * Sets the progress listener. By default, the progress will ignored.
         *
         * @param progressListener the progress listener
         * @return this builder
         */
        public Builder progressListener(RequestProgressListener progressListener) {
            this.progressListener = requireNonNull(progressListener);
            return this;
        }

        /**
         * Sets the response writer factory. By default, an instance of {@link HttpResponseWriter} will be created.
         *
         * @param responseWriterFactory the response writer factory
         * @return this builder
         */
        Builder responseWriterFactory(HttpResponseWriterFactory responseWriterFactory) {
            this.responseWriterFactory = requireNonNull(responseWriterFactory);
            return this;
        }

        /**
         * Sets the meter registry. By default, the metrics will not be available.
         *
         * @param meterRegistry the meter registry
         * @return this builder
         */
        public Builder meterRegistry(MeterRegistry meterRegistry) {
            requireNonNull(meterRegistry);
            this.meterRegistrySupplier = () -> meterRegistry;
            return this;
        }

        public Builder meterPrefix(String meterPrefix) {
            this.meterPrefix = meterPrefix;
            return this;
        }

        public Builder secure(boolean secure) {
            this.secure = secure;
            return this;
        }

        public Builder requestTracker(RequestTracker tracker) {
            this.tracker = requireNonNull(tracker);
            return this;
        }

        public Builder xOriginsHeader(CharSequence originsHeaderName) {
            this.originsHeaderName = originsHeaderName;
            return this;
        }

        /**
         * Builds a new instance based on the configured properties.
         *
         * @return a new instance
         */
        public HttpPipelineHandler build() {
            return new HttpPipelineHandler(this, this.tracker);
        }

        HttpPipelineHandler buildForIoExceptionTest() {
            return new HttpPipelineHandler(this, this.tracker) {
                @Override
                protected void channelRead0(ChannelHandlerContext ctx, LiveHttpRequest request) throws Exception {
                    throw new IOException("Connection reset by peer");
                }
            };
        }
    }

    private static InetSocketAddress remoteAddress(ChannelHandlerContext ctx) {
        if (ctx.channel() instanceof EmbeddedChannel) {
            return new InetSocketAddress(0);
        }

        return (InetSocketAddress) ctx.channel().remoteAddress();
    }
}
