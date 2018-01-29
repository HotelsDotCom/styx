/**
 * Copyright (C) 2013-2018 Expedia Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hotels.styx.client;

import com.google.common.collect.ImmutableList;
import com.hotels.styx.api.HttpClient;
import com.hotels.styx.api.HttpRequest;
import com.hotels.styx.api.HttpResponse;
import com.hotels.styx.api.Id;
import com.hotels.styx.api.client.Origin;
import com.hotels.styx.api.client.RemoteHost;
import com.hotels.styx.api.client.loadbalancing.spi.LoadBalancingStrategy;
import com.hotels.styx.api.client.retrypolicy.spi.RetryPolicy;
import com.hotels.styx.api.metrics.MetricRegistry;
import com.hotels.styx.api.metrics.codahale.CodaHaleMetricRegistry;
import com.hotels.styx.client.applications.BackendService;
import com.hotels.styx.client.applications.OriginStats;
import com.hotels.styx.client.retry.RetryNTimes;
import com.hotels.styx.client.stickysession.StickySessionLoadBalancingStrategy;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import rx.Observable;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Objects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.getFirst;
import static com.hotels.styx.api.HttpHeaderNames.CONTENT_LENGTH;
import static com.hotels.styx.api.HttpHeaderNames.TRANSFER_ENCODING;
import static com.hotels.styx.client.stickysession.StickySessionCookie.newStickySessionCookie;
import static io.netty.handler.codec.http.HttpMethod.HEAD;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * A configurable HTTP client that uses connection pooling, load balancing, etc.
 */
public final class StyxHttpClient implements HttpClient {
    private static final Logger LOGGER = getLogger(StyxHttpClient.class);

    private final Id id;
    private final RewriteRuleset rewriteRuleset;
    private final LoadBalancingStrategy loadBalancingStrategy;
    private final RetryPolicy retryPolicy;
    private final Transport transport;
    private final OriginStatsFactory originStatsFactory;
    private final BackendService backendService;
    private final MetricRegistry metricsRegistry;
    private final boolean contentValidation;
    private final StyxHeaderConfig styxHeaderConfig;

    private StyxHttpClient(Builder builder) {
        this.backendService = requireNonNull(builder.backendService);
        this.id = backendService.id();

        this.originStatsFactory = requireNonNull(builder.originStatsFactory);

        this.loadBalancingStrategy = requireNonNull(builder.loadBalancingStrategy);

        this.retryPolicy = builder.retryPolicy != null
                ? builder.retryPolicy
                : new RetryNTimes(3);

        this.rewriteRuleset = new RewriteRuleset(builder.rewriteRules);
        this.transport = requireNonNull(builder.transport);

        this.metricsRegistry = builder.metricsRegistry;
        this.contentValidation = builder.contentValidation;

        this.styxHeaderConfig = builder.styxHeaderConfig;
    }

    public boolean isHttps() {
        return backendService.tlsSettings().isPresent();
    }

    /**
     * Create a new builder.
     *
     * @return a new builder
     */
    public static Builder newHttpClientBuilder(BackendService backendService) {
        return new Builder(backendService);
    }


    Id id() {
        return id;
    }

    RetryPolicy retryPolicy() {
        return retryPolicy;
    }

    Transport transport() {
        return transport;
    }

    LoadBalancingStrategy loadBalancingStrategy() {
        return loadBalancingStrategy;
    }

    static class LBContext implements LoadBalancingStrategy.Context {
        private final HttpRequest request;
        private final Id id;
        private final OriginStatsFactory originStatsFactory;

        LBContext(HttpRequest request, Id id, OriginStatsFactory originStatsFactory) {
            this.request = requireNonNull(request);
            this.id = requireNonNull(id);
            this.originStatsFactory = requireNonNull(originStatsFactory);
        }

        @Override
        public Id appId() {
            return id;
        }

        @Override
        public HttpRequest currentRequest() {
            return request;
        }

        @Override
        public double oneMinuteRateForStatusCode5xx(Origin origin) {
            OriginStats originStats = originStatsFactory.originStats(origin);
            return originStats.oneMinuteErrorRate();
        }
    }

    private static boolean isError(HttpResponseStatus status) {
        return status.code() >= 400;
    }

    private static boolean bodyNeedsToBeRemoved(HttpRequest request, HttpResponse response) {
        return isHeadRequest(request) || isBodilessResponse(response);
    }

    private static HttpResponse responseWithoutBody(HttpResponse response) {
        return response.newBuilder()
                .header(CONTENT_LENGTH, 0)
                .removeHeader(TRANSFER_ENCODING)
                .removeBody()
                .build();
    }

    private static boolean isBodilessResponse(HttpResponse response) {
        int status = response.status().code();
        return status == 204 || status == 304 || status / 100 == 1;
    }

    private static boolean isHeadRequest(HttpRequest request) {
        return request.method().equals(HEAD);
    }

    @Override
    public Observable<HttpResponse> sendRequest(HttpRequest request) {
        HttpRequest rewrittenRequest = rewriteUrl(request);
        Optional<RemoteHost> pool = selectOrigin(rewrittenRequest);

        HttpTransaction txn = transport.send(rewrittenRequest, pool);

        RetryOnErrorHandler retryHandler = new RetryOnErrorHandler.Builder()
                .client(this)
                .attemptCount(0)
                .request(rewrittenRequest)
                .previouslyUsedOrigin(pool.orElse(null))
                .transaction(txn)
                .originStatsFactory(originStatsFactory)
                .build();

        return txn.response()
                .onErrorResumeNext(retryHandler)
                .map(this::addStickySessionIdentifier)
                .doOnError(throwable -> logError(rewrittenRequest, throwable))
                .doOnUnsubscribe(() -> {
                    pool.ifPresent(connectionPool -> originStatsFactory.originStats(connectionPool.connectionPool().getOrigin()).requestCancelled());
                    retryHandler.cancel();
                })
                .doOnNext(this::recordErrorStatusMetrics)
                .map(response -> removeUnexpectedResponseBody(request, response))
                .map(this::removeRedundantContentLengthHeader);
    }

    private static void logError(HttpRequest rewrittenRequest, Throwable throwable) {
        LOGGER.error("Error Handling request={} exceptionClass={} exceptionMessage=\"{}\"",
                new Object[]{rewrittenRequest, throwable.getClass().getName(), throwable.getMessage()});
    }

    private HttpResponse removeUnexpectedResponseBody(HttpRequest request, HttpResponse response) {
        if (contentValidation && bodyNeedsToBeRemoved(request, response)) {
            return responseWithoutBody(response);
        } else {
            return response;
        }
    }

    private HttpResponse removeRedundantContentLengthHeader(HttpResponse response) {
        if (contentValidation && response.contentLength().isPresent() && response.chunked()) {
            return response.newBuilder()
                    .removeHeader(CONTENT_LENGTH)
                    .build();
        }
        return response;
    }

    private void recordErrorStatusMetrics(HttpResponse response) {
        if (isError(response.status())) {
            metricsRegistry.counter("origins.response.status." + response.status().code()).inc();
        }
    }

    private Optional<RemoteHost> selectOrigin(HttpRequest rewrittenRequest) {
        LoadBalancingStrategy.Context lbContext = new LBContext(rewrittenRequest, id, originStatsFactory);
        Iterable<RemoteHost> votedOrigins = loadBalancingStrategy.vote(lbContext);
        return Optional.ofNullable(getFirst(votedOrigins, null));
    }

    private HttpResponse addStickySessionIdentifier(HttpResponse httpResponse) {
        if (loadBalancingStrategy() instanceof StickySessionLoadBalancingStrategy) {
            Optional<String> originId = httpResponse.header(styxHeaderConfig.originIdHeaderName());
            if (originId.isPresent()) {
                int maxAge = backendService.stickySessionConfig().stickySessionTimeoutSeconds();
                return httpResponse.newBuilder()
                        .addCookie(newStickySessionCookie(id, Id.id(originId.get()), maxAge))
                        .build();
            }
        }
        return httpResponse;
    }

    private HttpRequest rewriteUrl(HttpRequest request) {
        return rewriteRuleset.rewrite(request);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("id", id)
                .add("stickySessionConfig", backendService.stickySessionConfig())
                .add("retryPolicy", retryPolicy)
                .add("rewriteRuleset", rewriteRuleset)
                .add("loadBalancingStrategy", loadBalancingStrategy)
                .toString();
    }

    /**
     * A builder for {@link com.hotels.styx.client.StyxHttpClient}.
     */
    public static class Builder {
        private final BackendService backendService;
        private MetricRegistry metricsRegistry = new CodaHaleMetricRegistry();
        private List<RewriteRule> rewriteRules = emptyList();
        private RetryPolicy retryPolicy;
        private LoadBalancingStrategy loadBalancingStrategy;
        private boolean contentValidation;
        private StyxHeaderConfig styxHeaderConfig = new StyxHeaderConfig();
        private OriginStatsFactory originStatsFactory;
        public Transport transport;

        public Builder(BackendService backendService) {
            this.backendService = checkNotNull(backendService);
        }

        public Builder metricsRegistry(MetricRegistry metricsRegistry) {
            this.metricsRegistry = checkNotNull(metricsRegistry);
            return this;
        }

        public Builder retryPolicy(RetryPolicy retryPolicy) {
            this.retryPolicy = checkNotNull(retryPolicy);
            return this;
        }

        public Builder rewriteRules(List<? extends RewriteRule> rewriteRules) {
            this.rewriteRules = ImmutableList.copyOf(rewriteRules);
            return this;
        }


        public Builder loadBalancingStrategy(LoadBalancingStrategy loadBalancingStrategy) {
            this.loadBalancingStrategy = requireNonNull(loadBalancingStrategy);
            return this;
        }

        public Builder styxHeaderNames(StyxHeaderConfig styxHeaderConfig) {
            this.styxHeaderConfig = requireNonNull(styxHeaderConfig);
            return this;
        }

        public Builder enableContentValidation() {
            contentValidation = true;
            return this;
        }

        public Builder originStatsFactory(OriginStatsFactory originStatsFactory) {
            this.originStatsFactory = originStatsFactory;
            return this;
        }

        public Builder transport(Transport transport) {
            this.transport = transport;
            return this;
        }

        public StyxHttpClient build() {
            if (originStatsFactory == null) {
                originStatsFactory = new OriginStatsFactory(metricsRegistry);
            }
            if (transport == null) {
                transport = new Transport(backendService.id(), styxHeaderConfig);
            }
            return new StyxHttpClient(this);
        }

    }
}
