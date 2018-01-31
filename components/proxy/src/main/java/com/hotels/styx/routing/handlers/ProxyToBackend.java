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
package com.hotels.styx.routing.handlers;

import com.fasterxml.jackson.databind.JsonNode;
import com.hotels.styx.Environment;
import com.hotels.styx.api.HttpClient;
import com.hotels.styx.api.HttpHandler2;
import com.hotels.styx.api.HttpInterceptor;
import com.hotels.styx.api.HttpRequest;
import com.hotels.styx.api.HttpResponse;
import com.hotels.styx.api.client.Connection;
import com.hotels.styx.api.client.ConnectionPool;
import com.hotels.styx.client.OriginStatsFactory;
import com.hotels.styx.client.OriginsInventory;
import com.hotels.styx.client.applications.BackendService;
import com.hotels.styx.client.connectionpool.ExpiringConnectionFactory;
import com.hotels.styx.client.connectionpool.ConnectionPoolFactory;
import com.hotels.styx.client.netty.connectionpool.NettyConnectionFactory;
import com.hotels.styx.infrastructure.configuration.yaml.JsonNodeConfig;
import com.hotels.styx.proxy.BackendServiceClientFactory;
import com.hotels.styx.routing.config.HttpHandlerFactory;
import com.hotels.styx.routing.config.RouteHandlerDefinition;
import com.hotels.styx.routing.config.RouteHandlerFactory;
import rx.Observable;

import java.util.List;

import static com.hotels.styx.client.HttpRequestOperationFactory.Builder.httpRequestOperationFactoryBuilder;
import static com.hotels.styx.routing.config.RoutingSupport.append;
import static com.hotels.styx.routing.config.RoutingSupport.missingAttributeError;
import static java.lang.String.join;

/**
 * Routing object that proxies a request to a configured backend.
 */
public class ProxyToBackend implements HttpHandler2 {
    private final HttpClient client;

    private ProxyToBackend(HttpClient client) {
        this.client = client;
    }

    @Override
    public Observable<HttpResponse> handle(HttpRequest request, HttpInterceptor.Context context) {
        return client.sendRequest(request);
    }

    /**
     * ProxyToBackend factory that instantiates an object from the Yaml configuration.
     */
    public static class ConfigFactory implements HttpHandlerFactory {
        private Environment environment;
        private final BackendServiceClientFactory clientFactory;

        public ConfigFactory(Environment environment, BackendServiceClientFactory clientFactory) {
            this.environment = environment;
            this.clientFactory = clientFactory;
        }

        @Override
        public HttpHandler2 build(List<String> parents, RouteHandlerFactory builder, RouteHandlerDefinition configBlock) {
            JsonNodeConfig jsConfig = new JsonNodeConfig(configBlock.config());

            BackendService backendService = jsConfig
                    .get("backend", BackendService.class)
                    .orElseThrow(() ->  missingAttributeError(configBlock, join(".", parents), "backend"));

            int clientWorkerThreadsCount = environment.styxConfig().proxyServerConfig().clientWorkerThreadsCount();

            boolean requestLoggingEnabled = environment.styxConfig().get("request-logging.outbound.enabled", Boolean.class)
                    .orElse(false);

            boolean longFormat = environment.styxConfig().get("request-logging.outbound.longFormat", Boolean.class)
                    .orElse(false);

            JsonNode origins = jsConfig
                    .get("backend.origins", JsonNode.class)
                    .orElseThrow(() -> missingAttributeError(configBlock, join(".", append(parents, "backend")), "origins"));

            OriginStatsFactory originStatsFactory = new OriginStatsFactory(environment.metricRegistry());

            Connection.Factory connectionFactory = new NettyConnectionFactory.Builder()
                    .name("Styx")
                    .httpRequestOperationFactory(
                            httpRequestOperationFactoryBuilder()
                                    .flowControlEnabled(true)
                                    .originStatsFactory(originStatsFactory)
                                    .requestLoggingEnabled(requestLoggingEnabled)
                                    .responseTimeoutMillis(backendService.responseTimeoutMillis())
                                    .longFormat(longFormat)
                            .build())
                    .clientWorkerThreadsCount(clientWorkerThreadsCount)
                    .tlsSettings(backendService.tlsSettings().orElse(null))
                    .build();

            ConnectionPool.Settings poolSettings = backendService.connectionPoolConfig();

            if (poolSettings.connectionExpirationSeconds() > 0) {
                connectionFactory = new ExpiringConnectionFactory(poolSettings.connectionExpirationSeconds(), connectionFactory);
            }

            ConnectionPool.Factory connectionPoolFactory = new ConnectionPoolFactory.Builder()
                    .connectionFactory(connectionFactory)
                    .connectionPoolSettings(poolSettings)
                    .metricRegistry(environment.metricRegistry())
                    .build();

            OriginsInventory inventory = new OriginsInventory.Builder(backendService.id())
                    .eventBus(environment.eventBus())
                    .metricsRegistry(environment.metricRegistry())
                    .connectionPoolFactory(connectionPoolFactory)
                    .initialOrigins(backendService.origins())
                    .build();
            return new ProxyToBackend(clientFactory.createClient(backendService, inventory, originStatsFactory));
        }

    }
}
