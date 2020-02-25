/*
  Copyright (C) 2013-2020 Expedia Inc.

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
package com.hotels.styx.startup;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.eventbus.AsyncEventBus;
import com.hotels.styx.Environment;
import com.hotels.styx.InetServer;
import com.hotels.styx.StartupConfig;
import com.hotels.styx.StyxConfig;
import com.hotels.styx.StyxObjectRecord;
import com.hotels.styx.Version;
import com.hotels.styx.api.MetricRegistry;
import com.hotels.styx.api.configuration.Configuration;
import com.hotels.styx.api.extension.service.spi.StyxService;
import com.hotels.styx.api.metrics.codahale.CodaHaleMetricRegistry;
import com.hotels.styx.api.plugins.spi.Plugin;
import com.hotels.styx.common.format.SanitisedHttpHeaderFormatter;
import com.hotels.styx.common.format.SanitisedHttpMessageFormatter;
import com.hotels.styx.infrastructure.configuration.yaml.JsonNodeConfig;
import com.hotels.styx.proxy.plugin.NamedPlugin;
import com.hotels.styx.routing.RoutingObject;
import com.hotels.styx.routing.RoutingObjectRecord;
import com.hotels.styx.routing.RoutingObjectYamlRecord;
import com.hotels.styx.routing.config.Builtins;
import com.hotels.styx.routing.config.Builtins.StyxObjectDescriptor;
import com.hotels.styx.routing.config2.StyxObject;
import com.hotels.styx.routing.db.StyxObjectStore;
import com.hotels.styx.routing.handlers.RouteRefLookup.RouteDbRefLookup;
import com.hotels.styx.serviceproviders.ServiceProviderFactory;
import com.hotels.styx.startup.extensions.ConfiguredPluginFactory;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hotels.styx.StartupConfig.newStartupConfigBuilder;
import static com.hotels.styx.Version.readVersionFrom;
import static com.hotels.styx.infrastructure.logging.LOGBackConfigurer.initLogging;
import static com.hotels.styx.routing.config.Builtins.INTERCEPTOR_FACTORIES;
import static com.hotels.styx.routing.handlers2.SerialisersKt.objectMmapper;
import static com.hotels.styx.routing.handlers2.SerialisersKt.serverObjectMmapper;
import static com.hotels.styx.startup.StyxServerComponents.LoggingSetUp.DO_NOT_MODIFY;
import static com.hotels.styx.startup.extensions.PluginLoadingForStartup.loadPlugins;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.stream.Collectors.toList;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Configuration required to set-up the core Styx services, such as the proxy and admin servers.
 */
public class StyxServerComponents {
    private final Environment environment;
    //    private final Map<String, StyxService> services;
    private final List<NamedPlugin> plugins;
    private final StyxObjectStore<RoutingObjectRecord<RoutingObject>> routeObjectDb;
    private final StyxObjectStore<StyxObjectRecord<InetServer>> serverObjectDb = new StyxObjectStore<>();
    private final StyxObjectStore<StyxObjectRecord<StyxService>> providerObjectStore = new StyxObjectStore<>();
    private final StartupConfig startupConfig;

    private static final Logger LOGGER = getLogger(StyxServerComponents.class);
    private final StyxObject.Context styxObjectContext;
    private final Map<String, StyxObjectDescriptor<StyxObject<RoutingObject>>> routingObjectDescriptors;

    private StyxServerComponents(Builder builder) {
        StyxConfig styxConfig = requireNonNull(builder.styxConfig);
        this.routeObjectDb = builder.routingObjectDb;

        this.startupConfig = builder.startupConfig == null ? newStartupConfigBuilder().build() : builder.startupConfig;
        routingObjectDescriptors = builder.routingObjectDescriptors;
        Map<String, ? extends StyxObjectDescriptor<StyxObject<InetServer>>> serverObjectDescriptors = builder.styxServerDescriptors;

        this.environment = newEnvironment(styxConfig, builder.metricRegistry);
        builder.loggingSetUp.setUp(environment);

        // TODO In further refactoring, we will probably want this loading to happen outside of this constructor call,
        //  so that it doesn't delay the admin server from starting up
        this.plugins = builder.configuredPluginFactories.isEmpty()
                ? loadPlugins(environment)
                : loadPlugins(environment, builder.configuredPluginFactories);

        this.styxObjectContext = new StyxObject.Context(
                new RouteDbRefLookup(this.routeObjectDb),
                environment,
                routeObjectDb,
                plugins,
                INTERCEPTOR_FACTORIES,
                false);

        this.plugins.forEach(plugin -> this.environment.plugins().add(plugin));


        // TODO:
        //   - Take this outside of StyxServerComponents
        //   - Deserialise each object to `StyxObject` class.
        //   - Instantiate Styx Routing Object from `StyxObject`
        //   - Insert `RoutingObject` to StyxServerComponents via an API call.

        builder.additionalRoutingObjects.forEach(record -> {
            // record.getType() is actually object's name:
            routeObjectDb.insert(record.getType(), RoutingObjectRecord.Companion.create(
                    record.getConfig().type(),
                    record.getTags(),
                    record.getConfig(),
                    record.getConfig().build(styxObjectContext)
            )).ifPresent(previous -> previous.getRoutingObject().stop());
        });

        builder.additionalServerObjects.forEach(record -> {
            // record.getType() is actually object's name:
            StyxObject<InetServer> config = record.getConfig();

            serverObjectDb.insert(record.getType(), new StyxObjectRecord<>(
                    record.getConfig().type(),
                    record.getTags(),
                    config,
                    record.getConfig().build(styxObjectContext)
            )).ifPresent(previous -> previous.getStyxService().stop());
        });
    }

    public Environment environment() {
        return environment;
    }

    public Map<String, StyxObjectDescriptor<StyxObject<RoutingObject>>> routingObjectDescriptors() {
        return routingObjectDescriptors;
    }

    public List<NamedPlugin> plugins() {
        return plugins;
    }

    public StyxObjectStore<RoutingObjectRecord<RoutingObject>> routeDatabase() {
        return this.routeObjectDb;
    }

    public StyxObjectStore<StyxObjectRecord<StyxService>> servicesDatabase() {
        return this.providerObjectStore;
    }

    public StyxObjectStore<StyxObjectRecord<InetServer>> serversDatabase() {
        return this.serverObjectDb;
    }

    public StyxObject.Context routingObjectFactoryContext() {
        return this.styxObjectContext;
    }

    public StartupConfig startupConfig() {
        return startupConfig;
    }

    private static Environment newEnvironment(StyxConfig config, MetricRegistry metricRegistry) {

        SanitisedHttpHeaderFormatter headerFormatter = new SanitisedHttpHeaderFormatter(
                config.get("request-logging.hideHeaders", List.class).orElse(emptyList()),
                config.get("request-logging.hideCookies", List.class).orElse(emptyList()));

        SanitisedHttpMessageFormatter sanitisedHttpMessageFormatter = new SanitisedHttpMessageFormatter(headerFormatter);

        return new Environment.Builder()
                .configuration(config)
                .metricRegistry(metricRegistry)
                .buildInfo(readBuildInfo())
                .eventBus(new AsyncEventBus("styx", newSingleThreadExecutor()))
                .httpMessageFormatter(sanitisedHttpMessageFormatter)
                .build();
    }

    private static Version readBuildInfo() {
        return readVersionFrom("/version.json");
    }


    private static Map<String, JsonNode> readComponents(JsonNode root, ObjectMapper mapper) {
        Map<String, JsonNode> handlers = new HashMap<>();

        root.fields().forEachRemaining(entry -> handlers.put(entry.getKey(), entry.getValue()));

        return handlers;
    }

    /**
     * CoreConfig builder.
     */
    public static final class Builder {
        private StyxObjectStore<RoutingObjectRecord<RoutingObject>> routingObjectDb = new StyxObjectStore<>();

        private StyxConfig styxConfig;
        private LoggingSetUp loggingSetUp = DO_NOT_MODIFY;
        private List<ConfiguredPluginFactory> configuredPluginFactories = ImmutableList.of();
        private MetricRegistry metricRegistry = new CodaHaleMetricRegistry();
        private StartupConfig startupConfig;

        private static final Map<String, StyxObjectDescriptor<StyxObject<RoutingObject>>> routingObjectDescriptors = new HashMap<>();
        private static final Map<String, StyxObjectDescriptor<ServiceProviderFactory>> serviceProviderDescriptors = new HashMap<>();
        private static final Map<String, StyxObjectDescriptor<StyxObject<InetServer>>> styxServerDescriptors = new HashMap<>();

        private final List<RoutingObjectYamlRecord<RoutingObject>> additionalRoutingObjects = new ArrayList<>();
        private final List<RoutingObjectYamlRecord<InetServer>> additionalServerObjects = new ArrayList<>();

        static {
            routingObjectDescriptors.putAll(Builtins.ROUTING_OBJECT_DESCRIPTORS);
            styxServerDescriptors.putAll(Builtins.SERVER_DESCRIPTORS);

        }

        public static Builder fromConfiguration(StyxConfig styxConfig) {

            Builder builder = new Builder().styxConfig(styxConfig);

            styxConfig.get("routingObjects", JsonNode.class)
                    // Deserialise to StyxObjectDefinition block:
                    .map(a -> readComponents(a, objectMmapper(routingObjectDescriptors)))
                    .orElse(ImmutableMap.of())
                    .forEach((name, jsonNode) -> {
                        // Insert each StyxObjectDefinition to the database:
                        RoutingObjectYamlRecord<RoutingObject> yamlRecord = (RoutingObjectYamlRecord<RoutingObject>) new JsonNodeConfig(
                                jsonNode,
                                objectMmapper(routingObjectDescriptors))
                                .as(RoutingObjectYamlRecord.class);

                        builder.routingObject(name, yamlRecord.getTags(), yamlRecord.getConfig());
                    });

            styxConfig.get("servers", JsonNode.class)
                    .map(a -> readComponents(a, serverObjectMmapper(styxServerDescriptors)))
                    .orElse(ImmutableMap.of())
                    .forEach((name, jsonNode) -> {

                        RoutingObjectYamlRecord<InetServer> yamlRecord = (RoutingObjectYamlRecord<InetServer>) new JsonNodeConfig(
                                jsonNode,
                                serverObjectMmapper(styxServerDescriptors))
                                .as(RoutingObjectYamlRecord.class);

                        builder.server(name, yamlRecord.getTags(), yamlRecord.getConfig());
                    });

            return builder;
        }

        @NotNull
        public Builder routingObject(String name, StyxObject<RoutingObject> object) {
            additionalRoutingObjects.add(new RoutingObjectYamlRecord<>(name, emptySet(), object));
            return this;
        }

        @NotNull
        public Builder routingObject(String name, Set<String> tags, StyxObject<RoutingObject> object) {
            additionalRoutingObjects.add(new RoutingObjectYamlRecord<>(name, tags, object));
            return this;
        }

        @NotNull
        public Builder server(String name, StyxObject<InetServer> object) {
            additionalServerObjects.add(new RoutingObjectYamlRecord<>(name, emptySet(), object));
            return this;
        }

        @NotNull
        public Builder server(String name, Set<String> tags, StyxObject<InetServer> object) {
            additionalServerObjects.add(new RoutingObjectYamlRecord<>(name, tags, object));
            return this;
        }

        public Builder styxConfig(StyxConfig styxConfig) {
            this.styxConfig = requireNonNull(styxConfig);
            return this;
        }

        public Builder metricsRegistry(MetricRegistry metricRegistry) {
            this.metricRegistry = requireNonNull(metricRegistry);
            return this;
        }

        public Builder configuration(Configuration configuration) {
            return styxConfig(new StyxConfig(configuration));
        }

        public Builder loggingSetUp(LoggingSetUp loggingSetUp) {
            this.loggingSetUp = requireNonNull(loggingSetUp);
            return this;
        }

        @VisibleForTesting
        public Builder loggingSetUp(String logConfigLocation) {
            this.loggingSetUp = env -> initLogging(logConfigLocation, true);
            return this;
        }

        @VisibleForTesting
        public Builder plugins(Map<String, Plugin> plugins) {
            return pluginFactories(stubFactories(plugins));
        }

        private static List<ConfiguredPluginFactory> stubFactories(Map<String, Plugin> plugins) {
            return plugins.entrySet().stream().map(entry -> {
                String name = entry.getKey();
                Plugin plugin = entry.getValue();

                return new ConfiguredPluginFactory(name, any -> plugin);
            }).collect(toList());
        }

        public Builder pluginFactories(List<ConfiguredPluginFactory> configuredPluginFactories) {
            this.configuredPluginFactories = requireNonNull(configuredPluginFactories);
            return this;
        }

        public Builder startupConfig(StartupConfig startupConfig) {
            this.startupConfig = startupConfig;
            return this;
        }

        @NotNull
        public Builder routingObjectDescriptor(@NotNull StyxObjectDescriptor<StyxObject<RoutingObject>> descriptor) {
            routingObjectDescriptors.put(descriptor.type(), descriptor);
            return this;
        }

        @NotNull
        public Builder serviceProviderDescriptor(@NotNull StyxObjectDescriptor<ServiceProviderFactory> descriptor) {
            serviceProviderDescriptors.put(descriptor.type(), descriptor);
            return this;
        }

        @NotNull
        public Builder styxServerDescriptor(@NotNull StyxObjectDescriptor<StyxObject<InetServer>> descriptor) {
            styxServerDescriptors.put(descriptor.type(), descriptor);
            return this;
        }

        public StyxServerComponents build() {
            return new StyxServerComponents(this);
        }
    }

    /**
     * Set-up the logging.
     */
    public interface LoggingSetUp {
        LoggingSetUp DO_NOT_MODIFY = environment -> {
        };

        void setUp(Environment environment);
    }
}
