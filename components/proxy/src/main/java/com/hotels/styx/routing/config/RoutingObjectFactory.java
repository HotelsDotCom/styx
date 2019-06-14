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
package com.hotels.styx.routing.config;

import com.hotels.styx.Environment;
import com.hotels.styx.proxy.plugin.NamedPlugin;
import com.hotels.styx.routing.RoutingObject;
import com.hotels.styx.routing.RoutingObjectRecord;
import com.hotels.styx.routing.db.StyxObjectStore;
import com.hotels.styx.routing.handlers.RouteRefLookup;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * A factory for constructing HTTP handler objects from a RoutingObjectDefinition yaml config block.
 */
public interface RoutingObjectFactory {
    /**
     * Constructs a terminal action handler according to routing configuration block.
     * <p>
     * Constructs a terminal action handler for the HTTP request. The handler is constructed
     * according to the definition codified in the RoutingObjectDefinition instance.
     * The RoutingObjectFactory is a factory object for constructing any dependant routing
     * objects. The objectVariables is a map of already instantiated routing objects
     * that can be referred from the handler being built.
     * <p>
     *
     * @param parents
     * @param context
     * @param configBlock
     * @return
     */
    RoutingObject build(List<String> parents, Context context, RoutingObjectDefinition configBlock);

    /**
     * Contextual information for factory class.
     *
     * Provides contextual information for the factory class to allow HttpHandlers
     * to integrate into Styx runtime environment.
     */
    class Context {
        private final RouteRefLookup refLookup;
        private final Environment environment;
        private final StyxObjectStore<RoutingObjectRecord> routeDb;
        private final Map<String, RoutingObjectFactory> objectFactories;
        private final Iterable<NamedPlugin> plugins;
        private final Map<String, HttpInterceptorFactory> interceptorFactories;
        private final boolean requestTracking;

        public Context(
                RouteRefLookup refLookup,
                Environment environment,
                StyxObjectStore<RoutingObjectRecord> routeDb,
                Map<String, RoutingObjectFactory> objectFactories,
                Iterable<NamedPlugin> plugins,
                Map<String, HttpInterceptorFactory> interceptorFactories,
                boolean requestTracking) {
            this.refLookup = refLookup;
            this.environment = requireNonNull(environment);
            this.routeDb = requireNonNull(routeDb);
            this.objectFactories = requireNonNull(objectFactories);
            this.plugins = requireNonNull(plugins);
            this.interceptorFactories = requireNonNull(interceptorFactories);
            this.requestTracking = requestTracking;
        }

        public Environment environment() {
            return environment;
        }

        public StyxObjectStore<RoutingObjectRecord> routeDb() {
            return routeDb;
        }

        public Map<String, RoutingObjectFactory> objectFactories() {
            return objectFactories;
        }

        public Iterable<NamedPlugin> plugins() {
            return plugins;
        }

        public Map<String, HttpInterceptorFactory> interceptorFactories() {
            return interceptorFactories;
        }

        public boolean requestTracking() {
            return requestTracking;
        }

        public RouteRefLookup refLookup() {
            return refLookup;
        }
    }
}
