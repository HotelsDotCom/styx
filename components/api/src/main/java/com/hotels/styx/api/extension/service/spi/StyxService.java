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
package com.hotels.styx.api.extension.service.spi;

import com.hotels.styx.api.HttpHandler;
import com.hotels.styx.api.StyxLifecycleListener;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static java.util.Collections.emptyMap;

/**
 * A StyxService is a background task supporting Styx proxy by performing ongoing tasks,
 * such as service discovery or monitoring origin availability.
 */
public interface StyxService extends StyxLifecycleListener {

    /**
     * Invoked when Styx core starts the service.
     * <p>
     * An implementation of start() should:
     * <p>
     * - Asynchronously initialise all resources that are necessary for running
     * the service. Especially resources that involve IO, such as opening files
     * or establishing network connections, etc.
     *
     * @return StyxFuture associated to the asynchronous initialisation task.
     * <p>
     * - The returned StyxFuture must be completed with a *null* value upon
     * successful initialisation.
     * <p>
     * - The returned StyxFuture must be completed exceptionally with a failure
     * cause when the initialisation fails.
     */
    CompletableFuture<Void> start();

    /**
     * Invoked when Styx core stops the service.
     * <p>
     * An implementation of stop() should:
     * <p>
     * - Create an asynchronous task to initialise the service. The stop() method
     * should tear down any resources that are associated with the service.
     *
     * @return StyxFuture associated to the asynchronous teardown task.
     * <p>
     * - The returned StyxFuture must be completed with a *null* value when
     * successfully released all resources.
     * <p>
     * - The returned StyxFuture must be completed exceptionally with a failure
     * cause when the resource release fails.
     */
    CompletableFuture<Void> stop();

    /**
     * An admin interface hook for the service implementation.
     *
     * @return Returns a list of named admin interface handlers.
     */
    default Map<String, HttpHandler> adminInterfaceHandlers() {
        return emptyMap();
    }

    /**
     * Derives a new service interface with added side-effects for errors.
     * This could be used for logging, metrics, etc.
     *
     * @param consumer error consumer
     * @return a new service interface
     */
    default StyxService doOnError(Consumer<Throwable> consumer) {
        StyxService parent = this;

        return new StyxService() {
            @Override
            public CompletableFuture<Void> start() {
                return parent.start().exceptionally(throwable -> {
                    consumer.accept(throwable);

                    if (throwable instanceof RuntimeException) {
                        throw (RuntimeException) throwable;
                    }

                    if (throwable instanceof Error) {
                        throw (Error) throwable;
                    }

                    throw new RuntimeException(throwable);
                });
            }

            @Override
            public CompletableFuture<Void> stop() {
                return parent.stop();
            }

            @Override
            public Map<String, HttpHandler> adminInterfaceHandlers() {
                return parent.adminInterfaceHandlers();
            }

            @Override
            public String toString() {
                return parent.toString();
            }

        };
    }
}
