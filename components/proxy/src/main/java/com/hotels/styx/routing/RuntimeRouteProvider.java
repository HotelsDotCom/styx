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
package com.hotels.styx.routing;

import com.hotels.styx.api.HttpHandler2;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A namespace for runtime routing handlers.
 *
 * Q: Why `runtime`?
 * A: These handlers are available at styx runtime. This is, handlers into this
 *    namespace are being added/removed while styx is running. Not only when it
 *    starts up.
 *
 *
 */
public class RuntimeRouteProvider {

    private final ConcurrentHashMap<String, HttpHandler2> handlers = new ConcurrentHashMap<>();

    public RuntimeRouteProvider(Map<String, HttpHandler2> handlers) {
        this.handlers.putAll(handlers);
    }

    public HttpHandler2 get(String name) {
        return handlers.get(name);
    }

}
