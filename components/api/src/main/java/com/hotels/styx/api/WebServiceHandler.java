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
package com.hotels.styx.api;

/**
 * A web service handler that handles a {@link HttpRequest}, returning an {@link Eventual} that is expected to publish
 * a single {@link HttpResponse} value.
 */
@FunctionalInterface
public interface WebServiceHandler {
    /**
     * Processes an incoming request.
     *
     * @param request the current incoming request
     * @return an {@link Eventual} that is expected to publish a single response
     */
    Eventual<HttpResponse> handle(HttpRequest request, HttpInterceptor.Context context);
}
