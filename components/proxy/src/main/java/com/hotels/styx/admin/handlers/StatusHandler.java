/*
  Copyright (C) 2013-2018 Expedia Inc.

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
package com.hotels.styx.admin.handlers;

import com.hotels.styx.api.FullHttpResponse;
import com.hotels.styx.api.HttpHandler;
import com.hotels.styx.api.HttpInterceptor;
import com.hotels.styx.api.HttpRequest;
import com.hotels.styx.api.HttpResponse;
import com.hotels.styx.api.StyxObservable;
import com.hotels.styx.api.messages.HttpResponseStatus;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.net.MediaType.PLAIN_TEXT_UTF_8;
import static com.hotels.styx.api.messages.HttpResponseStatus.OK;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Delegates to another handler and then replaces the response body with "OK" if the status code is 200 OK or "NOT_OK" otherwise.
 */
public class StatusHandler implements HttpHandler {
    private final HttpHandler handler;

    /**
     * Constructs an instance, with another handler to delegate to.
     *
     * @param handler a handler to delegate to
     */
    public StatusHandler(HttpHandler handler) {
        this.handler = checkNotNull(handler);
    }

    @Override
    public StyxObservable<HttpResponse> handle(HttpRequest request, HttpInterceptor.Context context) {
        return handler.handle(request, context)
                .flatMap(response -> response.toFullResponse(0xffffff))
                .map(response ->
                        response.newBuilder()
                                .contentType(PLAIN_TEXT_UTF_8.toString())
                                .body(statusContent(response.status()), UTF_8)
                                .build())
                .map(FullHttpResponse::toStreamingResponse);
    }

    private static String statusContent(HttpResponseStatus status) {
        return OK.equals(status) ? "OK" : "NOT_OK";
    }
}
