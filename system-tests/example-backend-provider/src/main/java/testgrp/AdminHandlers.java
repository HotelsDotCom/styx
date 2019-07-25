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
package testgrp;

import com.google.common.collect.ImmutableMap;
import com.hotels.styx.api.HttpHandler;

import static com.hotels.styx.api.HttpResponse.response;
import static com.hotels.styx.api.HttpResponseStatus.OK;
import static java.nio.charset.StandardCharsets.UTF_8;


final class AdminHandlers {
    private AdminHandlers() {
    }

    static ImmutableMap<String, HttpHandler> adminHandlers(String endpoint, String responseContent) {
        return ImmutableMap.of(endpoint, (request, context) ->
                request.aggregate(1_000_000).map(anyRequest ->
                        response(OK)
                                .body(responseContent, UTF_8)
                                .build()
                                .stream()
                ));
    }
}
