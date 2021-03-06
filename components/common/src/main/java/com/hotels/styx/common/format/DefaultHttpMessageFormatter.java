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
package com.hotels.styx.common.format;

import com.hotels.styx.api.HttpRequest;
import com.hotels.styx.api.HttpResponse;
import com.hotels.styx.api.LiveHttpRequest;
import com.hotels.styx.api.LiveHttpResponse;
import io.netty.handler.codec.http.HttpObject;

/**
 * Provides default formatting for requests and responses.
 */
public class DefaultHttpMessageFormatter implements HttpMessageFormatter {

    @Override
    public String formatRequest(HttpRequest request) {
        return request == null ? null : request.toString();
    }

    @Override
    public String formatRequest(LiveHttpRequest request) {
        return request == null ? null : request.toString();
    }

    @Override
    public String formatResponse(HttpResponse response) {
        return response == null ? null : response.toString();
    }

    @Override
    public String formatResponse(LiveHttpResponse response) {
        return response == null ? null : response.toString();
    }

    @Override
    public String formatNettyMessage(HttpObject message) {
        return message == null ? null : message.toString();
    }

    @Override
    public Throwable wrap(Throwable t) {
        return t;
    }
}
