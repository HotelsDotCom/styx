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
package com.hotels.styx.proxy.interceptors;

import com.hotels.styx.api.Eventual;
import com.hotels.styx.api.HttpInterceptor;
import com.hotels.styx.api.HttpInterceptor.Chain;
import com.hotels.styx.api.LiveHttpResponse;
import com.hotels.styx.api.LiveHttpRequest;

/**
 * Request recording handler. useful for testing.
 *
 */
public final class RequestRecordingChain implements Chain {
    private final Chain delegate;
    private LiveHttpRequest request;

    private RequestRecordingChain(Chain delegate) {
        this.delegate = delegate;
    }

    public static RequestRecordingChain requestRecordingChain(Chain delegate) {
        return new RequestRecordingChain(delegate);
    }

    @Override
    public Eventual<LiveHttpResponse> proceed(LiveHttpRequest request) {
        this.request = request;
        return delegate.proceed(request);
    }

    @Override
    public HttpInterceptor.Context context() {
        return delegate.context();
    }

    public LiveHttpRequest recordedRequest() {
        return request;
    }
}
