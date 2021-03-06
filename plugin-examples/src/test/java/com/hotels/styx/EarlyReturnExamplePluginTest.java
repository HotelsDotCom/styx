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
package com.hotels.styx;

import com.hotels.styx.api.Eventual;
import com.hotels.styx.api.HttpInterceptor;
import com.hotels.styx.api.HttpResponse;
import com.hotels.styx.api.LiveHttpRequest;
import com.hotels.styx.api.LiveHttpResponse;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


/**
 * This tests the behaviours added in the EarlyReturnExamplePlugin.
 */

public class EarlyReturnExamplePluginTest {

    @Test
    public void returnsEarlyWhenHeaderIsPresent() {

        EarlyReturnExamplePlugin plugin = new EarlyReturnExamplePlugin();

        LiveHttpRequest request = LiveHttpRequest.get("/")
                .header("X-Respond", "foo")
                .build();

        HttpInterceptor.Chain chain = request1 -> Eventual.of(LiveHttpResponse.response().build());

        Eventual<LiveHttpResponse> eventualLive = plugin.intercept(request, chain);
        Eventual<HttpResponse> eventual = eventualLive.flatMap(response -> response.aggregate(100));

        HttpResponse response = Mono.from(eventual).block();

        assertThat(response.bodyAs(UTF_8), is("Responding from plugin"));
    }

}
