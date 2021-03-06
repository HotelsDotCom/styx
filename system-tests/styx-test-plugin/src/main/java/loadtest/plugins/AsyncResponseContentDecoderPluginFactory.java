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
package loadtest.plugins;

import com.hotels.styx.api.Eventual;
import com.hotels.styx.api.HttpResponse;
import com.hotels.styx.api.LiveHttpRequest;
import com.hotels.styx.api.LiveHttpResponse;
import com.hotels.styx.api.plugins.spi.Plugin;
import com.hotels.styx.api.plugins.spi.PluginFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;

public class AsyncResponseContentDecoderPluginFactory implements PluginFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncResponseContentDecoder.class);

    @Override
    public Plugin create(PluginFactory.Environment environment) {
        AsyncPluginConfig config = environment.pluginConfig(AsyncPluginConfig.class);
        return new AsyncResponseContentDecoder(config);
    }

    private static class AsyncResponseContentDecoder extends AbstractTestPlugin {
        private final int delayMillis;
        private final int maxContentLength;

        AsyncResponseContentDecoder(AsyncPluginConfig config) {
            this.delayMillis = config.delayMillis();
            this.maxContentLength = config.maxContentLength();

            LOGGER.warn("AsyncResponseContentDecoder starting. delayMillis={}, maxContentLength={}",
                    new Object[]{this.delayMillis, this.maxContentLength});
        }

        @Override
        public Eventual<LiveHttpResponse> intercept(LiveHttpRequest request, Chain chain) {
            return chain.proceed(request)
                    .flatMap(response ->  response.aggregate(this.maxContentLength))
                    .flatMap(fullResponse -> Eventual.from(asyncEvent(this.delayMillis))
                            .map(x -> fullResponse))
                    .map(HttpResponse::stream);
        }

        static CompletableFuture<Void> asyncEvent(int delayMillis) {
            CompletableFuture<Void> result = new CompletableFuture<>();

            new Timer().schedule(new TimerTask() {
                                     @Override
                                     public void run() {
                                         result.complete(null);
                                     }
                                 },
                    delayMillis);

            return result;
        }
    }
}
