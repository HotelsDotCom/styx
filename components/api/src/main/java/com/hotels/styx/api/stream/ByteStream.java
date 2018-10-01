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
package com.hotels.styx.api.stream;

import com.hotels.styx.api.Buffer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Byte stream class.
 */
public class ByteStream implements Publisher<Buffer> {
    private Publisher<Buffer> stream;

    public ByteStream(Publisher<Buffer> stream) {
        this.stream = requireNonNull(stream);
    }

    public ByteStream map(Function<Buffer, Buffer> mapping) {
        return new ByteStream(Flux.from(stream).map(mapping));
    }

    public ByteStream discard() {
        return new ByteStream(Flux.from(stream)
                .doOnNext(buffer -> buffer.delegate().release())
                .filter(buffer -> false));
    }

    // TODO: This could return a ByteStream of aggregated content
    public CompletableFuture<Buffer> aggregate(int maxContentBytes) {
        return new AggregateOperator(this.stream, maxContentBytes)
                .apply();
    }

    @Override
    public void subscribe(Subscriber<? super Buffer> subscriber) {
        stream.subscribe(subscriber);
    }
}
