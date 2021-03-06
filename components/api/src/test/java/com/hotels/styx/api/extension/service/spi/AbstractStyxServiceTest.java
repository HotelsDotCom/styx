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
package com.hotels.styx.api.extension.service.spi;

import com.hotels.styx.api.HttpResponse;
import com.hotels.styx.api.LiveHttpRequest;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.hotels.styx.api.extension.service.spi.MockContext.MOCK_CONTEXT;
import static com.hotels.styx.api.extension.service.spi.StyxServiceStatus.FAILED;
import static com.hotels.styx.api.extension.service.spi.StyxServiceStatus.RUNNING;
import static com.hotels.styx.api.extension.service.spi.StyxServiceStatus.STARTING;
import static com.hotels.styx.api.extension.service.spi.StyxServiceStatus.STOPPED;
import static com.hotels.styx.api.extension.service.spi.StyxServiceStatus.STOPPING;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AbstractStyxServiceTest {

    private final LiveHttpRequest get = LiveHttpRequest.get("/").build();

    @Test
    public void exposesNameAndStatusViaAdminInterface() throws ExecutionException, InterruptedException {
        DerivedStyxService service = new DerivedStyxService("derived-service", new CompletableFuture<>());

        HttpResponse response = Mono.from(service.adminInterfaceHandlers().get("status").handle(get, MOCK_CONTEXT)
                        .flatMap(r -> r.aggregate(1024))).block();

        assertThat(response.bodyAs(UTF_8), is("{ name: \"derived-service\" status: \"CREATED\" }"));
    }

    @Test
    public void inStartingStateWhenStartIsCalled() {
        DerivedStyxService service = new DerivedStyxService("derived-service", new CompletableFuture<>());

        CompletableFuture<Void> started = service.start();

        assertThat(service.status(), is(STARTING));
        assertThat(started.isDone(), is(false));
    }

    @Test
    public void inStartedStateWhenStartupCompletes() {
        CompletableFuture<Void> subclassStarted = new CompletableFuture<>();
        DerivedStyxService service = new DerivedStyxService("derived-service", subclassStarted);

        CompletableFuture<Void> started = service.start();

        assertThat(service.status(), is(STARTING));
        assertThat(started.isDone(), is(false));

        subclassStarted.complete(null);

        assertThat(service.status(), is(RUNNING));
        assertThat(started.isDone(), is(true));
    }

    @Test
    public void throwsExceptionFor2ndCallToStart() {
        CompletableFuture<Void> subclassStarted = new CompletableFuture<>();
        DerivedStyxService service = new DerivedStyxService("derived-service", subclassStarted);

        CompletableFuture<Void> started1st = service.start();

        assertThat(service.status(), is(STARTING));
        assertThat(started1st.isDone(), is(false));

        Exception e = assertThrows(IllegalStateException.class, () -> service.start());
        assertThat(service.status(), is(STARTING));
        assertThat(started1st.isDone(), is(false));
        assertEquals("Start 'derived-service' called in STARTING state", e.getMessage());
    }

    @Test
    public void inFailedStateAfterSubclassStartupFailure() {
        CompletableFuture<Void> subclassStarted = new CompletableFuture<>();
        DerivedStyxService service = new DerivedStyxService("derived-service", subclassStarted);

        CompletableFuture<Void> started = service.start();

        assertThat(service.status(), is(STARTING));
        assertThat(started.isDone(), is(false));

        subclassStarted.completeExceptionally(new RuntimeException("Derived failed to start"));

        assertThat(service.status(), is(FAILED));
        assertThat(started.isCompletedExceptionally(), is(true));
    }

    @Test
    public void inStoppingStateAfterStopIsCalled() {
        DerivedStyxService service = new DerivedStyxService("derived-service", completedFuture(null), new CompletableFuture<>());

        CompletableFuture<Void> started = service.start();
        assertThat(service.status(), is(RUNNING));
        assertThat(started.isDone(), is(true));

        CompletableFuture<Void> stopped = service.stop();
        assertThat(service.status(), is(STOPPING));
        assertThat(stopped.isDone(), is(false));
    }

    @Test
    public void inStoppedStateAfterSubClassHasStopped() {
        CompletableFuture<Void> subclassStopped = new CompletableFuture<>();
        DerivedStyxService service = new DerivedStyxService("derived-service", completedFuture(null), subclassStopped);

        CompletableFuture<Void> started = service.start();
        assertThat(service.status(), is(RUNNING));
        assertThat(started.isDone(), is(true));

        CompletableFuture<Void> stopped = service.stop();
        assertThat(service.status(), is(STOPPING));
        assertThat(stopped.isDone(), is(false));

        subclassStopped.complete(null);
        assertThat(service.status(), is(STOPPED));
        assertThat(stopped.isDone(), is(true));
    }

    @Test
    public void inFailedStateWhenSubclassFailsToStop() {
        CompletableFuture<Void> subclassStopped = new CompletableFuture<>();
        DerivedStyxService service = new DerivedStyxService("derived-service", completedFuture(null), subclassStopped);

        CompletableFuture<Void> started = service.start();
        assertThat(service.status(), is(RUNNING));
        assertThat(started.isDone(), is(true));

        CompletableFuture<Void> stopped = service.stop();
        assertThat(service.status(), is(STOPPING));
        assertThat(stopped.isDone(), is(false));

        subclassStopped.completeExceptionally(new RuntimeException("derived service failed to stop"));
        assertThat(service.status(), is(FAILED));
        assertThat(stopped.isCompletedExceptionally(), is(true));
    }

    @Test
    public void throwsExceptionWhenStopIsCalledInFailedState() {
        CompletableFuture<Void> subclassStopped = new CompletableFuture<>();
        DerivedStyxService service = new DerivedStyxService("derived-service", completedFuture(null), subclassStopped);

        CompletableFuture<Void> started = service.start();
        assertThat(service.status(), is(RUNNING));
        assertThat(started.isDone(), is(true));

        CompletableFuture<Void> stopped = service.stop();
        assertThat(service.status(), is(STOPPING));
        assertThat(stopped.isDone(), is(false));

        subclassStopped.completeExceptionally(new RuntimeException("derived service failed to stop"));
        assertThat(service.status(), is(FAILED));
        assertThat(stopped.isCompletedExceptionally(), is(true));

        Exception e = assertThrows(IllegalStateException.class, () -> service.stop());
        assertEquals("Service 'derived-service' stopped in FAILED state", e.getMessage());
    }


    static class DerivedStyxService extends AbstractStyxService {
        private final CompletableFuture<Void> startFuture;
        private final CompletableFuture<Void> stopFuture;

        DerivedStyxService(String name, CompletableFuture startFuture) {
            this(name, startFuture, completedFuture(null));
        }

        DerivedStyxService(String name, CompletableFuture<Void> startFuture, CompletableFuture<Void> stopFuture) {
            super(name);
            this.startFuture = startFuture;
            this.stopFuture = stopFuture;
        }

        @Override
        protected CompletableFuture<Void> startService() {
            return startFuture;
        }

        @Override
        protected CompletableFuture<Void> stopService() {
            return stopFuture;
        }
    }

}