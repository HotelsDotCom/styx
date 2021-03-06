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
package com.hotels.styx.client.connectionpool;

import com.hotels.styx.api.Id;
import com.hotels.styx.api.exceptions.StyxException;
import com.hotels.styx.api.extension.Origin;

import java.util.Optional;

import static java.lang.String.format;

/**
 * Launched when a connection pool is unable to establish new TCP connections.
 */
public class MaxPendingConnectionsExceededException extends ResourceExhaustedException implements StyxException {
    private final Origin origin;
    private final int pendingConnectionsCount;
    private final int maxPendingConnectionsPerHost;

    public MaxPendingConnectionsExceededException(Origin origin, int pendingConnectionsCount, int maxPendingConnectionsPerHost) {
        super(format("Maximum allowed pending connections exceeded for origin=%s. pendingConnectionsCount=%d "
                        + "is greater than maxPendingConnectionsPerHost=%d", origin,
                pendingConnectionsCount, maxPendingConnectionsPerHost));
        this.origin = origin;
        this.pendingConnectionsCount = pendingConnectionsCount;
        this.maxPendingConnectionsPerHost = maxPendingConnectionsPerHost;
    }

    public int pendingConnectionsCount() {
        return pendingConnectionsCount;
    }

    public int maxPendingConnectionsPerHost() {
        return maxPendingConnectionsPerHost;
    }

    @Override
    public Optional<Id> origin() {
        return Optional.of(origin.id());
    }

    @Override
    public Id application() {
        return origin.applicationId();
    }
}
