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
package com.hotels.styx.client.retry;

import com.hotels.styx.api.Environment;
import com.hotels.styx.api.configuration.Configuration;
import com.hotels.styx.api.configuration.Configuration.MapBackedConfiguration;
import com.hotels.styx.api.extension.retrypolicy.spi.RetryPolicy;
import org.junit.jupiter.api.Test;

import static com.hotels.styx.api.configuration.Configuration.EMPTY_CONFIGURATION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class RetryPolicyFactoryTest {
    private final Environment environment = mock(Environment.class);

    @Test
    public void shouldCreateRetryPolicyAccordingToConfiguration() {
        Configuration configuration = new MapBackedConfiguration().set("count", 2);

        RetryPolicy retryPolicy = new RetryPolicyFactory().create(environment, configuration);

        assertThat(retryPolicy, is(instanceOf(RetryNTimes.class)));
        assertThat(((RetryNTimes) retryPolicy).maxAttempts(), is(2));
    }

    @Test
    public void usesDefaultCountOf1IfNotSpecified() {
        RetryPolicy retryPolicy = new RetryPolicyFactory().create(environment, EMPTY_CONFIGURATION);

        assertThat(retryPolicy, is(instanceOf(RetryNTimes.class)));
        assertThat(((RetryNTimes) retryPolicy).maxAttempts(), is(1));
    }
}