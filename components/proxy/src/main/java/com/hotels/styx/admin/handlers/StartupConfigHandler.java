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
package com.hotels.styx.admin.handlers;

import com.google.common.collect.ImmutableMap;
import com.hotels.styx.StartupConfig;
import com.hotels.styx.common.http.handler.StaticBodyHttpHandler;

import static com.google.common.net.MediaType.HTML_UTF_8;
import static java.util.stream.Collectors.joining;

/**
 * Displays information about what settings Styx started up with.
 */
public class StartupConfigHandler extends StaticBodyHttpHandler {
    /**
     * Construct a new instance.
     *
     * @param startupConfig Styx Configuration
     */
    public StartupConfigHandler(StartupConfig startupConfig) {
        super(HTML_UTF_8, render(startupConfig));
    }

    private static String render(StartupConfig startupConfig) {
        return ImmutableMap.of(
                "Styx Home", startupConfig.styxHome(),
                "Config File Location", startupConfig.configFileLocation(),
                "Log Config Location", startupConfig.logConfigLocation())
                .entrySet().stream()
                .map(entry -> entry.getKey() + "='" + entry.getValue() + "'")
                .collect(joining("<br />", "<html><body>", "</body></html>"));
    }
}
