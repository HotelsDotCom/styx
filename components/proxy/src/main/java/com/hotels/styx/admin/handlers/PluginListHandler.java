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
package com.hotels.styx.admin.handlers;

import com.hotels.styx.api.Eventual;
import com.hotels.styx.api.HttpHandler;
import com.hotels.styx.api.HttpInterceptor;
import com.hotels.styx.api.LiveHttpRequest;
import com.hotels.styx.api.LiveHttpResponse;
import com.hotels.styx.configstore.ConfigStore;
import com.hotels.styx.proxy.plugin.NamedPlugin;

import java.util.List;
import java.util.stream.Stream;

import static com.google.common.net.MediaType.HTML_UTF_8;
import static com.hotels.styx.api.HttpHeaderNames.CONTENT_TYPE;
import static com.hotels.styx.api.HttpResponse.response;
import static com.hotels.styx.api.HttpResponseStatus.OK;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * Returns a simple HTML page with a list of plugins, split into enabled and disabled.
 */
public class PluginListHandler implements HttpHandler {
    private final ConfigStore configStore;

    public PluginListHandler(ConfigStore configStore) {
        this.configStore = requireNonNull(configStore);
    }

    @Override
    public Eventual<LiveHttpResponse> handle(LiveHttpRequest request, HttpInterceptor.Context context) {
        List<NamedPlugin> plugins = configStore.startingWith("plugins", NamedPlugin.class)
                .stream()
                .map(ConfigStore.ConfigEntry::value)
                .collect(toList());

        Stream<NamedPlugin> enabled = plugins.stream().filter(NamedPlugin::enabled);
        Stream<NamedPlugin> disabled = plugins.stream().filter(plugin -> !plugin.enabled());

        String output = section("Enabled", enabled)
                + section("Disabled", disabled);

        return Eventual.of(response(OK)
                .body(output, UTF_8)
                .addHeader(CONTENT_TYPE, HTML_UTF_8.toString())
                .build()
                .stream());
    }

    private static String section(String toggleState, Stream<NamedPlugin> plugins) {
        return format("<h3>%s</h3>", toggleState)
                + plugins.map(NamedPlugin::name)
                .map(PluginListHandler::pluginLink)
                .collect(joining());
    }

    private static String pluginLink(String name) {
        return format("<a href='/admin/plugins/%s'>%s</a><br />", name, name);
    }
}
