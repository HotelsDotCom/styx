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
package com.hotels.styx.api;

import com.google.common.collect.ImmutableSet;
import com.hotels.styx.api.cookies.RequestCookie;
import com.hotels.styx.api.messages.HttpMethod;
import com.hotels.styx.api.messages.HttpVersion;
import io.netty.buffer.Unpooled;
import rx.Observable;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.base.Objects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.hotels.styx.api.HttpHeaderNames.CONNECTION;
import static com.hotels.styx.api.HttpHeaderNames.CONTENT_LENGTH;
import static com.hotels.styx.api.HttpHeaderNames.COOKIE;
import static com.hotels.styx.api.HttpHeaderNames.HOST;
import static com.hotels.styx.api.HttpHeaderValues.KEEP_ALIVE;
import static com.hotels.styx.api.cookies.RequestCookie.decode;
import static com.hotels.styx.api.cookies.RequestCookie.encode;
import static com.hotels.styx.api.messages.HttpMethod.DELETE;
import static com.hotels.styx.api.messages.HttpMethod.GET;
import static com.hotels.styx.api.messages.HttpMethod.HEAD;
import static com.hotels.styx.api.messages.HttpMethod.METHODS;
import static com.hotels.styx.api.messages.HttpMethod.PATCH;
import static com.hotels.styx.api.messages.HttpMethod.POST;
import static com.hotels.styx.api.messages.HttpMethod.PUT;
import static com.hotels.styx.api.messages.HttpVersion.HTTP_1_1;
import static java.lang.Integer.parseInt;
import static java.net.InetSocketAddress.createUnresolved;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;

/**
 * HTTP request with a fully aggregated/decoded body.
 */
public class FullHttpRequest implements FullHttpMessage {
    private final Object id;
    // Relic of old API, kept for conversions
    private final InetSocketAddress clientAddress;
    private final HttpVersion version;
    private final HttpMethod method;
    private final Url url;
    private final HttpHeaders headers;
    private final boolean secure;
    private final byte[] body;

    FullHttpRequest(Builder builder) {
        this.id = builder.id == null ? randomUUID() : builder.id;
        this.clientAddress = builder.clientAddress;
        this.version = builder.version;
        this.method = builder.method;
        this.url = builder.url;
        this.secure = builder.secure;
        this.headers = builder.headers.build();
        this.body = requireNonNull(builder.body);
    }

    /**
     * Creates a request with the GET method.
     *
     * @param uri URI
     * @return {@code this}
     */
    public static Builder get(String uri) {
        return new Builder(GET, uri);
    }

    /**
     * Creates a request with the HEAD method.
     *
     * @param uri URI
     * @return {@code this}
     */
    public static Builder head(String uri) {
        return new Builder(HEAD, uri);
    }

    /**
     * Creates a request with the POST method.
     *
     * @param uri URI
     * @return {@code this}
     */
    public static Builder post(String uri) {
        return new Builder(POST, uri);
    }

    /**
     * Creates a request with the DELETE method.
     *
     * @param uri URI
     * @return {@code this}
     */
    public static Builder delete(String uri) {
        return new Builder(DELETE, uri);
    }

    /**
     * Creates a request with the PUT method.
     *
     * @param uri URI
     * @return {@code this}
     */
    public static Builder put(String uri) {
        return new Builder(PUT, uri);
    }

    /**
     * Creates a request with the PATCH method.
     *
     * @param uri URI
     * @return {@code this}
     */
    public static Builder patch(String uri) {
        return new Builder(PATCH, uri);
    }

    @Override
    public HttpVersion version() {
        return this.version;
    }

    @Override
    public HttpHeaders headers() {
        return headers;
    }

    @Override
    public List<String> headers(CharSequence name) {
        return headers.getAll(name);
    }

    /**
     * Returns message body as a byte array.
     * <p>
     * Returns the body of this message as a byte array, in its unencoded form.
     * Because FullHttpRequest is an immutable object, the returned byte array
     * reference cannot be used to modify the message content.
     * <p>
     *
     * @return Message body content
     */
    @Override
    public byte[] body() {
        return body.clone();
    }

    /**
     * Returns the message body as a String decoded with provided character set.
     * <p>
     * Decodes the message body into a Java String object with a provided charset.
     * The caller must ensure the provided charset is compatible with message content
     * type and encoding.
     *
     * @param charset Charset used to decode message body.
     * @return Message body as a String.
     */
    @Override
    public String bodyAs(Charset charset) {
        // CHECKSTYLE:OFF
        return new String(this.body, charset);
        // CHECKSTYLE:ON
    }

    /**
     * Gets the unique ID for this request.
     *
     * @return request ID
     */
    public Object id() {
        return id;
    }

    /**
     * Returns the HTTP method of this request.
     *
     * @return the HTTP method
     */
    public HttpMethod method() {
        return method;
    }

    /**
     * Returns the requested URI (or alternatively, path).
     *
     * @return The URI being requested
     */
    public Url url() {
        return url;
    }

    /**
     * Returns the requested path.
     *
     * @return the path being requested
     */
    public String path() {
        return url.path();
    }

    /**
     * Returns {@code true} if and only if the connection can remain open and thus 'kept alive'.
     * This methods respects the value of the {@code "Connection"} header first and if this has no such header
     * then the keep-alive status is determined by the HTTP version, that is, HTTP/1.1 is keep-alive by default,
     * HTTP/1.0 is not keep-alive by default.
     *
     * @return true if the connection is keep-alive
     */
    public boolean keepAlive() {
        return HttpMessageSupport.keepAlive(headers, version);
    }

    /**
     * Checks if the request has been transferred over a secure connection. If the protocol is HTTPS and the
     * content is delivered over SSL then the request is considered to be secure.
     *
     * @return true if the request is transferred securely
     */
    public boolean isSecure() {
        return secure;
    }

    // Relic of old API, kept only for conversions
    InetSocketAddress clientAddress() {
        return this.clientAddress;
    }

    /**
     * Get a query parameter by name if present.
     *
     * @param name parameter name
     * @return query parameter if present
     */
    public Optional<String> queryParam(String name) {
        return url.queryParam(name);
    }

    /**
     * Gets query parameters by name.
     *
     * @param name parameter name
     * @return query parameters
     */
    public Iterable<String> queryParams(String name) {
        return url.queryParams(name);
    }

    /**
     * Get all query parameters.
     *
     * @return all query parameters
     */
    public Map<String, List<String>> queryParams() {
        return url.queryParams();
    }

    /**
     * Get the names of all query parameters.
     *
     * @return the names of all query parameters.
     */
    public Iterable<String> queryParamNames() {
        return url.queryParamNames();
    }

    /**
     * Return a new {@link Builder} that will inherit properties from this request.
     * This allows a new request to be made that will be identical to this one except for the properties
     * overridden by the builder methods.
     *
     * @return new builder based on this request
     */
    public Builder newBuilder() {
        return new Builder(this);
    }

    /**
     * Converts this request into a streaming form (HttpRequest).
     * <p>
     * Converts this request into a HttpRequest object which represents the HTTP request as a
     * stream of bytes.
     *
     * @return A streaming HttpRequest object.
     */
    public HttpRequest toStreamingRequest() {
        HttpRequest.Builder streamingBuilder = new HttpRequest.Builder(this)
                .disableValidation()
                .clientAddress(clientAddress);

        if (this.body.length == 0) {
            return streamingBuilder.body(new StyxCoreObservable<>(Observable.empty())).build();
        } else {
            return streamingBuilder.body(StyxObservable.of(Unpooled.copiedBuffer(body))).build();
        }
    }

    /**
     * Decodes the "Cookie" header in this request and returns the cookies.
     *
     * @return cookies
     */
    public Set<RequestCookie> cookies() {
        // Note: there should only be one "Cookie" header, but we check for multiples just in case
        // the alternative would be to respond with a 400 Bad Request status if multiple "Cookie" headers were detected

        return headers.getAll(COOKIE).stream()
                .map(RequestCookie::decode)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    /**
     * Decodes the "Cookie" header in this request and returns the specified cookie.
     *
     * @param name cookie name
     * @return cookies
     */
    public Optional<RequestCookie> cookie(String name) {
        return cookies().stream()
                .filter(cookie -> cookie.name().equals(name))
                .findFirst();
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("version", version)
                .add("method", method)
                .add("uri", url)
                .add("headers", headers)
                .add("id", id)
                .add("secure", secure)
                .toString();
    }

    /**
     * Builder.
     */
    public static final class Builder {
        private static final InetSocketAddress LOCAL_HOST = createUnresolved("127.0.0.1", 0);

        private Object id;
        private HttpMethod method = HttpMethod.GET;
        private InetSocketAddress clientAddress = LOCAL_HOST;
        private boolean validate = true;
        private Url url;
        private boolean secure;
        private HttpHeaders.Builder headers;
        private HttpVersion version = HTTP_1_1;
        private byte[] body;

        public Builder() {
            this.url = Url.Builder.url("/").build();
            this.headers = new HttpHeaders.Builder();
            this.body = new byte[0];
        }

        public Builder(HttpMethod method, String uri) {
            this();
            this.method = requireNonNull(method);
            this.url = Url.Builder.url(uri).build();
            this.secure = url.isSecure();
        }

        public Builder(HttpRequest request, byte[] body) {
            this.id = request.id();
            this.method = request.method();
            this.clientAddress = request.clientAddress();
            this.url = request.url();
            this.secure = request.isSecure();
            this.version = request.version();
            this.headers = request.headers().newBuilder();
            this.body = body;
        }

        Builder(FullHttpRequest request) {
            this.id = request.id();
            this.method = request.method();
            this.clientAddress = request.clientAddress;
            this.url = request.url();
            this.secure = request.isSecure();
            this.version = request.version();
            this.headers = request.headers().newBuilder();
            this.body = request.body();
        }

        /**
         * Sets the request URI.
         *
         * @param uri URI
         * @return {@code this}
         */
        public Builder uri(String uri) {
            return this.url(Url.Builder.url(uri).build());
        }

        /**
         * Sets the request body.
         * <p>
         * This method encodes a String content to a byte array using the specified
         * charset, and sets the Content-Length header accordingly.
         *
         * @param content request body
         * @param charset Charset for string encoding.
         * @return {@code this}
         */
        public Builder body(String content, Charset charset) {
            return body(content, charset, true);
        }

        /**
         * Sets the request body.
         * <p>
         * This method encodes the content to a byte array using the specified
         * charset, and sets the Content-Length header *if* the setContentLength
         * argument is true.
         *
         * @param content          request body
         * @param charset          Charset used for encoding request body.
         * @param setContentLength If true, Content-Length header is set, otherwise it is not set.
         * @return {@code this}
         */
        public Builder body(String content, Charset charset, boolean setContentLength) {
            requireNonNull(charset, "Charset is not provided.");
            String sanitised = content == null ? "" : content;
            return body(sanitised.getBytes(charset), setContentLength);
        }

        /**
         * Sets the request body.
         * <p>
         * This method encodes the content to a byte array provided, and
         * sets the Content-Length header *if* the setContentLength
         * argument is true.
         *
         * @param content          request body
         * @param setContentLength If true, Content-Length header is set, otherwise it is not set.
         * @return {@code this}
         */
        public Builder body(byte[] content, boolean setContentLength) {
            this.body = content != null ? content.clone() : new byte[0];

            if (setContentLength) {
                header(CONTENT_LENGTH, this.body.length);
            }

            return this;
        }

        /**
         * Sets the unique ID for this request.
         *
         * @param id request ID
         * @return {@code this}
         */
        public Builder id(Object id) {
            this.id = id;
            return this;
        }

        /**
         * Sets the (only) value for the header with the specified name.
         * <p/>
         * All existing values for the same header will be removed.
         *
         * @param name  The name of the header
         * @param value The value of the header
         * @return {@code this}
         */
        public Builder header(CharSequence name, Object value) {
            this.headers.set(name, value);
            return this;
        }

        /**
         * Sets the headers.
         *
         * @param headers headers
         * @return {@code this}
         */
        public Builder headers(HttpHeaders headers) {
            this.headers = headers.newBuilder();
            return this;
        }

        /**
         * Adds a new header with the specified {@code name} and {@code value}.
         * <p/>
         * Will not replace any existing values for the header.
         *
         * @param name  The name of the header
         * @param value The value of the header
         * @return {@code this}
         */
        public Builder addHeader(CharSequence name, Object value) {
            this.headers.add(name, value);
            return this;
        }

        /**
         * Removes the header with the specified name.
         *
         * @param name The name of the header to remove
         * @return {@code this}
         */
        public Builder removeHeader(CharSequence name) {
            headers.remove(name);
            return this;
        }

        /**
         * Sets the request fully qualified url.
         *
         * @param url fully qualified url
         * @return {@code this}
         */
        public Builder url(Url url) {
            this.url = url;
            this.secure = url.isSecure();
            return this;
        }

        /**
         * Sets the HTTP version.
         *
         * @param version HTTP version
         * @return {@code this}
         */
        public Builder version(HttpVersion version) {
            this.version = requireNonNull(version);
            return this;
        }

        /**
         * Sets the HTTP method.
         *
         * @param method HTTP method
         * @return {@code this}
         */
        public Builder method(HttpMethod method) {
            this.method = requireNonNull(method);
            return this;
        }

        /**
         * Sets the cookies on this request by overwriting the value of the "Cookie" header.
         *
         * @param cookies cookies
         * @return this builder
         */
        public Builder cookies(RequestCookie... cookies) {
            return cookies(asList(cookies));
        }

        /**
         * Sets the cookies on this request by overwriting the value of the "Cookie" header.
         *
         * @param cookies cookies
         * @return this builder
         */
        public Builder cookies(Collection<RequestCookie> cookies) {
            headers.remove(COOKIE);

            if (!cookies.isEmpty()) {
                header(COOKIE, encode(cookies));
            }
            return this;
        }

        /**
         * Adds cookies into the "Cookie" header. If the name matches an already existing cookie, the value will be overwritten.
         * <p>
         * Note that this requires decoding the current header value before re-encoding, so it is most efficient to
         * add all new cookies in one call to the method rather than spreading them out.
         *
         * @param cookies new cookies
         * @return this builder
         */
        public Builder addCookies(RequestCookie... cookies) {
            return addCookies(asList(cookies));
        }

        /**
         * Adds cookies into the "Cookie" header. If the name matches an already existing cookie, the value will be overwritten.
         * <p>
         * Note that this requires decoding the current header value before re-encoding, so it is most efficient to
         * add all new cookies in one call to the method rather than spreading them out.
         *
         * @param cookies new cookies
         * @return this builder
         */
        public Builder addCookies(Collection<RequestCookie> cookies) {
            Set<RequestCookie> currentCookies = decode(headers.get(COOKIE));

            List<RequestCookie> combinedCookies = new ArrayList<>(currentCookies.size() + cookies.size());
            combinedCookies.addAll(cookies);
            combinedCookies.addAll(currentCookies);
            return cookies(combinedCookies);
        }

        /**
         * Removes all cookies matching one of the supplied names by overwriting the value of the "Cookie" header.
         *
         * @param names cookie names
         * @return this builder
         */
        public Builder removeCookies(String... names) {
            return removeCookies(asList(names));
        }

        /**
         * Removes all cookies matching one of the supplied names by overwriting the value of the "Cookie" header.
         *
         * @param names cookie names
         * @return this builder
         */
        public Builder removeCookies(Collection<String> names) {
            return removeCookiesIf(toSet(names)::contains);
        }

        private Builder removeCookiesIf(Predicate<String> removeIfName) {
            Predicate<RequestCookie> keepIf = cookie -> !removeIfName.test(cookie.name());

            List<RequestCookie> newCookies = decode(headers.get(COOKIE)).stream()
                    .filter(keepIf)
                    .collect(toList());

            return cookies(newCookies);
        }

        private static <T> Set<T> toSet(Collection<T> collection) {
            return collection instanceof Set ? (Set<T>) collection : ImmutableSet.copyOf(collection);
        }

        /**
         * Sets whether the request is be secure.
         *
         * @param secure true if secure
         * @return {@code this}
         */
        public Builder secure(boolean secure) {
            this.secure = secure;
            return this;
        }

        /**
         * Enables Keep-Alive.
         *
         * @return {@code this}
         */
        public Builder enableKeepAlive() {
            return header(CONNECTION, KEEP_ALIVE);
        }

        /**
         * Enable validation of uri and some headers.
         *
         * @return {@code this}
         */
        public Builder disableValidation() {
            this.validate = false;
            return this;
        }

        /**
         * Builds a new full request based on the settings configured in this builder.
         * If {@code validate} is set to true:
         * <ul>
         * <li>the host header will be set if absent</li>
         * <li>an exception will be thrown if the content length is not an integer, or more than one content length exists</li>
         * <li>an exception will be thrown if the request method is not a valid HTTP method</li>
         * </ul>
         *
         * @return a new full request
         */
        public FullHttpRequest build() {
            if (validate) {
                ensureContentLengthIsValid();
                ensureMethodIsValid();
                setHostHeader();
            }

            return new FullHttpRequest(this);
        }

        private void setHostHeader() {
            url.authority()
                    .ifPresent(authority -> header(HOST, authority.hostAndPort()));
        }

        private void ensureMethodIsValid() {
            checkArgument(isMethodValid(), "Unrecognised HTTP method=%s", this.method);
        }

        private boolean isMethodValid() {
            return METHODS.contains(this.method);
        }

        private void ensureContentLengthIsValid() {
            List<String> contentLengths = headers.build().getAll(CONTENT_LENGTH);

            checkArgument(contentLengths.size() <= 1, "Duplicate Content-Length found. %s", contentLengths);

            if (contentLengths.size() == 1) {
                checkArgument(isInteger(contentLengths.get(0)), "Invalid Content-Length found. %s", contentLengths.get(0));
            }
        }

        private static boolean isInteger(String contentLength) {
            try {
                parseInt(contentLength);
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        }
    }
}
