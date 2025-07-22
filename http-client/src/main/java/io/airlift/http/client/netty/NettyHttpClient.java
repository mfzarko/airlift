package io.airlift.http.client.netty;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import io.airlift.http.client.BodyGenerator;
import io.airlift.http.client.FileBodyGenerator;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.HttpRequestFilter;
import io.airlift.http.client.HttpStatusListener;
import io.airlift.http.client.HttpVersion;
import io.airlift.http.client.Request;
import io.airlift.http.client.RequestStats;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.StaticBodyGenerator;
import io.airlift.http.client.StreamingBodyGenerator;
import io.airlift.http.client.StreamingResponse;
import io.airlift.http.client.jetty.FailedHttpResponseFuture;
import io.airlift.security.pem.PemReader;
import io.airlift.units.Duration;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.handler.ssl.CipherSuiteFilter;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import io.netty.util.internal.EmptyArrays;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.TracerProvider;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.semconv.HttpAttributes;
import io.opentelemetry.semconv.NetworkAttributes;
import io.opentelemetry.semconv.ServerAttributes;
import io.opentelemetry.semconv.UrlAttributes;
import io.opentelemetry.semconv.incubating.HttpIncubatingAttributes;
import org.reactivestreams.Publisher;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.ByteBufMono;
import reactor.netty.http.HttpProtocol;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.HttpClientRequest;
import reactor.netty.http.client.HttpClientResponse;
import reactor.netty.transport.ProxyProvider;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.net.HttpHeaders.CONTENT_LENGTH;
import static com.google.common.net.InetAddresses.isInetAddress;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.http.client.netty.NettyHttpClient.HttpClientBuilder.httpClientBuilder;
import static io.airlift.http.client.netty.NettyResponseFuture.NettyAsyncHttpState.CONNECTED;
import static io.airlift.http.client.netty.NettyResponseFuture.NettyAsyncHttpState.PROCESSING_RESPONSE;
import static io.netty.buffer.Unpooled.wrappedBuffer;
import static io.netty.channel.ChannelOption.ALLOCATOR;
import static io.netty.channel.ChannelOption.CONNECT_TIMEOUT_MILLIS;
import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static java.lang.Math.toIntExact;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.eclipse.jetty.client.HttpClient.normalizePort;
import static reactor.netty.transport.ProxyProvider.Proxy.HTTP;
import static reactor.netty.transport.ProxyProvider.Proxy.SOCKS4;

public class NettyHttpClient
        implements io.airlift.http.client.HttpClient
{
    private static final int CHUNK_SIZE = 8192;
    private static final AttributeKey<Boolean> EXCEPTION_ESCAPED = AttributeKey.booleanKey("exception.escaped");
    private static final OpenTelemetry NOOP_OPEN_TELEMETRY = OpenTelemetry.noop();
    private static final Tracer NOOP_TRACER = TracerProvider.noop().get("noop");
    private static final AttributeKey<String> CLIENT_NAME = stringKey("airlift.http.client_name");
    private final String name;
    private final TextMapPropagator propagator;
    private final Tracer tracer;
    private HttpClient client;
    private final List<? extends HttpRequestFilter> requestFilters;
    private final java.time.Duration idleTimeout;
    private final EventLoopGroup eventLoop;

    public NettyHttpClient(
            String name,
            HttpClientConfig config,
            Iterable<? extends HttpRequestFilter> requestFilters,
            Iterable<? extends HttpStatusListener> httpStatusListeners)
    {
        this(name, config, requestFilters, NOOP_OPEN_TELEMETRY, NOOP_TRACER, Optional.empty(), Optional.empty(), httpStatusListeners);
    }

    public NettyHttpClient(
            String name,
            HttpClientConfig config,
            Iterable<? extends HttpRequestFilter> requestFilters,
            Optional<String> environment,
            Optional<SslContext> maybeSslContextFactory)
    {
        this(name, config, requestFilters, NOOP_OPEN_TELEMETRY, NOOP_TRACER, environment, maybeSslContextFactory);
    }

    public NettyHttpClient(
            String name,
            HttpClientConfig config,
            Iterable<? extends HttpRequestFilter> requestFilters,
            OpenTelemetry openTelemetry,
            Tracer tracer,
            Optional<String> environment,
            Optional<SslContext> maybeSslContextFactory)
    {
        this(name, config, requestFilters, openTelemetry, tracer, environment, maybeSslContextFactory, ImmutableList.of());
    }

    public NettyHttpClient(
            String name,
            HttpClientConfig config,
            Iterable<? extends HttpRequestFilter> requestFilters,
            OpenTelemetry openTelemetry,
            Tracer tracer,
            Optional<String> environment,
            Optional<SslContext> maybeSslContext,
            Iterable<? extends HttpStatusListener> httpStatusListeners)
    {
        this.name = requireNonNull(name, "name is null");
        this.propagator = openTelemetry.getPropagators().getTextMapPropagator();
        this.tracer = requireNonNull(tracer, "tracer is null");

        requireNonNull(config, "config is null");
        requireNonNull(requestFilters, "requestFilters is null");
        requireNonNull(httpStatusListeners, "httpStatusListeners is null");

        this.eventLoop = new MultiThreadIoEventLoopGroup(config.getMaxThreads(), daemonThreadsNamed("http-client-" + name + "-%s"), NioIoHandler.newFactory());
        this.client = httpClientBuilder()
                .configure(configureProxy(config))
                .configure(configureSSL(config, maybeSslContext.orElseGet(() -> getSslContextFactory(config, environment))))
                .configure(configureTimeouts(config))
                .configure(configureStatusListeners(httpStatusListeners))
                .configure(configureEventLoop(eventLoop))
                .build();
        this.idleTimeout = config.getIdleTimeout().toJavaTime();
        this.requestFilters = ImmutableList.copyOf(requestFilters);
    }

    private HttpClientConfigurer configureEventLoop(EventLoopGroup eventLoop)
    {
        return client -> client
                .runOn(eventLoop)
                .option(ALLOCATOR, PooledByteBufAllocator.DEFAULT);
    }

    private HttpClientConfigurer configureStatusListeners(Iterable<? extends HttpStatusListener> httpStatusListeners)
    {
        return client -> client.doOnResponse((response, _) -> {
            for (HttpStatusListener httpStatusListener : httpStatusListeners) {
                httpStatusListener.statusReceived(response.status().code());
            }
        });
    }

    private static HttpClientConfigurer configureProxy(HttpClientConfig config)
    {
        return client -> {
            if (config.getHttpProxy() != null) {
                return client.proxy(proxyOptions -> {
                    ProxyProvider.Builder builder = proxyOptions.type(HTTP)
                            .host(config.getHttpProxy().getHost())
                            .port(config.getHttpProxy().getPort());
                    config.getHttpProxyPassword().ifPresent(builder::username);
                    config.getHttpProxyPassword().ifPresent(password -> builder.password(_ -> password));
                });
            }
            if (config.getSocksProxy() != null) {
                return client.proxy(proxyOptions -> {
                    ProxyProvider.Builder builder = proxyOptions.type(SOCKS4)
                            .host(config.getSocksProxy().getHost())
                            .port(config.getSocksProxy().getPort());
                    config.getHttpProxyUser().ifPresent(builder::username);
                    config.getHttpProxyPassword().ifPresent(password -> builder.password(_ -> password));
                });
            }
            return client.noProxy();
        };
    }

    private static HttpClientConfigurer configureTimeouts(HttpClientConfig config)
    {
        return client -> client
                .option(CONNECT_TIMEOUT_MILLIS, toIntExact(config.getConnectTimeout().toMillis()))
                .doOnConnected(conn -> conn
                    .addHandlerLast(new ReadTimeoutHandler(toIntExact(config.getIdleTimeout().toMillis())))
                    .addHandlerLast(new WriteTimeoutHandler(toIntExact(config.getIdleTimeout().toMillis()))));
    }

    private static HttpClientConfigurer configureSSL(HttpClientConfig config, SslContext sslContext)
    {
        return client -> client.secure(spec -> spec.sslContext(sslContext)
                .handlerConfigurator(sslHandler -> {
                    SSLEngine engine = sslHandler.engine();
                    SSLParameters params = engine.getSSLParameters();
                    params.setServerNames(getSniServerNames(engine, params.getServerNames()));
                    engine.setSSLParameters(params);
                }));
    }

    private SslContext getSslContextFactory(HttpClientConfig config, Optional<String> environment)
    {
        SslContextBuilder sslContextBuilder = SslContextBuilder.forClient();
        sslContextBuilder.endpointIdentificationAlgorithm(config.isVerifyHostname() ? "HTTPS" : null);
//
//        String keyStorePassword = firstNonNull(config.getKeyStorePassword(), "");
//        KeyStore keyStore = null;
//        if (config.getKeyStorePath() != null) {
//            keyStore = loadKeyStore(config.getKeyStorePath(), config.getKeyStorePassword());
//            sslContextBuilder.keyManager(keyStore);
//            sslContextBuilder.setKeyStorePassword(keyStorePassword);
//        }
//
//        if (config.getTrustStorePath() != null || config.getAutomaticHttpsSharedSecret() != null) {
//            KeyStore trustStore = loadTrustStore(config.getTrustStorePath(), config.getTrustStorePassword());
//            if (config.getAutomaticHttpsSharedSecret() != null) {
//                addAutomaticTrust(config.getAutomaticHttpsSharedSecret(), trustStore, environment
//                        .orElseThrow(() -> new IllegalArgumentException("Environment must be provided when automatic HTTPS is enabled")));
//            }
//            sslContextFactory.setTrustStore(trustStore);
//            sslContextFactory.setTrustStorePassword("");
//        }
//        else if (keyStore != null) {
//            // Backwards compatibility for with Jetty's internal behavior
//            sslContextFactory.setTrustStore(keyStore);
//            sslContextFactory.setTrustStorePassword(keyStorePassword);
//        }

        List<String> includedCipherSuites = config.getHttpsIncludedCipherSuites();
        List<String> excludedCipherSuites = config.getHttpsExcludedCipherSuites();
        sslContextBuilder.ciphers(includedCipherSuites, cipherSuiteFilter(new HashSet<>(excludedCipherSuites)));

        try {
            if (config.getSecureRandomAlgorithm() != null) {
                sslContextBuilder.secureRandom(SecureRandom.getInstance(config.getSecureRandomAlgorithm()));
            }
            return sslContextBuilder.build();
        }
        catch (SSLException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T, E extends Exception> T execute(Request request, ResponseHandler<T, E> responseHandler)
            throws E
    {
        try {
            return executeAsync(request, responseHandler)
                    .get(request.getIdleTimeout().map(Duration::toJavaTime).orElse(idleTimeout).toMillis(), TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        catch (ExecutionException executionException) {
            Throwable cause = executionException.getCause();
            if (cause instanceof Exception) {
                return responseHandler.handleException(request, (Exception) cause);
            }
            throwIfUnchecked(cause);
            return responseHandler.handleException(request, new RuntimeException(cause));
        }
        catch (Throwable e) {
            sneakyThrow(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T, E extends Exception> HttpResponseFuture<T> executeAsync(Request request, ResponseHandler<T, E> responseHandler)
    {
        return sendRequest(request, responseHandler);
    }

    private <T, E extends Exception> HttpResponseFuture<T> sendRequest(Request request, ResponseHandler<T, E> responseHandler)
    {
        NettyResponseFuture<T, E> nettyFuture = new NettyResponseFuture<>(request.getUri());
        try {
            request = applyRequestFilters(request);
        }
        catch (RuntimeException e) {
            startSpan(request)
                    .setStatus(StatusCode.ERROR, e.getMessage())
                    .recordException(e, Attributes.of(EXCEPTION_ESCAPED, true))
                    .end();
            return new FailedHttpResponseFuture<>(e);
        }

        Span span = startSpan(request);
        request = injectTracing(request, span);
        final Request finalRequest = request;
        Disposable subscription = sendRequest(request, nettyFuture)
                .responseSingle((HttpClientResponse clientResponse, ByteBufMono content) -> Mono.just(clientResponse).zipWith(orElseEmpty(content)))
                .subscribe(tuple -> {
                    try {
                        // record attributes
                        span.setAttribute(HttpAttributes.HTTP_RESPONSE_STATUS_CODE, tuple.getT1().status().code());

                        // negotiated http version
                        span.setAttribute(NetworkAttributes.NETWORK_PROTOCOL_NAME, "HTTP"); // https://osi-model.com/application-layer/
                        span.setAttribute(NetworkAttributes.NETWORK_PROTOCOL_VERSION, getHttpVersion(tuple.getT1().version()));

                        try (var _ = tuple.getT2()) {
                            NettyResponse nettyResponse = new NettyResponse(tuple.getT1(), tuple.getT2());
                            nettyFuture.setValue(responseHandler.handle(finalRequest, nettyResponse));
                            span.setAttribute(HttpIncubatingAttributes.HTTP_RESPONSE_BODY_SIZE, nettyResponse.getBytesRead());
                        }
                    }
                    catch (Exception exception) {
                        nettyFuture.setException(exception);
                    }
                }, nettyFuture::setException);

        addCallback(nettyFuture, new FutureCallback<>() {
            @Override
            public void onSuccess(T result)
            {
            }

            @Override
            public void onFailure(Throwable t)
            {
                if (t instanceof CancellationException) {
                    subscription.dispose();
                }
                span.setStatus(StatusCode.ERROR, t.getMessage());
                span.recordException(t, Attributes.of(EXCEPTION_ESCAPED, true));
                span.end();
            }
        }, directExecutor());

        return nettyFuture;
    }

    private static Mono<InputStream> orElseEmpty(ByteBufMono content)
    {
        return content
                .asInputStream()
                .switchIfEmpty(Mono.defer(() -> Mono.just(new ByteArrayInputStream(new byte[0]))));
    }

    private String getHttpVersion(io.netty.handler.codec.http.HttpVersion version)
    {
        if (version.equals(io.netty.handler.codec.http.HttpVersion.HTTP_1_0)) {
            return "1.0"; // https://datatracker.ietf.org/doc/html/rfc1945
        }
        if (version.equals(io.netty.handler.codec.http.HttpVersion.HTTP_1_1)) {
            return "1.1"; // https://datatracker.ietf.org/doc/html/rfc2616
        }
        if (version.majorVersion() == 2) {
            return "2"; // https://datatracker.ietf.org/doc/html/rfc9113
        }
        if (version.majorVersion() == 3) {
            return "3"; // https://datatracker.ietf.org/doc/html/rfc9114
        }
        return "0.9"; // https://datatracker.ietf.org/doc/html/rfc1945
    }

    public <T, E extends Exception> HttpClient.ResponseReceiver<?> sendRequest(Request request, NettyResponseFuture<T, E> nettyResponseFuture)
    {
        HttpClient requestClient = client
                .headers(headerBuilder -> request.getHeaders().forEach(headerBuilder::add))
                .followRedirect(request.isFollowRedirects())
                .doOnConnected(_ -> nettyResponseFuture.setState(CONNECTED))
                .doOnResponse((_, _) -> nettyResponseFuture.setState(PROCESSING_RESPONSE));

        if (request.getHttpVersion().isPresent()) {
            requestClient = requestClient.protocol(listProtocols(request.getHttpVersion().get()));
        }

        if (request.getRequestTimeout().isPresent()) {
            requestClient = requestClient.responseTimeout(request.getRequestTimeout().get().toJavaTime());
        }
        HttpClient.ResponseReceiver<?> responseReceiver = switch (request.getMethod()) {
            case "GET" -> requestClient.get();
            case "POST" -> requestClient.post()
                    .send((req, out) -> out.send(bodyGeneratorToFlux(req, request.getBodyGenerator())));
            case "DELETE" -> requestClient.delete()
                    .send((req, out) -> out.send(bodyGeneratorToFlux(req, request.getBodyGenerator())));
            case "HEAD" -> requestClient.head();
            case "PUT" -> requestClient.put()
                    .send((req, out) -> out.send(bodyGeneratorToFlux(req, request.getBodyGenerator())));
            default -> throw new IllegalStateException("Unexpected value: " + request.getMethod());
        };
        return responseReceiver.uri(request.getUri());
    }

    private HttpProtocol[] listProtocols(HttpVersion httpVersion)
    {
        return switch (httpVersion) {
            case HTTP_1 -> new HttpProtocol[ ]{HttpProtocol.HTTP11};
            case HTTP_2, HTTP_3 -> new HttpProtocol[] {HttpProtocol.H2, HttpProtocol.H2C, HttpProtocol.HTTP11};
        };
    }

    private Publisher<? extends ByteBuf> bodyGeneratorToFlux(HttpClientRequest req, BodyGenerator bodyGenerator)
    {
        if (bodyGenerator == null) {
            return Flux.empty();
        }

        InputStream in = switch (bodyGenerator) {
            case StreamingBodyGenerator streamingBodyGenerator -> streamingBodyGenerator.source();
            case StaticBodyGenerator staticBodyGenerator -> {
                req.addHeader(CONTENT_LENGTH, String.valueOf(staticBodyGenerator.getLength()));
                yield new ByteArrayInputStream(staticBodyGenerator.getBody());
            }
            case FileBodyGenerator fileBodyGenerator -> {
                try {
                    req.addHeader(CONTENT_LENGTH, String.valueOf(fileBodyGenerator.length()));
                    yield new FileInputStream(fileBodyGenerator.getPath().toFile());
                }
                catch (FileNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
            default -> throw new IllegalArgumentException("Unsupported BodyGenerator type: " + bodyGenerator.getClass().getName());
        };

        ByteBuffer byteBuffer = ByteBuffer.allocate(CHUNK_SIZE);

        return Flux.generate(
            () -> byteBuffer,
            (buffer, sink) -> {
                try {
                    buffer.clear();
                    int read = in.read(buffer.array(), 0, buffer.capacity());
                    if (read == -1) {
                        in.close();
                        sink.complete();
                    }
                    else {
                        sink.next(wrappedBuffer(buffer.array(), 0, read).retain());
                    }
                }
                catch (Exception e) {
                    try {
                        in.close();
                    }
                    catch (IOException ex) {
                        sink.error(ex);
                    }
                    sink.error(e);
                }
                return buffer;
            });
    }

    @Override
    public StreamingResponse executeStreaming(Request request)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    @Managed
    @Flatten
    public RequestStats getStats()
    {
        return null;
    }

    @Override
    public void close()
    {
        //eventLoop.close();
    }

    @Override
    public boolean isClosed()
    {
        return eventLoop.isTerminated();
    }

    private static KeyStore loadKeyStore(String keystorePath, String keystorePassword)
    {
        requireNonNull(keystorePath, "keystorePath is null");
        try {
            File keyStoreFile = new File(keystorePath);
            if (PemReader.isPem(keyStoreFile)) {
                return PemReader.loadKeyStore(keyStoreFile, keyStoreFile, Optional.ofNullable(keystorePassword), true);
            }
        }
        catch (IOException | GeneralSecurityException e) {
            throw new IllegalArgumentException("Error loading PEM key store: " + keystorePath, e);
        }

        try (InputStream in = new FileInputStream(keystorePath)) {
            KeyStore keyStore = KeyStore.getInstance("JKS");
            keyStore.load(in, keystorePassword.toCharArray());
            return keyStore;
        }
        catch (IOException | GeneralSecurityException e) {
            throw new IllegalArgumentException("Error loading Java key store: " + keystorePath, e);
        }
    }

    private Span startSpan(Request request)
    {
        String method = request.getMethod().toUpperCase(ENGLISH);
        int port = normalizePort(request.getUri().getScheme(), request.getUri().getPort());
        return request.getSpanBuilder()
                .orElseGet(() -> tracer.spanBuilder(name + " " + method))
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute(CLIENT_NAME, name)
                .setAttribute(UrlAttributes.URL_FULL, request.getUri().toString())
                .setAttribute(HttpAttributes.HTTP_REQUEST_METHOD, method)
                .setAttribute(ServerAttributes.SERVER_ADDRESS, request.getUri().getHost())
                .setAttribute(ServerAttributes.SERVER_PORT, (long) port)
                .startSpan();
    }

    private Request injectTracing(Request request, Span span)
    {
        Context context = Context.current().with(span);
        Request.Builder builder = Request.Builder.fromRequest(request);
        propagator.inject(context, builder, Request.Builder::addHeader);
        return builder.build();
    }

    private static KeyStore loadTrustStore(String truststorePath, String truststorePassword)
    {
        if (truststorePath == null) {
            try {
                KeyStore keyStore = KeyStore.getInstance("JKS");
                keyStore.load(null, new char[0]);
                return keyStore;
            }
            catch (GeneralSecurityException | IOException e) {
                throw new RuntimeException(e);
            }
        }

        try {
            File keyStoreFile = new File(truststorePath);
            if (PemReader.isPem(keyStoreFile)) {
                return PemReader.loadTrustStore(keyStoreFile);
            }
        }
        catch (IOException | GeneralSecurityException e) {
            throw new IllegalArgumentException("Error loading PEM trust store: " + truststorePath, e);
        }

        try (InputStream in = new FileInputStream(truststorePath)) {
            KeyStore keyStore = KeyStore.getInstance("JKS");
            keyStore.load(in, truststorePassword == null ? null : truststorePassword.toCharArray());
            return keyStore;
        }
        catch (IOException | GeneralSecurityException e) {
            throw new IllegalArgumentException("Error loading Java trust store: " + truststorePath, e);
        }
    }

    private static List<SNIServerName> getSniServerNames(SSLEngine sslEngine, List<SNIServerName> serverNames)
    {
        // work around the JDK TLS implementation not allowing single label domains
        if (serverNames == null || serverNames.isEmpty()) {
            String host = sslEngine.getPeerHost();
            if (host != null && !isInetAddress(host) && !host.contains(".")) {
                try {
                    return List.of(new SNIHostName(host));
                }
                catch (IllegalArgumentException ignored) {
                }
            }
        }
        return serverNames;
    }

    private static CipherSuiteFilter cipherSuiteFilter(Set<String> excludedCipherSuites)
    {
        return (ciphers, defaultCiphers, supportedCiphers) -> {
            final List<String> newCiphers;
            if (ciphers == null) {
                newCiphers = new ArrayList<>(defaultCiphers.size());
                ciphers = defaultCiphers;
            }
            else {
                newCiphers = new ArrayList<>(supportedCiphers.size());
            }
            for (String c : ciphers) {
                if (c == null) {
                    break;
                }
                if (supportedCiphers.contains(c) && !excludedCipherSuites.contains(c)) {
                    newCiphers.add(c);
                }
            }
            return newCiphers.toArray(EmptyArrays.EMPTY_STRINGS);
        };
    }

    private Request applyRequestFilters(Request request)
    {
        for (HttpRequestFilter requestFilter : requestFilters) {
            request = requestFilter.filterRequest(request);
        }
        return request;
    }

    public static <E extends Throwable> void sneakyThrow(Throwable e)
            throws E
    {
        throw (E) e;
    }

    public static class HttpClientBuilder
    {
        private HttpClient client;

        private HttpClientBuilder(HttpClient client)
        {
            this.client = requireNonNull(client, "client is null");
        }

        public HttpClientBuilder configure(HttpClientConfigurer configurer)
        {
            this.client = configurer.configure(client);
            return this;
        }

        public static HttpClientBuilder httpClientBuilder()
        {
            return new HttpClientBuilder(HttpClient.create());
        }

        public HttpClient build()
        {
            return client;
        }
    }

    @FunctionalInterface
    interface HttpClientConfigurer
    {
        HttpClient configure(HttpClient client);
    }
}
