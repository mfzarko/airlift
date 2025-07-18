package io.airlift.http.client.jetty;

import io.airlift.http.client.AbstractHttpClientTest;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.Request;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.StreamingResponse;

import java.util.Optional;

import static io.airlift.http.client.HttpClientConfig.ClientImplementation.NETTY;

public class TestJettyHttpClient
        extends AbstractHttpClientTest
{
    @Override
    protected HttpClientConfig createClientConfig()
    {
        return new HttpClientConfig()
                .setHttp2Enabled(false)
                .setClientImplementation(NETTY);
    }

    @Override
    public Optional<StreamingResponse> executeRequest(CloseableTestHttpServer server, Request request)
    {
        HttpClient client = server.createClient(createClientConfig());
        return Optional.of(new TestingStreamingResponse(() -> client.executeStreaming(request), client));
    }

    @Override
    public <T, E extends Exception> T executeRequest(CloseableTestHttpServer server, Request request, ResponseHandler<T, E> responseHandler)
            throws Exception
    {
        return executeRequest(server, createClientConfig(), request, responseHandler);
    }

    @Override
    public <T, E extends Exception> T executeRequest(CloseableTestHttpServer server, HttpClientConfig config, Request request, ResponseHandler<T, E> responseHandler)
            throws Exception
    {
        try (HttpClient client = server.createClient(config)) {
            return client.execute(request, responseHandler);
        }
    }
}
