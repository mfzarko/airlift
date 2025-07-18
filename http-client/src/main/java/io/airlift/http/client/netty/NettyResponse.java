package io.airlift.http.client.netty;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.io.CountingInputStream;
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.HttpVersion;
import io.netty.handler.codec.http.HttpHeaders;
import reactor.netty.http.client.HttpClientResponse;

import java.io.IOException;
import java.io.InputStream;

public class NettyResponse
        implements io.airlift.http.client.Response
{
    private final HttpVersion httpVersion;
    private final int statusCode;
    private final ListMultimap<HeaderName, String> headers;
    private final CountingInputStream inputStream;

    public NettyResponse(HttpClientResponse clientResponse, InputStream inputStream)
    {
        this.httpVersion = detectHttpVersion(clientResponse.version());
        this.statusCode = clientResponse.status().code();
        this.headers = toHeadersMap(clientResponse.responseHeaders());
        this.inputStream = new CountingInputStream(inputStream);
    }

    private HttpVersion detectHttpVersion(io.netty.handler.codec.http.HttpVersion clientResponse)
    {
        if (clientResponse.majorVersion() == 2) {
            return HttpVersion.HTTP_2;
        }
        else if (clientResponse.majorVersion() == 3) {
            return HttpVersion.HTTP_3;
        }
        else if (clientResponse.majorVersion() == 1) {
            return HttpVersion.HTTP_1;
        }

        throw new IllegalArgumentException("Unsupported HTTP version: " + clientResponse);
    }

    @Override
    public HttpVersion getHttpVersion()
    {
        return httpVersion;
    }

    @Override
    public int getStatusCode()
    {
        return statusCode;
    }

    @Override
    public ListMultimap<HeaderName, String> getHeaders()
    {
        return headers;
    }

    @Override
    public long getBytesRead()
    {
        return inputStream.getCount();
    }

    @Override
    public InputStream getInputStream()
            throws IOException
    {
        return inputStream;
    }

    private static ListMultimap<HeaderName, String> toHeadersMap(HttpHeaders headers)
    {
        ImmutableListMultimap.Builder<HeaderName, String> builder = ImmutableListMultimap.builder();
        for (String name : headers.names()) {
            for (String value : headers.getAll(name)) {
                builder.put(HeaderName.of(name), value);
            }
        }
        return builder.build();
    }
}
