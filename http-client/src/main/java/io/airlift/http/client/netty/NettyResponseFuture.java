package io.airlift.http.client.netty;

import com.google.common.util.concurrent.AbstractFuture;
import io.airlift.http.client.HttpClient;
import io.netty.handler.timeout.TimeoutException;

import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicReference;

import static io.airlift.http.client.netty.NettyResponseFuture.NettyAsyncHttpState.CANCELED;
import static io.airlift.http.client.netty.NettyResponseFuture.NettyAsyncHttpState.DONE;
import static io.airlift.http.client.netty.NettyResponseFuture.NettyAsyncHttpState.FAILED;
import static io.airlift.http.client.netty.NettyResponseFuture.NettyAsyncHttpState.TIMEOUT;

public class NettyResponseFuture<T, E extends Exception>
        extends AbstractFuture<T>
        implements HttpClient.HttpResponseFuture<T>
{
    private final AtomicReference<NettyAsyncHttpState> state = new AtomicReference<>(NettyAsyncHttpState.WAITING_FOR_CONNECTION);

    enum NettyAsyncHttpState
    {
        WAITING_FOR_CONNECTION,
        CONNECTED,
        PROCESSING_RESPONSE,
        DONE,
        FAILED,
        CANCELED,
        TIMEOUT
    }

    @Override
    public String getState()
    {
        return state.get().toString();
    }

    public void setState(NettyAsyncHttpState newState)
    {
        state.set(newState);
    }

    public boolean setValue(T value)
    {
        setState(DONE);
        return set(value);
    }

    public boolean setException(Throwable exception)
    {
        if (exception instanceof CancellationException) {
            setState(CANCELED);
            return super.setException(exception);
        }
        if (exception instanceof TimeoutException e) {
            setState(TIMEOUT);
            return super.setException(new java.util.concurrent.TimeoutException(e.getMessage()));
        }
        setState(FAILED);
        return super.setException(exception);
    }
}
