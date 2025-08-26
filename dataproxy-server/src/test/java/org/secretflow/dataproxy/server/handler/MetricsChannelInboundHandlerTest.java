/*
 * Copyright 2025 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.secretflow.dataproxy.server.handler;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author yuexie
 * @date 2025/6/6 14:12
 **/
@ExtendWith(MockitoExtension.class)
public class MetricsChannelInboundHandlerTest {

    @Mock
    private ChannelHandlerContext ctx;

    @Mock
    private FullHttpRequest request;

    @Mock
    private ChannelFuture channelFuture;

    @Captor
    private ArgumentCaptor<FullHttpResponse> responseCaptor;

    // Test Case: Correctly respond to the /metrics request
    @Test
    void testHandleMetricsRequest() throws Exception {
        // Given
        when(request.uri()).thenReturn("/metrics");
        when(ctx.writeAndFlush(any(FullHttpResponse.class))).thenReturn(channelFuture);
        MetricsChannelInboundHandler handler = new MetricsChannelInboundHandler();

        // When
        handler.channelRead0(ctx, request);

        // Then
        verify(ctx).writeAndFlush(responseCaptor.capture());
        FullHttpResponse response = responseCaptor.getValue();

        assertEquals(HttpResponseStatus.OK, response.status());
        assertEquals("text/plain; version=0.0.4;charset=utf-8",
                response.headers().get(HttpHeaderNames.CONTENT_TYPE));
        assertTrue(response.content().readableBytes() > 0);
        verify(channelFuture).addListener(ChannelFutureListener.CLOSE);
    }

    // Test Case: Handling non/metrics path requests
    @Test
    void testHandleOtherPathRequest() throws Exception {
        // Given
        when(request.uri()).thenReturn("/other");
        when(ctx.writeAndFlush(any(FullHttpResponse.class))).thenReturn(channelFuture);
        MetricsChannelInboundHandler handler = new MetricsChannelInboundHandler();

        // When
        handler.channelRead0(ctx, request);

        // Then
        verify(ctx).writeAndFlush(responseCaptor.capture());
        FullHttpResponse response = responseCaptor.getValue();

        assertEquals(HttpResponseStatus.NOT_FOUND, response.status());
        assertNull(response.headers().get(HttpHeaderNames.CONTENT_TYPE));
        assertEquals(0, response.content().readableBytes());
        verify(channelFuture).addListener(ChannelFutureListener.CLOSE);
    }

    // Test Case: Processing /metrics requests with parameters
    @Test
    void testHandleMetricsWithQueryParams() throws Exception {
        // Given
        when(request.uri()).thenReturn("/metrics?debug=true");
        when(ctx.writeAndFlush(any(FullHttpResponse.class))).thenReturn(channelFuture);
        MetricsChannelInboundHandler handler = new MetricsChannelInboundHandler();

        // When
        handler.channelRead0(ctx, request);

        // Then
        verify(ctx).writeAndFlush(responseCaptor.capture());
        assertEquals(HttpResponseStatus.NOT_FOUND, responseCaptor.getValue().status());
    }

    // Test Case: Verify the encoding of the response content
    @Test
    void testResponseContentEncoding() throws Exception {
        // Given
        when(request.uri()).thenReturn("/metrics");
        when(ctx.writeAndFlush(any(FullHttpResponse.class))).thenReturn(channelFuture);
        MetricsChannelInboundHandler handler = new MetricsChannelInboundHandler();

        // When
        handler.channelRead0(ctx, request);

        // Then
        verify(ctx).writeAndFlush(responseCaptor.capture());
        String content = responseCaptor.getValue().content()
                .toString(StandardCharsets.UTF_8);
        assertTrue(content.contains("jvm_classes_loaded")); // 验证基础指标存在
    }

    // Test Case: Handle root path requests
    @Test
    void testHandleRootPathRequest() throws Exception {
        // Given
        when(request.uri()).thenReturn("/");
        when(ctx.writeAndFlush(any(FullHttpResponse.class))).thenReturn(channelFuture);
        MetricsChannelInboundHandler handler = new MetricsChannelInboundHandler();

        // When
        handler.channelRead0(ctx, request);

        // Then
        verify(ctx).writeAndFlush(responseCaptor.capture());
        assertEquals(HttpResponseStatus.NOT_FOUND, responseCaptor.getValue().status());
    }

    // Test Case: Verify the HTTP version
    @Test
    void testHttpProtocolVersion() throws Exception {
        // Given
        when(request.uri()).thenReturn("/metrics");
        when(ctx.writeAndFlush(any(FullHttpResponse.class))).thenReturn(channelFuture);
        MetricsChannelInboundHandler handler = new MetricsChannelInboundHandler();

        // When
        handler.channelRead0(ctx, request);

        // Then
        verify(ctx).writeAndFlush(responseCaptor.capture());
        assertEquals(HttpVersion.HTTP_1_1, responseCaptor.getValue().protocolVersion());
    }

    // Test Case: Handling all-caps URI requests
    @Test
    void testHandleUpperCaseUri() throws Exception {
        // Given
        when(request.uri()).thenReturn("/METRICS");
        when(ctx.writeAndFlush(any(FullHttpResponse.class))).thenReturn(channelFuture);
        MetricsChannelInboundHandler handler = new MetricsChannelInboundHandler();

        // When
        handler.channelRead0(ctx, request);

        // Then
        verify(ctx).writeAndFlush(responseCaptor.capture());
        assertEquals(HttpResponseStatus.NOT_FOUND, responseCaptor.getValue().status());
    }

    // Test Case: Verify the response to close the connection
    @Test
    void testResponseCloseConnection() throws Exception {
        // Given
        when(request.uri()).thenReturn("/metrics");
        when(ctx.writeAndFlush(any(FullHttpResponse.class))).thenReturn(channelFuture);
        MetricsChannelInboundHandler handler = new MetricsChannelInboundHandler();

        // When
        handler.channelRead0(ctx, request);

        // Then
        verify(channelFuture).addListener(ChannelFutureListener.CLOSE);
    }

}
