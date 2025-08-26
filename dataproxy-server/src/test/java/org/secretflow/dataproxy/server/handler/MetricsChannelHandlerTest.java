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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author yuexie
 * @date 2025/6/5 14:14
 **/

@ExtendWith(MockitoExtension.class)
public class MetricsChannelHandlerTest {

    @Mock
    private SocketChannel socketChannel;
    @Mock
    private ChannelPipeline pipeline;

    private final MetricsChannelHandler handler = new MetricsChannelHandler();

    // Test Scenario: Verify the order in which processors are added when the channel is initialized
    @Test
    public void testInitChannel_OrderOfHandlers() {
        when(socketChannel.pipeline()).thenReturn(pipeline);

        handler.initChannel(socketChannel);

        InOrder inOrder = inOrder(pipeline);
        inOrder.verify(pipeline).addLast(any(HttpServerCodec.class));
        inOrder.verify(pipeline).addLast(any(HttpObjectAggregator.class));
        inOrder.verify(pipeline).addLast(any(MetricsChannelInboundHandler.class));
    }

    // Test Case: Verify the correctness of the HttpObjectAggregator parameters
    @Test
    public void testHttpObjectAggregator_MaxContentLength() {
        when(socketChannel.pipeline()).thenReturn(pipeline);
        ArgumentCaptor<HttpObjectAggregator> captor = ArgumentCaptor.forClass(HttpObjectAggregator.class);
        handler.initChannel(socketChannel);

        verify(pipeline, times(3)).addLast(captor.capture());

        assertEquals(3, captor.getAllValues().size());
        HttpObjectAggregator httpObjectAggregator = captor.getAllValues().get(1);
        assertEquals(HttpObjectAggregator.class, httpObjectAggregator.getClass());
        assertEquals(65536, httpObjectAggregator.maxContentLength());
    }

    // [Single Test Case] Test Scenario: Verify that three processors are added correctly
    @Test
    public void testInitChannel_AllHandlersAdded() {
        when(socketChannel.pipeline()).thenReturn(pipeline);

        handler.initChannel(socketChannel);

        verify(pipeline, times(3)).addLast(any());
    }

    // [Single Test Case] Test Scenario: Verify the correctness of the processor type
    @Test
    public void testHandlerTypes() {
        when(socketChannel.pipeline()).thenReturn(pipeline);
        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);

        handler.initChannel(socketChannel);

        verify(pipeline, times(3)).addLast((ChannelHandler) captor.capture());
        assertEquals(HttpServerCodec.class, captor.getAllValues().get(0).getClass());
        assertEquals(HttpObjectAggregator.class, captor.getAllValues().get(1).getClass());
        assertEquals(MetricsChannelInboundHandler.class, captor.getAllValues().get(2).getClass());
    }


}
