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

import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.secretflow.dataproxy.metrics.JvmMetricsRegistrar;

import java.nio.charset.StandardCharsets;

/**
 * @author yuexie
 * @date 2025/6/3 14:40
 **/
public class MetricsChannelInboundHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private final static PrometheusMeterRegistry PROMETHEUS_METER_REGISTRY = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

    static {
        JvmMetricsRegistrar.registerClassLoaderMetrics(PROMETHEUS_METER_REGISTRY);
        JvmMetricsRegistrar.registerJvmGcMetrics(PROMETHEUS_METER_REGISTRY);
        JvmMetricsRegistrar.registerJvmMemoryMetrics(PROMETHEUS_METER_REGISTRY);
        JvmMetricsRegistrar.registerJvmThreadMetrics(PROMETHEUS_METER_REGISTRY);
        JvmMetricsRegistrar.registerFileDescriptorMetrics(PROMETHEUS_METER_REGISTRY);
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req) throws Exception {
        if ("/metrics".equals(req.uri())) {

            String metrics = PROMETHEUS_METER_REGISTRY.scrape();
            FullHttpResponse response = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1,
                    HttpResponseStatus.OK,
                    Unpooled.copiedBuffer(metrics, StandardCharsets.UTF_8)
            );
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; version=0.0.4;charset=utf-8");
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);

        } else {

            FullHttpResponse response = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1,
                    HttpResponseStatus.NOT_FOUND
            );
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);

        }
    }
}
