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

package org.secretflow.dataproxy.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import lombok.extern.slf4j.Slf4j;
import org.secretflow.dataproxy.core.config.FlightServerConfigKey;
import org.secretflow.dataproxy.core.config.FlightServerContext;
import org.secretflow.dataproxy.server.handler.MetricsChannelHandler;

import java.util.function.Supplier;

/**
 * @author yuexie
 * @date 2025/4/11 18:06
 **/
@Slf4j
public class DataProxyMetricsServer {

    public static final int DEFAULT_METRICS_PORT = 9101;
    private static final String METRICS_ENDPOINT = "/metrics";

    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final Supplier<String> hostAddressSupplier;

    public DataProxyMetricsServer() {
        this.bossGroup = new NioEventLoopGroup(1);
        this.workerGroup = new NioEventLoopGroup();
        this.hostAddressSupplier = this::getHostAddress;
    }

    // Constructor for testing
    public DataProxyMetricsServer(EventLoopGroup bossGroup, EventLoopGroup workerGroup, Supplier<String> hostAddressSupplier) {
        this.bossGroup = bossGroup;
        this.workerGroup = workerGroup;
        this.hostAddressSupplier = hostAddressSupplier;
    }

    public void start() throws InterruptedException {
        Integer metricsPort = FlightServerContext.getOrDefault(FlightServerConfigKey.METRICS_PORT, Integer.class, DEFAULT_METRICS_PORT);
        log.info("Starting Metrics Server on port {}", metricsPort);

        try {
            ServerBootstrap b = createServerBootstrap();
            Channel ch = b.bind(DEFAULT_METRICS_PORT).sync().channel();
            log.info("Metrics server started at http://{}:{}{}",
                    hostAddressSupplier.get(), DEFAULT_METRICS_PORT, METRICS_ENDPOINT);
            ch.closeFuture().sync();
        } catch (InterruptedException e) {
            log.error("Metrics server was interrupted", e);
            throw e;
        } finally {
            shutdownEventLoopGroups(bossGroup, workerGroup);
        }
    }

    private ServerBootstrap createServerBootstrap() {
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new MetricsChannelHandler());
        return b;
    }

    private void shutdownEventLoopGroups(EventLoopGroup bossGroup, EventLoopGroup workerGroup) {
        try {
            bossGroup.shutdownGracefully().sync();
            workerGroup.shutdownGracefully().sync();
        } catch (InterruptedException e) {
            log.warn("Interrupted while shutting down event loop groups", e);
            Thread.currentThread().interrupt();
        }
    }

    String getHostAddress() {
        return FlightServerContext.getInstance().getFlightServerConfig().host();
    }
}
