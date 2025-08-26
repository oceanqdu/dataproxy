/*
 * Copyright 2024 Ant Group Co., Ltd.
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

import lombok.extern.slf4j.Slf4j;
import org.secretflow.dataproxy.core.config.FlightServerConfig;
import org.secretflow.dataproxy.core.config.FlightServerContext;

/**
 * @author yuexie
 * @date 2024/11/6 16:27
 **/
@Slf4j
public class DataProxyServerApplication {

    public static void main(String[] args) {
        log.info("Starting DataProxyFlightServer");

        FlightServerConfig flightServerConfig = FlightServerContext.getInstance().getFlightServerConfig();

        try (DataProxyFlightServer dataProxyFlightServer = new DataProxyFlightServer(flightServerConfig)) {
            dataProxyFlightServer.start();
            log.info("Data proxy flight server start at {}:{}", flightServerConfig.getLocation().getUri().getHost(), flightServerConfig.port());

            new Thread(() -> {
                try {
                    new DataProxyMetricsServer().start();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }, "mertics-thread").start();

            dataProxyFlightServer.awaitTermination();
        } catch (Exception e) {
            log.error("DataProxyFlightServer start failed", e);
            throw new RuntimeException(e);
        } finally {
            log.warn("DataProxyFlightServer stopped");
            flightServerConfig.getBufferAllocator().close();
        }
    }
}
