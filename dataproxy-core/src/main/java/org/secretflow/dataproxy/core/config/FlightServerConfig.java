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

package org.secretflow.dataproxy.core.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.secretflow.dataproxy.core.listener.DataProxyAllocationListener;

/**
 * @author yuexie
 * @date 2024/10/30 16:14
 **/
@Slf4j
public record FlightServerConfig(String host, int port) {

    private static final BufferAllocator ROOT_ALLOCATOR =
            new RootAllocator(new DataProxyAllocationListener(), 2L * 1024 * 1024 * 1024);

    public Location getLocation() {
        return Location.forGrpcInsecure(host, port);
    }

    public BufferAllocator getBufferAllocator() {
        return ROOT_ALLOCATOR;
    }
}
