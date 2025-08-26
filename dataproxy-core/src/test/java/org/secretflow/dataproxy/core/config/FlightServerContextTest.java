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
package org.secretflow.dataproxy.core.config;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * @author yuexie
 * @date 2025/06/16 10:59:10
 */
public class FlightServerContextTest {
    
    @Test
    void testGetInstanceInMultiThread() throws InterruptedException {
        final int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicReference<FlightServerContext> firstInstance = new AtomicReference<>();
        
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                FlightServerContext instance = FlightServerContext.getInstance();
                if (firstInstance.get() == null) {
                    firstInstance.set(instance);
                } else {
                    assertSame(firstInstance.get(), instance, "The same instance should be returned in a multi-threaded environment");
                }
                latch.countDown();
            });
        }
        
        latch.await(5, TimeUnit.SECONDS);
        executor.shutdown();
    }
    @Test
    void testGetOrDefault() {
        String testKey = "nonexistent.key";
        String defaultValue = "default";
        
        String result = FlightServerContext.getOrDefault(testKey, String.class, defaultValue);
        assertEquals(defaultValue, result);
    }

    @Test
    void testFlightServerConfig() {
        FlightServerContext context = FlightServerContext.getInstance();
        assertNotNull(context.getFlightServerConfig(), "flightServerConfig should be initialized");
    }

}
