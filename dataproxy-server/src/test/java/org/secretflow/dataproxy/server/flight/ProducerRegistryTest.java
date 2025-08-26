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

package org.secretflow.dataproxy.server.flight;

import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

public class ProducerRegistryTest {

    private ProducerRegistry registry;
    private FlightProducer mockProducer;

    @BeforeEach
    void setUp() {
        registry = ProducerRegistry.getInstance();
        mockProducer = mock(FlightProducer.class);
    }

    @Test
    void shouldReturnSameInstance() {
        ProducerRegistry anotherInstance = ProducerRegistry.getInstance();
        assertSame(registry, anotherInstance);
    }

    @Test
    void shouldRegisterAndGetProducer() {
        registry.register("testKey", mockProducer);
        FlightProducer result = registry.getOrDefaultNoOp("testKey");
        
        assertSame(mockProducer, result);
    }

    @Test
    void shouldReturnDefaultForUnknownKey() {
        FlightProducer result = registry.getOrDefaultNoOp("unknown");
        
        assertNotNull(result);
        assertInstanceOf(NoOpFlightProducer.class, result);
    }

    @Test
    void shouldOverrideExistingProducer() {
        registry.register("testKey", mockProducer);
        
        FlightProducer anotherProducer = mock(FlightProducer.class);
        registry.register("testKey", anotherProducer);
        
        assertSame(anotherProducer, registry.getOrDefaultNoOp("testKey"));
    }

    @Test
    void shouldBeThreadSafeForConcurrentAccess() throws InterruptedException {
        final int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int index = i;
            executor.execute(() -> {
                FlightProducer producer = mock(FlightProducer.class);
                registry.register("key" + index, producer);
                latch.countDown();
            });
        }

        latch.await(5, TimeUnit.SECONDS);

        for (int i = 0; i < threadCount; i++) {
            String key = "key" + i;
            FlightProducer producer = registry.getOrDefaultNoOp(key);
            assertNotNull(producer, "Producer for " + key + " should not be null");
            assertFalse(producer instanceof NoOpFlightProducer, "Should not return NoOpProducer for " + key);
        }
    }

    @Test
    void shouldNotLoseRegistrationUnderConcurrency() throws InterruptedException {
        final int threadCount = 100;
        final String sharedKey = "sharedKey";
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        
        for (int i = 0; i < threadCount; i++) {
            executor.execute(() -> {
                FlightProducer producer = mock(FlightProducer.class);
                registry.register(sharedKey, producer);
                latch.countDown();
            });
        }
        
        latch.await(5, TimeUnit.SECONDS);
        
        FlightProducer result = registry.getOrDefaultNoOp(sharedKey);
        assertNotNull(result);
        assertFalse(result instanceof NoOpFlightProducer);
    }
}
