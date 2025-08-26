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
package org.secretflow.dataproxy.core.listener;

import org.apache.arrow.memory.AllocationOutcome;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class DataProxyAllocationListenerTest {

    private final DataProxyAllocationListener listener = new DataProxyAllocationListener();

    @Test
    public void testOnPreAllocation() {
        assertDoesNotThrow(() -> listener.onPreAllocation(1024L));
    }

    @Test
    public void testOnAllocation() {
        assertDoesNotThrow(() -> listener.onAllocation(1024L));
    }

    @Test
    public void testOnRelease() {
        assertDoesNotThrow(() -> listener.onRelease(1024L));
    }

    @Test
    public void testOnFailedAllocation_returnsFalseByDefault() {
        AllocationOutcome outcome = Mockito.mock(AllocationOutcome.class);
        assertDoesNotThrow(() -> {
            boolean result = listener.onFailedAllocation(1024L, outcome);
            assertFalse(result);
        });
    }

    @Test
    public void testOnChildAdded() {
        try (BufferAllocator parentAllocator = Mockito.mock(BufferAllocator.class);
             BufferAllocator childAllocator = Mockito.mock(BufferAllocator.class)) {

            Mockito.when(parentAllocator.getName()).thenReturn("parentAllocator");
            Mockito.when(childAllocator.getName()).thenReturn("childAllocator");
            Mockito.when(childAllocator.getLimit()).thenReturn(1024L);

            assertDoesNotThrow(() -> listener.onChildAdded(parentAllocator, childAllocator));
        }
    }

    @Test
    public void testOnChildRemoved() {

        try (BufferAllocator parentAllocator = Mockito.mock(BufferAllocator.class);
             BufferAllocator childAllocator = Mockito.mock(BufferAllocator.class)) {

            Mockito.when(parentAllocator.getName()).thenReturn("parentAllocator");
            Mockito.when(childAllocator.getName()).thenReturn("childAllocator");
            Mockito.when(childAllocator.getLimit()).thenReturn(1024L);

            assertDoesNotThrow(() -> listener.onChildRemoved(parentAllocator, childAllocator));
        }
    }


}