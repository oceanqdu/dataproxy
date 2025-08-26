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

import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.AllocationOutcome;
import org.apache.arrow.memory.BufferAllocator;

/**
 * @author yuexie
 * @date 2025/4/14 16:14
 **/
@Slf4j
public class DataProxyAllocationListener implements AllocationListener {

    /**
     * Called each time a new buffer has been requested.
     *
     * <p>An exception can be safely thrown by this method to terminate the allocation.
     *
     * @param size the buffer size being allocated
     */
    @Override
    public void onPreAllocation(long size) {
        AllocationListener.super.onPreAllocation(size);
        log.debug("onPreAllocation, size: {}", size);
    }

    /**
     * Called each time a new buffer has been allocated.
     *
     * <p>An exception cannot be thrown by this method.
     *
     * @param size the buffer size being allocated
     */
    @Override
    public void onAllocation(long size) {
        AllocationListener.super.onAllocation(size);
        log.debug("onAllocation, size: {}", size);
    }

    /**
     * Informed each time a buffer is released from allocation.
     *
     * <p>An exception cannot be thrown by this method.
     *
     * @param size The size of the buffer being released.
     */
    @Override
    public void onRelease(long size) {
        AllocationListener.super.onRelease(size);
    }

    /**
     * Called whenever an allocation failed, giving the caller a chance to create some space in the
     * allocator (either by freeing some resource, or by changing the limit), and, if successful,
     * allowing the allocator to retry the allocation.
     *
     * @param size    the buffer size that was being allocated
     * @param outcome the outcome of the failed allocation. Carries information of what failed
     * @return true, if the allocation can be retried; false if the allocation should fail
     */
    @Override
    public boolean onFailedAllocation(long size, AllocationOutcome outcome) {
        log.debug("onFailedAllocation, size: {}, outcome: {}", size, outcome);
        return AllocationListener.super.onFailedAllocation(size, outcome);
    }

    /**
     * Called immediately after a child allocator was added to the parent allocator.
     *
     * @param parentAllocator The parent allocator to which a child was added
     * @param childAllocator  The child allocator that was just added
     */
    @Override
    public void onChildAdded(BufferAllocator parentAllocator, BufferAllocator childAllocator) {
        AllocationListener.super.onChildAdded(parentAllocator, childAllocator);
        log.debug("onChildAdded, childAllocator: {}, size: {}", childAllocator.getName(), childAllocator.getLimit());
    }

    /**
     * Called immediately after a child allocator was removed from the parent allocator.
     *
     * @param parentAllocator The parent allocator from which a child was removed
     * @param childAllocator  The child allocator that was just removed
     */
    @Override
    public void onChildRemoved(BufferAllocator parentAllocator, BufferAllocator childAllocator) {
        AllocationListener.super.onChildRemoved(parentAllocator, childAllocator);
        log.debug("onChildRemoved, childAllocator: {}, size: {}", childAllocator.getName(), childAllocator.getLimit());
    }
}
