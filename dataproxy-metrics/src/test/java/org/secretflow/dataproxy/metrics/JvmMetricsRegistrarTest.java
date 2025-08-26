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

package org.secretflow.dataproxy.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;

/**
 * @author yuexie
 * @date 2025/6/3 11:16
 **/
@ExtendWith(MockitoExtension.class)
public class JvmMetricsRegistrarTest {

    /**
     * Test Scenario: Under normal circumstances, the JVM memory metric is registered
     */
    @Test
    public void testRegisterJvmMemoryMetrics_success() {
        MeterRegistry registry = Mockito.mock(MeterRegistry.class);
        try (MockedConstruction<JvmMemoryMetrics> mocked = Mockito.mockConstruction(JvmMemoryMetrics.class)) {
            JvmMetricsRegistrar.registerJvmMemoryMetrics(registry);
            verify(mocked.constructed().get(0)).bindTo(registry);
        }
    }

    /**
     * Test Scenario: MeterRegistry is null, which registers JVM memory metrics
     */
    @Test
    public void testRegisterJvmMemoryMetrics_nullRegistry() {
        assertThrows(NullPointerException.class, () -> JvmMetricsRegistrar.registerJvmMemoryMetrics(null));
    }

    /**
     * Test Scenario: Normal, register the JVM thread metric
     */
    @Test
    public void testRegisterJvmThreadMetrics_success() {
        MeterRegistry registry = Mockito.mock(MeterRegistry.class);
        try (MockedConstruction<JvmThreadMetrics> mocked = Mockito.mockConstruction(JvmThreadMetrics.class)) {
            JvmMetricsRegistrar.registerJvmThreadMetrics(registry);
            verify(mocked.constructed().get(0)).bindTo(registry);
        }
    }

    /**
     * Test Scenario: MeterRegistry is null, which registers JVM thread metrics
     */
    @Test
    public void testRegisterJvmThreadMetrics_nullRegistry() {
        assertThrows(NullPointerException.class, () -> JvmMetricsRegistrar.registerJvmThreadMetrics(null));
    }

    /**
     * Test Scenario: Normal, register the JVM garbage collection metric
     */
    @Test
    public void testRegisterJvmGcMetrics_success() {
        MeterRegistry registry = Mockito.mock(MeterRegistry.class);
        try (MockedConstruction<JvmGcMetrics> mocked = Mockito.mockConstruction(JvmGcMetrics.class)) {
            JvmMetricsRegistrar.registerJvmGcMetrics(registry);
            verify(mocked.constructed().get(0)).bindTo(registry);
        }
    }

    /**
     * Test scenario: MeterRegistry is null and the JVM garbage collection metric is registered
     */
    @Test
    public void testRegisterJvmGcMetrics_nullRegistry() {
        assertThrows(NullPointerException.class, () -> JvmMetricsRegistrar.registerJvmGcMetrics(null));
    }

    /**
     * Test scenario: Normal, register the classloader metric
     */
    @Test
    public void testRegisterClassLoaderMetrics_success() {
        MeterRegistry registry = Mockito.mock(MeterRegistry.class);
        try (MockedConstruction<ClassLoaderMetrics> mocked = Mockito.mockConstruction(ClassLoaderMetrics.class)) {
            JvmMetricsRegistrar.registerClassLoaderMetrics(registry);
            verify(mocked.constructed().get(0)).bindTo(registry);
        }
    }

    /**
     * Test scenario: MeterRegistry is null, and the classloader indicator is registered
     */
    @Test
    public void testRegisterClassLoaderMetrics_nullRegistry() {
        assertThrows(NullPointerException.class, () -> JvmMetricsRegistrar.registerClassLoaderMetrics(null));
    }

    /**
     * Test Scenario: Normal, register file descriptor metrics
     */
    @Test
    public void testRegisterFileDescriptorMetrics_success() {
        MeterRegistry registry = Mockito.mock(MeterRegistry.class);
        try (MockedConstruction<FileDescriptorMetrics> mocked = Mockito.mockConstruction(FileDescriptorMetrics.class)) {
            JvmMetricsRegistrar.registerFileDescriptorMetrics(registry);
            verify(mocked.constructed().get(0)).bindTo(registry);
        }
    }

    /**
     * Test scenario: MeterRegistry is null, and the file descriptor indicator is registered
     */
    @Test
    public void testRegisterFileDescriptorMetrics_nullRegistry() {
        assertThrows(NullPointerException.class, () -> JvmMetricsRegistrar.registerFileDescriptorMetrics(null));
    }
}
