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

/**
 * @author yuexie
 * @date 2025/4/14 10:46
 **/
public class JvmMetricsRegistrar {
    public static void registerJvmMemoryMetrics(MeterRegistry registry) {
        new JvmMemoryMetrics().bindTo(registry);
    }

    public static void registerJvmThreadMetrics(MeterRegistry registry) {
        new JvmThreadMetrics().bindTo(registry);
    }

    public static void registerJvmGcMetrics(MeterRegistry registry) {
        try (JvmGcMetrics jvmGcMetrics = new JvmGcMetrics()) {
            jvmGcMetrics.bindTo(registry);
        }
    }
    public static void registerClassLoaderMetrics(MeterRegistry registry) {
        new ClassLoaderMetrics().bindTo(registry);
    }
    public static void registerFileDescriptorMetrics(MeterRegistry registry) {
        new FileDescriptorMetrics().bindTo(registry);
    }
}
