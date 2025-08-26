# Copyright 2024 Ant Group Co., Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM secretflow-registry.cn-hangzhou.cr.aliyuncs.com/secretflow/java-base:17.0.13-jre-anolis23

ENV LANG=C.UTF-8
WORKDIR /app

# fix: RunP proot + java bug
RUN ln -s ${JAVA_HOME}/lib/libjli.so /lib64

COPY dataproxy-server/target/dataproxy-server-0.0.1-SNAPSHOT.jar dataproxy.jar
COPY libs/*.jar libs/

# At present, when the concurrency of the task is not high,
# the heap memory consumption is not large, the maximum setting is 512M,
# if the resources are sufficient, and the concurrency can be appropriately increased,
# but the MaxDirectMemorySize needs to be increased according to the situation,
# the current default setting is 1536m,
# You can override this configuration by adding environment variables
ENV JAVA_OPTS="-server -XX:+UseG1GC -XX:+UseContainerSupport -Xms256m -Xmx512m -XX:MaxDirectMemorySize=1536m -XX:+DisableExplicitGC -XX:InitiatingHeapOccupancyPercent=68 -Xlog:gc*=info:file=gc.log:time,tags:filecount=5,filesize=10M -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/app/log -XX:ErrorFile=/app/log/hs_err_pid%p.log"
ENV LOG_LEVEL=INFO
EXPOSE 8023
ENTRYPOINT ${JAVA_HOME}/bin/java ${JAVA_OPTS} -Dsun.net.http.allowRestrictedHeaders=true --add-opens=java.base/java.nio=ALL-UNNAMED -jar ./dataproxy.jar