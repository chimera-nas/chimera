# SPDX-FileCopyrightText: 2025 Ben Jarvis
#
# SPDX-License-Identifier: Unlicense

FROM ubuntu:24.04 AS build
ARG BUILD_TYPE=Release


RUN sed -i 's|archive.ubuntu.com|azure.archive.ubuntu.com|g' /etc/apt/sources.list.d/ubuntu.sources && \
    sed -i 's|ports.ubuntu.com|azure.ports.ubuntu.com|g' /etc/apt/sources.list.d/ubuntu.sources

RUN apt-get -y update && \
    apt-get -y --no-install-recommends upgrade && \
    apt-get -y --no-install-recommends install clang cmake ninja-build git flex bison uuid-dev uthash-dev \
    librdmacm-dev libjansson-dev libclang-rt-18-dev llvm libxxhash-dev liburcu-dev liburing-dev libunwind-dev librocksdb-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY / /chimera

RUN mkdir -p /build
WORKDIR /build

RUN cmake -G Ninja -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DDISABLE_TESTS=ON /chimera && \
    ninja && \
    ninja install

FROM ubuntu:24.04
ARG BUILD_TYPE=Release
RUN apt-get -y update && \
    apt-get -y --no-install-recommends upgrade && \
    apt-get -y --no-install-recommends install libuuid1 librdmacm1 libjansson4 liburcu8t64 ibverbs-providers liburing2 libunwind8 librocksdb8.9 && \
    if [ "${BUILD_TYPE}" = "Debug" ]; then \
    apt-get -y --no-install-recommends install llvm gdb ; \
    fi && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV LD_LIBRARY_PATH=/usr/local/lib

COPY --from=build /usr/local/etc/chimera.json /usr/local/etc/chimera.json
COPY --from=build /usr/local/sbin/chimera /usr/local/sbin/chimera
COPY --from=build /usr/local/lib/* /usr/local/lib/

# Just to check it will at least execute at build time
RUN /usr/local/sbin/chimera -v

ENTRYPOINT ["/usr/local/sbin/chimera"]

