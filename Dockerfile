# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

ARG DOCKER_MIRROR
FROM ${DOCKER_MIRROR}ubuntu:24.04 AS build
ARG BUILD_TYPE=Release
ARG APT_MIRROR

RUN if [ -n "$APT_MIRROR" ]; then \
    echo "deb $APT_MIRROR noble main universe" > /etc/apt/sources.list.d/local-mirror.list && \
    echo "deb $APT_MIRROR noble-updates main universe" >> /etc/apt/sources.list.d/local-mirror.list && \
    echo "deb $APT_MIRROR noble-security main universe" >> /etc/apt/sources.list.d/local-mirror.list && \
    rm -f /etc/apt/sources.list.d/ubuntu.sources; \
    fi

RUN apt-get -y update && \
    apt-get -y --no-install-recommends upgrade && \
    apt-get -y --no-install-recommends install gcc g++ cmake ninja-build git flex bison uuid-dev uthash-dev libkrb5-3 libkrb5-dev libgssapi-krb5-2 \
    librdmacm-dev libjansson-dev libxxhash-dev liburcu-dev liburing-dev libunwind-dev librocksdb-dev libssl-dev openssl libnuma-dev \
    libwbclient-dev python3 python3-pip python3-venv python3-requests pkg-config && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Note: We use our own NTLM implementation instead of gss-ntlmssp

RUN if [ "$ENABLE_XLIO" = "1" ] ; then \
git clone https://github.com/Mellanox/libdpcp.git /libdpcp && \
cd /libdpcp && \
./autogen.sh && \
./configure && \
make -j8 && \
make install && \
git clone https://github.com/benjarvis/libxlio.git /libxlio  && \
cd /libxlio && \
git checkout 3.60.2-nlfix && \
./autogen.sh && \
./configure --with-dpcp=/usr/local && \
make -j8 && \
make install ; \
fi

COPY / /chimera

RUN mkdir -p /build
WORKDIR /build

RUN cmake -G Ninja -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DDISABLE_TESTS=ON /chimera && \
    ninja && \
    ninja install

FROM ${DOCKER_MIRROR}ubuntu:24.04
ARG BUILD_TYPE=Release
RUN apt-get -y update && \
    apt-get -y --no-install-recommends upgrade && \
    apt-get -y --no-install-recommends install libuuid1 librdmacm1 libjansson4 liburcu8t64 ibverbs-providers \
    libasan8 liburing2 libunwind8 librocksdb8.9 libkrb5-3 libgssapi-krb5-2 openssl libnuma1 libwbclient0 \
    python3 python3-requests && \
    if [ "${BUILD_TYPE}" = "Debug" ]; then \
    apt-get -y --no-install-recommends install llvm gdb ; \
    fi && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


ENV LD_LIBRARY_PATH=/usr/local/lib

COPY --from=build /usr/local/etc/chimera.json /usr/local/etc/chimera.json
COPY --from=build /usr/local/sbin/chimera /usr/local/sbin/chimera
COPY --from=build /usr/local/lib/* /usr/local/lib/

COPY /suppressions.txt /suppressions.txt

ENV LSAN_OPTIONS=suppressions=/suppressions.txt

# Just to check it will at least execute at build time
RUN /usr/local/sbin/chimera -v

ENTRYPOINT ["/usr/local/sbin/chimera"]

