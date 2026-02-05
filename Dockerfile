# SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
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
    apt-get -y --no-install-recommends install clang cmake ninja-build git flex bison uuid-dev uthash-dev libkrb5-3 libkrb5-dev libgssapi-krb5-2  gss-ntlmssp-dev \
    librdmacm-dev libjansson-dev libclang-rt-18-dev llvm libxxhash-dev liburcu-dev liburing-dev libunwind-dev librocksdb-dev libssl-dev openssl libnuma-dev \
    python3 python3-pip python3-venv python3-requests && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# We need gssapi-ntlm 1.3.x for a fix to work with recent versions of windows
# For now, hack in a build from source until we can get it packaged properly later
RUN apt-get update
RUN apt-get install -y build-essential git autoconf automake libtool pkg-config gettext xsltproc docbook-xml docbook-xsl \
    libgssapi-krb5-2 libkrb5-dev libssl-dev libunistring-dev libwbclient-dev zlib1g-dev libxml2-utils ca-certificates && \
    git clone https://github.com/gssapi/gss-ntlmssp.git && \
    cd gss-ntlmssp && \
    autoreconf -fvi && \
    ./configure --prefix=/usr --libdir=/usr/lib/`uname -m`-linux-gnu && \
    make -j"$(nproc)" && \
    make install && \
    ldconfig && \
    cp -a /usr/lib/`uname -m`-linux-gnu/gssntlmssp/gssntlmssp.so /tmp/gssntlmssp.so && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf /gss-ntlmssp

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
    libasan8 liburing2 libunwind8 librocksdb8.9 gss-ntlmssp libkrb5-3 libkrb5-dev libgssapi-krb5-2 gss-ntlmssp-dev openssl libnuma1 \
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

# Copy built gss-ntlmssp
COPY --from=build /tmp/gssntlmssp.so /tmp/gssntlmssp.so

RUN cp -a /tmp/gssntlmssp.so /usr/lib/`uname -m`-linux-gnu/gssntlmssp/gssntlmssp.so

COPY /suppressions.txt /suppressions.txt

ENV LSAN_OPTIONS=suppressions=/suppressions.txt

# Just to check it will at least execute at build time
RUN /usr/local/sbin/chimera -v

ENTRYPOINT ["/usr/local/sbin/chimera"]

