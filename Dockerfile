# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Global scope, ahead of every FROM: an ARG reaches a FROM line only if it is
# declared before the first FROM.  Below one it belongs to that stage instead
# and the prefix expands to empty, sending the pull to Docker Hub unmirrored.
ARG DOCKER_MIRROR

# Current ccache, mirrored into GHCR by .github/workflows/mirror-ccache.yml.
# CI overrides CCACHE_REGISTRY to the internal pull-through proxy so an image
# build never reaches out to ghcr.io -- the same public-default/internal-override
# split the KVM guest images use (see kvm/CMakeLists.txt).
#
# Not the distro package: CCACHE_REMOTE_STORAGE arrived in ccache 4.7, and
# rocky9 and ubuntu22 ship 4.6, which reads the pre-rename name and so silently
# ignored the shared cache -- those cells compiled everything cold, every run.
# One pinned upstream binary everywhere retires that whole class of skew.
ARG CCACHE_REGISTRY=ghcr.io/chimera-nas/ccache
ARG CCACHE_VERSION=4.14
FROM ${CCACHE_REGISTRY}:${CCACHE_VERSION} AS ccache

FROM ${DOCKER_MIRROR}ubuntu:26.04 AS build
ARG BUILD_TYPE=Release
ARG APT_MIRROR
ARG UBUNTU_MIRROR=""
ARG ENABLE_XLIO=0

# The mirrored ccache, ahead of anything the distro might provide on PATH.
COPY --from=ccache /ccache /usr/local/bin/ccache

# Two mirror mechanisms, both off by default, so a plain `docker build` reaches
# whatever the base image ships and nothing unexpected.
#
# APT_MIRROR=<url> replaces the sources list outright.  The public CI has no
# such mirror, but private forks do, and this is what they set.
#
# UBUNTU_MIRROR=azure instead swaps the archive hosts for Azure's, which is
# what the public CI passes: the GitHub-hosted runners are Azure VMs -- their
# own host apt is pointed there for the same reason -- and the public mirrors
# are periodically caught mid-sync, serving an index whose size disagrees with
# its Release file, which fails the whole image build.  A host swap rather than
# a rewritten sources list, so the suites, components and Signed-By the base
# image chose all survive; archive and security are amd64's two hosts, ports is
# arm64's, and each keeps its path under the Azure name.
#
# APT_MIRROR wins if both are set: a fork that has stood one up means it.
RUN if [ -n "$APT_MIRROR" ]; then \
    echo "deb $APT_MIRROR resolute main universe" > /etc/apt/sources.list.d/local-mirror.list && \
    echo "deb $APT_MIRROR resolute-updates main universe" >> /etc/apt/sources.list.d/local-mirror.list && \
    echo "deb $APT_MIRROR resolute-security main universe" >> /etc/apt/sources.list.d/local-mirror.list && \
    rm -f /etc/apt/sources.list.d/ubuntu.sources; \
    elif [ "$UBUNTU_MIRROR" = azure ]; then \
        sed -i -e 's|//archive\.ubuntu\.com|//azure.archive.ubuntu.com|g' \
               -e 's|//security\.ubuntu\.com|//azure.archive.ubuntu.com|g' \
               -e 's|//ports\.ubuntu\.com|//azure.ports.ubuntu.com|g' \
            /etc/apt/sources.list.d/ubuntu.sources; \
    fi

RUN apt-get -y update && \
    apt-get -y --no-install-recommends upgrade && \
    apt-get -y --no-install-recommends install gcc g++ cmake ninja-build git flex bison uuid-dev uthash-dev libkrb5-3 libkrb5-dev libgssapi-krb5-2 \
    librdmacm-dev libjansson-dev libxxhash-dev liburcu-dev liburing-dev libunwind-dev librocksdb-dev libssl-dev openssl libnuma-dev \
    libwbclient-dev libcrypt-dev python3 python3-pip python3-venv python3-requests pkg-config && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Note: We use our own NTLM implementation instead of gss-ntlmssp

RUN if [ "$ENABLE_XLIO" = "1" ] ; then \
git clone --depth 1 --branch gcc_15.2.0_build_fix https://github.com/arenaud16/libdpcp.git /libdpcp && \
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

FROM ${DOCKER_MIRROR}ubuntu:26.04
ARG BUILD_TYPE=Release
RUN apt-get -y update && \
    apt-get -y --no-install-recommends upgrade && \
    apt-get -y --no-install-recommends install libuuid1 librdmacm1 libjansson4 liburcu8t64 ibverbs-providers \
    libasan8 liburing2 libunwind8 librocksdb9.11 libkrb5-3 libgssapi-krb5-2 openssl libnuma1 libwbclient0 \
    python3 python3-requests && \
    if [ "${BUILD_TYPE}" = "Debug" ]; then \
    apt-get -y --no-install-recommends install llvm gdb ; \
    fi && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


ENV LD_LIBRARY_PATH=/usr/local/lib

COPY --from=build /usr/local/sbin/chimera /usr/local/sbin/chimera
COPY --from=build /usr/local/lib/* /usr/local/lib/

COPY /chimera-docker.json /usr/local/etc/chimera.json
COPY /suppressions.txt /suppressions.txt

RUN mkdir -p /export

ENV LSAN_OPTIONS=suppressions=/suppressions.txt

# Just to check it will at least execute at build time
RUN /usr/local/sbin/chimera -v

ENTRYPOINT ["/usr/local/sbin/chimera"]

