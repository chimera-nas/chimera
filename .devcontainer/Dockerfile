# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

ARG DOCKER_MIRROR
FROM ${DOCKER_MIRROR}ubuntu:26.04
ARG APT_MIRROR
ARG ENABLE_XLIO=1
ARG ENABLE_DOCA=0
ARG ENABLE_FIO=1
ARG ENABLE_ELBENCHO=1
ARG ENABLE_CLAUDE=1
ARG ENABLE_CODEX=1
ARG ENABLE_UV=1
ARG ENABLE_WPTS=1
# Pinned to an exact chimera-nas/pynfs commit (on branch chimera-fast-ci).  Using
# a SHA rather than a moving branch makes the image reproducible and, critically,
# busts the Docker layer cache whenever pynfs is bumped -- otherwise the cached
# clone layer keeps shipping a stale pynfs.  Bump this SHA when pynfs changes.
ARG PYNFS_REF=b861f7b8211b4dc798b88a7e81b300fb4615189c
# Pinned Microsoft Windows Protocol Test Suites (WPTS) FileServer release.
ARG WPTS_VERSION=4.26.3.0
ARG ELBENCHO_REF=96b1bf29c35e437715c0527194e69e4c96ed9922
ARG ENABLE_S3TESTS=1
# Pinned ceph/s3-tests commit. Bump to pull in new compliance cases; a SHA
# (not a branch) keeps the image reproducible and busts the layer cache on bump.
ARG S3TESTS_REF=5522d1c351f75bc00ae0f64f742f3f095f5939d9

ENV DEBIAN_FRONTEND=noninteractive

RUN if [ -n "$APT_MIRROR" ]; then \
    echo "deb $APT_MIRROR resolute main universe" > /etc/apt/sources.list.d/local-mirror.list && \
    echo "deb $APT_MIRROR resolute-updates main universe" >> /etc/apt/sources.list.d/local-mirror.list && \
    echo "deb $APT_MIRROR resolute-security main universe" >> /etc/apt/sources.list.d/local-mirror.list && \
    rm -f /etc/apt/sources.list.d/ubuntu.sources; \
    fi

RUN apt-get -y update && \
    apt-get -y --no-install-recommends upgrade && \
    apt-get -y --no-install-recommends install unminimize apt-utils && \
    yes | unminimize && \
    apt-get -y --no-install-recommends install clang clang-tools cmake ninja-build git flex bison lldb gdb less vim psmisc uncrustify \
    net-tools tshark tcpdump uuid-dev iproute2 man-db manpages-dev ca-certificates ssh libjansson-dev libclang-rt-18-dev llvm wget \
    libxxhash-dev liburcu-dev librdmacm-dev liburing-dev libaio-dev libunwind-dev librocksdb-dev libcurl4-openssl-dev clangd uthash-dev libnuma-dev \
    libboost-dev libboost-program-options-dev libboost-thread-dev libboost-system-dev \
    libcurl4-openssl-dev build-essential ruby-full autoconf automake make libtool pkg-config libs3-dev gh npm reuse libssl-dev openssl \
    libkrb5-3 libkrb5-dev libgssapi-krb5-2 libwbclient-dev curl jq libicu-dev awscli \
    krb5-kdc krb5-admin-server krb5-user python3-pip python3-setuptools python3-venv \
    dh-python pybuild-plugin-pyproject debhelper-compat \
    samba samba-ad-dc samba-ad-provision samba-vfs-modules smbclient samba-testsuite && \
    gem install jekyll bundler

RUN if [ "$ENABLE_CLAUDE" = "1" ] ; then \
    npm install -g @anthropic-ai/claude-code ; \
    fi

RUN if [ "$ENABLE_CODEX" = "1" ] ; then \
    npm install -g @openai/codex ; \
    fi

RUN if [ "$ENABLE_FIO" = "1" ] ; then \
    git clone --depth 1 --branch fio-3.40 https://github.com/axboe/fio.git /fio && \
    cd /fio && \
    ./configure --disable-libnfs && \
    make -j8 && \
    make install ; \
    fi

RUN if [ "$ENABLE_ELBENCHO" = "1" ] ; then \
    git clone https://github.com/chimera-nas/elbencho.git /elbencho && \
    cd /elbencho && \
    git checkout "$ELBENCHO_REF" && \
    make -j8 && \
    make install ; \
    fi

RUN if [ "$ENABLE_DOCA" = "1" ] ; then \
    wget https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2404/sbsa/cuda-keyring_1.1-1_all.deb && \
    dpkg -i cuda-keyring_1.1-1_all.deb && \
    apt-get update && \
    apt-get -y install cuda-toolkit-12-8 ; \
    fi

RUN if [ "$ENABLE_XLIO" = "1" ] ; then \
    git clone --depth 1 --branch gcc_15.2.0_build_fix https://github.com/arenaud16/libdpcp.git /libdpcp && \
    cd /libdpcp && \
    ./autogen.sh && \
    ./configure && \
    make -j8 && \
    make install && \
    git clone --depth 1 --branch 3.60.2-nlfix https://github.com/benjarvis/libxlio.git /libxlio && \
    cd /libxlio && \
    ./autogen.sh && \
    ./configure --with-dpcp=/usr/local && \
    make -j8 && \
    make install ; \
    fi

RUN apt-get -y update && \
    apt-get -y --no-install-recommends install qemu-system-x86  qemu-utils && \
    install -m 0755 -d /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc && \
    chmod a+r /etc/apt/keyrings/docker.asc && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu resolute stable" \
        > /etc/apt/sources.list.d/docker.list && \
    apt-get -y update && \
    apt-get -y --no-install-recommends install docker-ce docker-ce-cli containerd.io docker-buildx-plugin && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN apt-get -y update && \
    apt-get -y --no-install-recommends install python3-ply python3-gssapi python3-pip && \
    pip3 install --break-system-packages xdrlib3 && \
    git clone https://github.com/chimera-nas/pynfs.git /opt/pynfs && \
    cd /opt/pynfs && git checkout "$PYNFS_REF" && python3 ./setup.py build && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN if [ "$ENABLE_UV" = "1" ] ; then \
    curl -LsSf https://astral.sh/uv/install.sh | sh ; \
    fi

# ceph/s3-tests: an unofficial-but-broad S3 API compatibility suite, run against
# the chimera S3 server (src/server/s3/tests/ceph wires it into ctest). Cloned at
# a pinned SHA with its own venv so the system python is untouched.
ENV S3TESTS_DIR=/opt/s3-tests
RUN if [ "$ENABLE_S3TESTS" = "1" ] ; then \
    git clone https://github.com/ceph/s3-tests.git "$S3TESTS_DIR" && \
    cd "$S3TESTS_DIR" && git checkout "$S3TESTS_REF" && \
    /root/.local/bin/uv venv .venv --python 3.12 && \
    /root/.local/bin/uv pip install --python .venv -r requirements.txt pytest-timeout -e . && \
    chmod -R a+rx "$S3TESTS_DIR/.venv" ; \
    fi

# .NET 8 + Microsoft Windows Protocol Test Suites (WPTS) FileServer suite, used
# by the MS-SMB2 server conformance ctests (src/server/smb/tests). WPTS speaks
# SMB2 from its own managed stack, so it runs host-side in a netns against the
# chimera daemon -- no VM needed. CMake auto-detects /opt/dotnet and /opt/wpts.
ENV DOTNET_ROOT=/opt/dotnet
RUN if [ "$ENABLE_WPTS" = "1" ] ; then \
    curl -sSL https://dot.net/v1/dotnet-install.sh -o /tmp/dotnet-install.sh && \
    bash /tmp/dotnet-install.sh --channel 8.0 --install-dir /opt/dotnet && \
    ln -sf /opt/dotnet/dotnet /usr/local/bin/dotnet && \
    rm -f /tmp/dotnet-install.sh && \
    mkdir -p /opt/wpts && \
    curl -sSL "https://github.com/microsoft/WindowsProtocolTestSuites/releases/download/${WPTS_VERSION}/FileServer-TestSuite-ServerEP.tar.gz" -o /tmp/wpts.tar.gz && \
    tar -C /opt/wpts -xzf /tmp/wpts.tar.gz && \
    rm -f /tmp/wpts.tar.gz ; \
    fi
