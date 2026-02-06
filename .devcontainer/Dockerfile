# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

ARG DOCKER_MIRROR
ARG APT_MIRROR
ARG ENABLE_XLIO=1
ARG ENABLE_DOCA=0
ARG ENABLE_FIO=1
ARG ENABLE_CLAUDE=1
ARG ENABLE_CODEX=1
ARG ENABLE_UV=1
FROM ${DOCKER_MIRROR}ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN if [ -n "$APT_MIRROR" ]; then \
    echo "deb $APT_MIRROR noble main universe" > /etc/apt/sources.list.d/local-mirror.list && \
    echo "deb $APT_MIRROR noble-updates main universe" >> /etc/apt/sources.list.d/local-mirror.list && \
    echo "deb $APT_MIRROR noble-security main universe" >> /etc/apt/sources.list.d/local-mirror.list && \
    rm -f /etc/apt/sources.list.d/ubuntu.sources; \
    fi

RUN apt-get -y update && \
    apt-get -y --no-install-recommends upgrade && \
    apt-get -y --no-install-recommends install unminimize apt-utils && \
    yes | unminimize && \
    apt-get -y --no-install-recommends install clang clang-tools cmake ninja-build git flex bison lldb gdb less vim psmisc uncrustify \
    net-tools tshark tcpdump uuid-dev iproute2 man-db manpages-dev ca-certificates ssh libjansson-dev libclang-rt-18-dev llvm wget \
    libxxhash-dev liburcu-dev librdmacm-dev liburing-dev libaio-dev libunwind-dev librocksdb-dev libcurl4-openssl-dev clangd uthash-dev libnuma-dev \
    libcurl4-openssl-dev build-essential ruby-full autoconf automake make libtool pkg-config libs3-dev gh npm reuse libssl-dev openssl \
    libkrb5-3 libkrb5-dev libgssapi-krb5-2 libwbclient-dev curl jq \
    krb5-kdc krb5-admin-server krb5-user \
    samba samba-ad-dc samba-ad-provision samba-vfs-modules smbclient && \
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

RUN if [ "$ENABLE_DOCA" = "1" ] ; then \
    wget https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2404/sbsa/cuda-keyring_1.1-1_all.deb && \
    dpkg -i cuda-keyring_1.1-1_all.deb && \
    apt-get update && \
    apt-get -y install cuda-toolkit-12-8 ; \
    fi

RUN if [ "$ENABLE_XLIO" = "1" ] ; then \
    git clone --depth 1 https://github.com/Mellanox/libdpcp.git /libdpcp && \
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
    apt-get -y --no-install-recommends install qemu-kvm qemu-utils && \
    install -m 0755 -d /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc && \
    chmod a+r /etc/apt/keyrings/docker.asc && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu noble stable" \
        > /etc/apt/sources.list.d/docker.list && \
    apt-get -y update && \
    apt-get -y --no-install-recommends install docker-ce docker-ce-cli containerd.io && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN if [ "$ENABLE_UV" = "1" ] ; then \
    curl -LsSf https://astral.sh/uv/install.sh | sh ; \
    fi
