# SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

FROM ubuntu:24.04
ARG ENABLE_XLIO=1
ARG ENABLE_DOCA=0
ARG ENABLE_FIO=1
ARG ENABLE_CLAUDE=1
ARG ENABLE_UV=1

RUN sed -i 's|archive.ubuntu.com|azure.archive.ubuntu.com|g' /etc/apt/sources.list.d/ubuntu.sources && \
    sed -i 's|ports.ubuntu.com|azure.ports.ubuntu.com|g' /etc/apt/sources.list.d/ubuntu.sources

RUN apt-get -y update && \
    apt-get -y --no-install-recommends upgrade && \
    apt-get -y --no-install-recommends install unminimize && \
    echo y | unminimize && \
    apt-get -y --no-install-recommends install clang clang-tools cmake ninja-build git flex bison lldb gdb less libnfs-dev vim psmisc uncrustify \
    net-tools tshark tcpdump uuid-dev iproute2 man-db manpages-dev ca-certificates ssh libjansson-dev libclang-rt-18-dev llvm wget \
    libxxhash-dev liburcu-dev librdmacm-dev liburing-dev libunwind-dev librocksdb-dev libcurl4-openssl-dev clangd uthash-dev libnuma-dev \
    libcurl4-openssl-dev build-essential ruby-full autoconf automake make libtool pkg-config libs3-dev gh npm reuse libssl-dev openssl \
    gss-ntlmssp-dev libkrb5-3 libkrb5-dev libgssapi-krb5-2 curl && \
    gem install jekyll bundler 

RUN if [ "$ENABLE_CLAUDE" = "1" ] ; then \
    npm install -g @anthropic-ai/claude-code ; \
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

RUN if [ "$ENABLE_UV" = "1" ] ; then \
    curl -LsSf https://astral.sh/uv/install.sh | sh ; \
    fi
