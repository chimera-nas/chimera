FROM ubuntu:24.04
ARG ENABLE_XLIO=1
ARG ENABLE_DOCA=0
ARG ENABLE_FIO=1

RUN apt-get -y update && \
    apt-get -y --no-install-recommends upgrade && \
    apt-get -y --no-install-recommends install unminimize && \
    echo y | unminimize && \
    apt-get -y --no-install-recommends install clang clang-tools cmake ninja-build git flex bison lldb gdb less libnfs-dev vim psmisc uncrustify \
    net-tools tshark tcpdump uuid-dev iproute2 man-db manpages-dev ca-certificates ssh libjansson-dev libclang-rt-18-dev llvm wget \
    libxxhash-dev liburcu-dev librdmacm-dev liburing-dev libunwind-dev librocksdb-dev libcurl4-openssl-dev clangd \
    libcurl4-openssl-dev build-essential ruby-full autoconf automake make libtool pkg-config libs3-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

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
    git clone --depth 1 --branch listen https://github.com/benjarvis/libxlio.git /libxlio && \
    cd /libxlio && \
    ./autogen.sh && \
    ./configure --with-dpcp=/usr/local && \
    make -j8 && \
    make install ; \
    fi
