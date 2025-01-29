FROM ubuntu:24.04
ARG ENABLE_XLIO=1

RUN apt-get -y update && \
    apt-get -y --no-install-recommends upgrade && \
    apt-get -y --no-install-recommends install unminimize && \
    echo y | unminimize && \
    apt-get -y --no-install-recommends install clang cmake ninja-build git flex bison lldb gdb less libnfs-dev vim psmisc uncrustify \
    net-tools tshark tcpdump uuid-dev iproute2 man-db manpages-dev ca-certificates ssh libjansson-dev libclang-rt-18-dev llvm \
    libxxhash-dev liburcu-dev librdmacm-dev liburing-dev libunwind-dev librocksdb-dev libcurl4-openssl-dev \
    libcurl4-openssl-dev build-essential ruby-full autoconf automake make libtool pkg-config && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN if [ "$ENABLE_XLIO" = "1" ] ; then \
    git clone https://github.com/Mellanox/libdpcp.git /libdpcp && \
    cd /libdpcp && \
    ./autogen.sh && \
    ./configure && \
    make -j8 && \
    make install && \
    git clone https://github.com/benjarvis/libxlio.git /libxlio && \
    cd /libxlio && \
    git checkout listen && \
    ./autogen.sh && \
    ./configure --with-dpcp=/usr/local && \
    make -j8 && \
    make install ; \
    fi
