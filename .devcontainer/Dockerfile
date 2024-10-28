FROM ubuntu:24.04

ARG USERNAME
ARG USER_UID=1000
ARG USER_GID=$USER_UID

RUN apt-get -y update && \
    apt-get -y upgrade && \
    apt-get -y install clang cmake ninja-build git flex bison gdb less
