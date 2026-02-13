#!/bin/bash

# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

set -e

TYPE=$1
TAG=$2
SOURCE=$3

if [ -z "$TAG" ]; then
    echo "Usage: $0 <tag> <path to source>"
    exit 1
fi  

if [ -z "$SOURCE" ]; then
    echo "Usage: $0 <tag> <path to source>"
    exit 1
fi

docker build --build-arg BUILD_TYPE=$1 ${DOCKER_MIRROR:+--build-arg DOCKER_MIRROR=$DOCKER_MIRROR} -t chimera/chimera:$TAG $SOURCE
