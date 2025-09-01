#!/bin/bash

# SPDX-FileCopyrightText: 2025 Ben Jarvis
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

docker build --build-arg BUILD_TYPE=$1 -t chimera/chimera:$TAG $SOURCE
