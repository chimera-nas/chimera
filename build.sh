#!/bin/bash

set -e

TAG=$1
SOURCE=$2

if [ -z "$TAG" ]; then
    echo "Usage: $0 <tag> <path to source>"
    exit 1
fi  

if [ -z "$SOURCE" ]; then
    echo "Usage: $0 <tag> <path to source>"
    exit 1
fi

docker build -t chimera/chimera:$TAG $SOURCE
