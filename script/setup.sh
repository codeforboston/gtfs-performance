#!/bin/bash


if ! which protoc; then
    sudo apt-get install protobuf-compiler
fi

out_dir=$(cd $(dirname "$BASH_SOURCE") && cd .. && pwd)

if [ ! -f "gtfs-realtime.proto" ]; then
    wget https://developers.google.com/transit/gtfs-realtime/gtfs-realtime.proto
fi
mkdir -p "$out_dir/src/java"
protoc -I="." --java_out="$out_dir/src/java" "./gtfs-realtime.proto"
