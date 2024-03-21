# syntax=docker/dockerfile:1.2

FROM golang:bullseye

FROM ubuntu:20.04

RUN DEBIAN_FRONTEND=noninteractive apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get -y install \
    gcc libssl-dev pkg-config protobuf-compiler \
    ca-certificates libssl1.1 vim strace lsof curl jq && \
    rm -rf /var/cache/apt /var/lib/apt/lists/*

ADD /substreams-sink-graph-load /app/substreams-sink-graph-load

ENV PATH "$PATH:/app"

ENTRYPOINT ["/app/substreams-sink-graph-load"]
