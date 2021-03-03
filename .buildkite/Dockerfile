FROM golang:1.16

RUN apt update && apt install -y --no-install-recommends \
    protobuf-compiler

RUN mkdir -p /temporal
WORKDIR /temporal
