FROM golang:1.15

RUN apt update && apt install -y --no-install-recommends \
    protobuf-compiler

RUN mkdir -p /temporal
WORKDIR /temporal
