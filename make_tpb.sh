#!/bin/bash

GO111MODULE=off go get -u github.com/gogo/protobuf/protoc-gen-gogoslick
GO111MODULE=off go get -u go.uber.org/yarpc/encoding/protobuf/protoc-gen-yarpc-go

git submodule update --remote

protoc --proto_path=tpb --gogoslick_out=paths=source_relative:tpb tpb/*.proto 
protoc --proto_path=tpb --yarpc-go_out=tpb tpb/*.proto 
