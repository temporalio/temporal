module build

go 1.18

require (
	go.temporal.io/api v1.26.1-0.20231208002736-41ecb5363b8a
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.3.0
)

require google.golang.org/protobuf v1.31.0 // indirect
