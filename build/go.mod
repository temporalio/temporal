module build

go 1.18

require (
	go.temporal.io/api v1.44.1
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.3.0
)

require google.golang.org/protobuf v1.34.2 // indirect
