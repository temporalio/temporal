module build

go 1.18

require (
	go.temporal.io/api v1.29.0
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.3.0
)

require google.golang.org/protobuf v1.33.0 // indirect
