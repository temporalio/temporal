module build

go 1.18

require (
	go.temporal.io/api v1.26.1-0.20240124000020-2149fa650d6b
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.3.0
)

require google.golang.org/protobuf v1.32.0 // indirect
