module build

go 1.18

replace go.temporal.io/api => github.com/tdeebswihart/temporal-api-go v0.0.0-20231020155344-f1f14322de41

require (
	go.temporal.io/api v0.0.0-00010101000000-000000000000
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.3.0
)

require (
	github.com/iancoleman/strcase v0.3.0 // indirect
	google.golang.org/protobuf v1.31.0 // indirect
)
