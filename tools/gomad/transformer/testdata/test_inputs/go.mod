// NOTE: `go.mod` has to be here instead of its parent or the Go dependency resolution won't work
module go.temporal.io/server/tools/gomad/transformer/testdata

go 1.24.0

require github.com/pingcap/failpoint v0.0.0-20220801062533-2eaa32854a6c

require (
	github.com/pingcap/errors v0.11.4 // indirect
	github.com/stretchr/testify v1.9.0 // indirect
	go.uber.org/goleak v1.3.0 // indirect
	golang.org/x/net v0.48.0 // indirect
	golang.org/x/sys v0.39.0 // indirect
	golang.org/x/text v0.32.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251202230838-ff82c1b0f217 // indirect
	google.golang.org/grpc v1.79.1 // indirect
	google.golang.org/protobuf v1.36.10 // indirect
)
