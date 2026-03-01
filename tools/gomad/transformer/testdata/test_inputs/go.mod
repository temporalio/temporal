// NOTE: `go.mod` has to be here instead of its parent or the Go dependency resolution won't work
module go.temporal.io/server/tools/gomad/transformer/testdata

go 1.21

require github.com/pingcap/failpoint v0.0.0-20220801062533-2eaa32854a6c

require (
	github.com/pingcap/errors v0.11.4 // indirect
	github.com/stretchr/testify v1.9.0 // indirect
	go.uber.org/goleak v1.3.0 // indirect
)
