module go.temporal.io/server/tests/mixedbrain

go 1.26.4

require (
	github.com/blang/semver/v4 v4.0.0
	github.com/siderolabs/grpc-proxy v0.5.2
	github.com/stretchr/testify v1.11.1
	github.com/temporalio/omes v0.0.0-20260529203146-c6ee1f56c726
	go.temporal.io/api v1.63.4
	go.temporal.io/server v0.0.0-00010101000000-000000000000
	google.golang.org/grpc v1.80.0
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/gofrs/flock v0.13.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.29.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/nexus-rpc/nexus-proto-annotations v0.1.0 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.1 // indirect
	golang.org/x/net v0.57.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260420184626-e10c466a9529 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260420184626-e10c466a9529 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace go.temporal.io/server => ../..

replace go.temporal.io/sdk => go.temporal.io/sdk v1.42.0
