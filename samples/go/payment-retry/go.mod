module temporal.io/samples/go/payment-retry

go 1.21

require (
	go.temporal.io/sdk v0.35.0
	github.com/uber-go/tally/v4 v4.1.7
)

// Transitive dependencies like github.com/prometheus/client_golang
// will be added by 'go mod tidy' or 'go mod download'.
// For now, only direct dependencies are listed.
