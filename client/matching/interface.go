package matching

import m "github.com/uber/cadence/.gen/go/matching"

// Client is the interface exposed by matching service client
type Client interface {
	m.TChanMatchingService
}
