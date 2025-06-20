package temporal

import (
	"time"

	"go.temporal.io/server/common/primitives"
)

const (
	mismatchLogMessage  = "Supplied configuration key/value mismatches persisted cluster metadata. Continuing with the persisted value as this value cannot be changed once initialized."
	serviceStartTimeout = time.Duration(15) * time.Second
	serviceStopTimeout  = time.Duration(5) * time.Minute
)

type (
	Server interface {
		Start() error
		Stop() error
	}
)

var (
	// Services is the set of all valid temporal services as strings (needs to be strings to
	// keep ServerOptions interface stable)
	Services = []string{
		string(primitives.FrontendService),
		string(primitives.InternalFrontendService),
		string(primitives.HistoryService),
		string(primitives.MatchingService),
		string(primitives.WorkerService),
	}

	// DefaultServices is the set of services to start by default if services are not given on
	// the command line.
	DefaultServices = []string{
		string(primitives.FrontendService),
		string(primitives.HistoryService),
		string(primitives.MatchingService),
		string(primitives.WorkerService),
	}
)

// NewServer returns a new instance of server that serves one or many services.
func NewServer(opts ...ServerOption) (Server, error) {
	return NewServerFx(TopLevelModule, opts...)
}
