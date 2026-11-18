//go:build !test_dep

package failpoint

import (
	"go.temporal.io/server/api/failpointservice/v1"
)

type registryNoop struct {
	failpointservice.UnimplementedFailpointServiceServer
}

// NewRegistry creates a no-op failpoint registry for production builds.
func NewRegistry() Registry {
	return &registryNoop{}
}

func (r *registryNoop) Get(key Key) any {
	return nil
}

func (r *registryNoop) Set(key Key, fp any) {
}

func (r *registryNoop) Delete(key Key) {
}
