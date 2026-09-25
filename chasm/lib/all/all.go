// Package all registers every CHASM library with nil handlers, for callers that decode
// persisted trees without linking production dependencies, such as tdbg.
package all

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common/log"
)

// NewRegistry returns a registry holding every CHASM library, with nil handlers. Callers
// needing libraries of their own can register them onto the result, as tdbg does.
//
// When adding a library under chasm/lib, export a NewNilLibrary() and add a line to the libs
// slice below; TestAllNilLibrariesRegistered fails if the two drift.
//
// A new entry makes that library's persisted state decodable by every offline reader,
// including ones that write it to long lived external storage. Add deliberately.
func NewRegistry(logger log.Logger) (*chasm.Registry, error) {
	libs := []chasm.Library{
		&chasm.CoreLibrary{},
		activity.NewNilLibrary(),
		callback.NewNilLibrary(),
		nexusoperation.NewNilLibrary(),
		scheduler.NewNilLibrary(),
		workflow.NewNilLibrary(),
	}

	registry := chasm.NewRegistry(logger)
	for _, lib := range libs {
		if err := registry.Register(lib); err != nil {
			return nil, err
		}
	}
	return registry, nil
}
