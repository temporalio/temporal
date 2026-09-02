package all

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/workflow"
)

// RegisterAll registers all CHASM libraries in their decoding (nil handler) mode into registry.
// Used by tdbg to decode mutable state payloads without pulling in production dependencies.
// When adding a new library, add a NewNilLibrary() call here.
func RegisterAll(registry *chasm.Registry) error {
	libs := []chasm.Library{
		&chasm.CoreLibrary{},
		activity.NewNilLibrary(),
		callback.NewNilLibrary(),
		nexusoperation.NewNilLibrary(),
		scheduler.NewNilLibrary(),
		workflow.NewNilLibrary(),
	}
	for _, lib := range libs {
		if err := registry.Register(lib); err != nil {
			return err
		}
	}
	return nil
}
