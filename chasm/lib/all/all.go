// Package all provides the canonical list of all CHASM libraries for decoding contexts.
// When adding a new library to chasm/lib, add its NewNilLibrary() call here and run
// the unit tests — TestAllLibrariesRegistered will fail if a library is missing.
package all

import (
	"go.temporal.io/server/chasm"
	activitylib "go.temporal.io/server/chasm/lib/activity"
	callbacklib "go.temporal.io/server/chasm/lib/callback"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	chasmscheduler "go.temporal.io/server/chasm/lib/scheduler"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
)

// RegisterAll registers all CHASM libraries in their decoding (nil handler) mode into registry.
func RegisterAll(registry *chasm.Registry) error {
	libs := []chasm.Library{
		&chasm.CoreLibrary{},
		chasmworkflow.NewNilLibrary(),
		activitylib.NewNilLibrary(),
		chasmscheduler.NewNilLibrary(),
		callbacklib.NewNilLibrary(),
		chasmnexus.NewNilLibrary(),
	}
	for _, lib := range libs {
		if err := registry.Register(lib); err != nil {
			return err
		}
	}
	return nil
}
