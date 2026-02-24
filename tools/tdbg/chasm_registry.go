package tdbg

import (
	"go.temporal.io/server/chasm"
	chasmscheduler "go.temporal.io/server/chasm/lib/scheduler"
	chasmtests "go.temporal.io/server/chasm/lib/tests"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common/log"
)

func newChasmRegistry(logger log.Logger) (*chasm.Registry, error) {
	registry := chasm.NewRegistry(logger)

	if err := registry.Register(&chasm.CoreLibrary{}); err != nil {
		return nil, err
	}

	if err := registry.Register(chasmworkflow.NewLibrary()); err != nil {
		return nil, err
	}

	if err := registry.Register(chasmscheduler.NewLibrary(nil, nil, nil, nil, nil, nil)); err != nil {
		return nil, err
	}

	if err := registry.Register(chasmtests.Library); err != nil {
		return nil, err
	}

	// Note: Activity and Callback libraries are not included because their constructors
	// are unexported. Add them if/when they're needed.

	return registry, nil
}
