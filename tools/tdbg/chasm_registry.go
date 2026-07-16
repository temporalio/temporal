package tdbg

import (
	"go.temporal.io/server/chasm"
	activitylib "go.temporal.io/server/chasm/lib/activity"
	callbacklib "go.temporal.io/server/chasm/lib/callback"
	chasmscheduler "go.temporal.io/server/chasm/lib/scheduler"
	chasmtests "go.temporal.io/server/chasm/lib/tests"
	"go.temporal.io/server/chasm/lib/tquserdata"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common/log"
)

func newChasmRegistry(logger log.Logger) (*chasm.Registry, error) {
	registry := chasm.NewRegistry(logger)

	if err := registry.Register(&chasm.CoreLibrary{}); err != nil {
		return nil, err
	}

	if err := registry.Register(chasmworkflow.NewLibrary(chasmworkflow.NewRegistry())); err != nil {
		return nil, err
	}

	if err := registry.Register(activitylib.NewNilLibrary()); err != nil {
		return nil, err
	}

	if err := registry.Register(chasmscheduler.NewNilLibrary()); err != nil {
		return nil, err
	}

	if err := registry.Register(chasmtests.Library); err != nil {
		return nil, err
	}

	if err := registry.Register(callbacklib.NewNilLibrary()); err != nil {
		return nil, err
	}

	if err := registry.Register(tquserdata.Library); err != nil {
		return nil, err
	}

	return registry, nil
}
