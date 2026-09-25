package tdbg

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/all"
	"go.temporal.io/server/common/log"
)

func newChasmRegistry(logger log.Logger) (*chasm.Registry, error) {
	registry := chasm.NewRegistry(logger)
	if err := all.RegisterAll(registry); err != nil {
		return nil, err
	}
	return registry, nil
}
