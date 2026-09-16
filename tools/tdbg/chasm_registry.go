package tdbg

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/all"
	chasmtests "go.temporal.io/server/chasm/lib/tests"
	"go.temporal.io/server/common/log"
)

func newChasmRegistry(logger log.Logger) (*chasm.Registry, error) {
	registry, err := all.NewRegistry(logger)
	if err != nil {
		return nil, err
	}

	// The test library lives under chasm/lib/tests and is deliberately not part of the shared
	// set; tdbg registers it so test archetypes stay decodable.
	if err := registry.Register(chasmtests.Library); err != nil {
		return nil, err
	}

	return registry, nil
}
