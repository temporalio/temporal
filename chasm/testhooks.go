package chasm

import "go.temporal.io/server/common/testing/testhooks"

var (
	RegistryInitializer    = testhooks.NewGlobalKey[func(*Registry) error]()
	HistoryRuntimeProvider = testhooks.NewGlobalKey[func(Engine, VisibilityManager, *Registry)]()
)
