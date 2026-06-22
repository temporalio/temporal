package chasm

import "go.temporal.io/server/common/testing/testhooks"

var HistoryRuntimeProvider = testhooks.NewGlobalKey[func(Engine, VisibilityManager, *Registry)]()
