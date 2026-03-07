package dynamicconfig

import (
	"fmt"
	"sync/atomic"
)

type (
	registry struct {
		settings map[Key]GenericSetting
		regNames map[Key]string
		queried  atomic.Bool
	}
)

var (
	globalRegistry registry
)

func register(s GenericSetting, regName string) {
	if globalRegistry.queried.Load() {
		panic("dynamicconfig.New*Setting must only be called from static initializers")
	}
	if globalRegistry.settings == nil {
		globalRegistry.settings = make(map[Key]GenericSetting)
		globalRegistry.regNames = make(map[Key]string)
	}
	if globalRegistry.settings[s.Key()] != nil {
		// nolint:forbidigo // only called during static initialization
		panic(fmt.Sprintf("duplicate registration of dynamic config key: %q", s.Key().String()))
	}
	globalRegistry.settings[s.Key()] = s
	globalRegistry.regNames[s.Key()] = regName
}

func queryRegistry(k Key) GenericSetting {
	if !globalRegistry.queried.Load() {
		globalRegistry.queried.Store(true)
	}
	return globalRegistry.settings[k]
}

func getRegisteredName(k Key) (string, bool) {
	if !globalRegistry.queried.Load() {
		globalRegistry.queried.Store(true)
	}
	name, ok := globalRegistry.regNames[k]
	return name, ok
}

// For testing only; do not call from regular code!
func ResetRegistryForTest() {
	globalRegistry.settings = nil
	globalRegistry.regNames = nil
	globalRegistry.queried.Store(false)
}
