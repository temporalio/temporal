package dynamicconfig

import (
	"fmt"
	"sync/atomic"
)

type (
	registry struct {
		settings map[Key]GenericSetting
		queried  atomic.Bool
	}
)

var (
	globalRegistry registry
)

func register(s GenericSetting) {
	if globalRegistry.queried.Load() {
		panic("dynamicconfig.New*Setting must only be called from static initializers")
	}
	if globalRegistry.settings == nil {
		globalRegistry.settings = make(map[Key]GenericSetting)
	}
	lowerKey := s.Key().Lower()
	if globalRegistry.settings[lowerKey] != nil {
		panic(fmt.Sprintf("duplicate registration of dynamic config key: %q", lowerKey))
	}
	globalRegistry.settings[lowerKey] = s
}

func queryRegistry(k Key) GenericSetting {
	if !globalRegistry.queried.Load() {
		globalRegistry.queried.Store(true)
	}
	return globalRegistry.settings[k.Lower()]
}

// For testing only; do not call from regular code!
func ResetRegistryForTest() {
	globalRegistry.settings = nil
	globalRegistry.queried.Store(false)
}
