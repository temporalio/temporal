package dynamicconfig

import (
	"fmt"
	"strings"
	"sync/atomic"
)

type (
	registry struct {
		settings map[string]GenericSetting
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
		globalRegistry.settings = make(map[string]GenericSetting)
	}
	keyStr := strings.ToLower(s.Key().String())
	if globalRegistry.settings[keyStr] != nil {
		panic(fmt.Sprintf("duplicate registration of dynamic config key: %q", keyStr))
	}
	globalRegistry.settings[keyStr] = s
}

func queryRegistry(k Key) GenericSetting {
	if !globalRegistry.queried.Load() {
		globalRegistry.queried.Store(true)
	}
	return globalRegistry.settings[strings.ToLower(k.String())]
}

func GetDefaultValues(k Key) []ConstrainedValue {
	s := queryRegistry(k)
	return s.DefaultValue()
}

// For testing only; do not call from regular code!
func ResetRegistryForTest() {
	globalRegistry.settings = nil
	globalRegistry.queried.Store(false)
}
