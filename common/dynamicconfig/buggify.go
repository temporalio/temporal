package dynamicconfig

import (
	"fmt"
	"maps"
	"sync"
)

var (
	buggifiableMu sync.Mutex
	buggifiable   = map[Key][]any{}
)

// Buggifiable marks setting as one that test builds may randomize when they run with BUGGIFY, and
// returns it. The owner of the setting marks it, next to its declaration, and lists alternative
// values that are legal in every deployment and likely to expose bugs, such as a batch size of 1.
// A BUGGIFY run picks one of them for the whole run from its seed, unless dynamic config sets a
// value without constraints for the setting. It has no effect on builds without BUGGIFY, and
// doesn't change how the setting is read.
//
// It must be called from static initializers, and panics if a value is not valid for the setting
// or the setting is already marked.
func Buggifiable[T any, S interface {
	GenericSetting
	WithDefault(T) S
}](setting S, values ...T) S {
	if len(values) == 0 {
		//nolint:forbidigo // only called during static initialization
		panic(fmt.Sprintf("dynamicconfig.Buggifiable(%s) requires values", setting.Key()))
	}
	anyValues := make([]any, len(values))
	for i, v := range values {
		if err := setting.Validate(v); err != nil {
			//nolint:forbidigo // only called during static initialization
			panic(fmt.Sprintf("dynamicconfig.Buggifiable(%s): invalid value %v: %v", setting.Key(), v, err))
		}
		anyValues[i] = v
	}
	buggifiableMu.Lock()
	defer buggifiableMu.Unlock()
	if _, ok := buggifiable[setting.Key()]; ok {
		//nolint:forbidigo // only called during static initialization
		panic(fmt.Sprintf("dynamicconfig.Buggifiable(%s) called twice", setting.Key()))
	}
	buggifiable[setting.Key()] = anyValues
	return setting
}

// BuggifiableSettings returns the alternative values of the settings marked with Buggifiable, by
// key.
func BuggifiableSettings() map[Key][]any {
	buggifiableMu.Lock()
	defer buggifiableMu.Unlock()
	return maps.Clone(buggifiable)
}
