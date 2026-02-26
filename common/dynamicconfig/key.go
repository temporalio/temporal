package dynamicconfig

import (
	"strings"
	"unique"
)

type (
	// Key is a key/property stored in dynamic config.
	Key struct {
		handle unique.Handle[string] // string must be lower-case
	}
)

func MakeKey(s string) Key {
	return Key{handle: unique.Make(strings.ToLower(s))}
}

func (k Key) String() string {
	return k.handle.Value()
}

// RegisteredName returns the original name that the setting with this key was registered with.
// Note that it only works on registered settings (i.e. New{Scope}{Type}Setting). If called on
// a key that does not correspond to a registered setting, it will return the empty string.
func (k Key) RegisteredName() string {
	if name, ok := getRegisteredName(k); ok {
		return name
	}
	return ""
}
