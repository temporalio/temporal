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
