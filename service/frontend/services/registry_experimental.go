//go:build experimental

package services

import "fmt"

var entries = map[string]variant{}
var registryErr error

func register(name string, v variant) {
	if _, dup := entries[name]; dup {
		registryErr = errorsJoin(registryErr, fmt.Errorf("duplicate registration for variant %s", name))
		return
	}
	entries[name] = v
}

func get(name string) (variant, bool) {
	v, ok := entries[name]
	return v, ok
}

func names() []string {
	out := make([]string, 0, len(entries))
	for name := range entries {
		out = append(out, name)
	}
	return out
}

func registryError() error {
	return registryErr
}

func errorsJoin(current error, next error) error {
	if current == nil {
		return next
	}
	return fmt.Errorf("%w; %w", current, next)
}
