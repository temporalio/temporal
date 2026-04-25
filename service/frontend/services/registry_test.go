//go:build experimental

package services

import (
	"sort"
	"testing"
)

func TestRegisteredVariants(t *testing.T) {
	names := names()
	sort.Strings(names)
	t.Logf("variants compiled in: %v", names)
}
