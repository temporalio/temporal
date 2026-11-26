package failpoint_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/parallelsuite"
)

type executeSuite struct {
	parallelsuite.Suite[*executeSuite]
}

func TestExecuteSuite(t *testing.T) {
	parallelsuite.Run(t, &executeSuite{})
}

func (s *executeSuite) TestExecuteSwitch() {
	newRegistry := func(keys ...failpoint.Key[int]) *failpoint.Registry {
		registry := failpoint.NewRegistry()
		for _, key := range keys {
			fp := failpoint.New[int](key)
			fp.On()
			registry.Set(fp)
		}
		return registry
	}

	tests := []struct {
		name       string
		registered []failpoint.Key[int]
		key        failpoint.Key[int]
		want       bool
	}{
		{name: "not registered", key: intKey, want: false},
		{name: "unscoped", registered: []failpoint.Key[int]{intKey}, key: intKey, want: true},
		{name: "scoped", registered: []failpoint.Key[int]{intKey.ForScope("ns-1")}, key: intKey.ForScope("ns-1"), want: true},
		{name: "scoped key falls back to unscoped", registered: []failpoint.Key[int]{intKey}, key: intKey.ForScope("ns-1"), want: true},
		{name: "other scope does not fire", registered: []failpoint.Key[int]{intKey.ForScope("ns-1")}, key: intKey.ForScope("ns-2"), want: false},
		{name: "unscoped key does not see scoped failpoints", registered: []failpoint.Key[int]{intKey.ForScope("ns-1")}, key: intKey, want: false},
	}
	for _, tc := range tests {
		s.Run(tc.name, func(s *executeSuite) {
			s.Equal(tc.want, failpoint.ExecuteSwitch(newRegistry(tc.registered...), tc.key))
		})
	}

	s.Run("scoped failpoint takes precedence over unscoped", func(s *executeSuite) {
		registry := newRegistry(intKey)
		scoped := failpoint.New[int](intKey.ForScope("ns-1"))
		registry.Set(scoped) // off
		s.False(failpoint.ExecuteSwitch(registry, intKey.ForScope("ns-1")))
		s.True(failpoint.ExecuteSwitch(registry, intKey))
	})

	s.Run("off", func(s *executeSuite) {
		registry := failpoint.NewRegistry()
		fp := failpoint.New[int](intKey)
		registry.Set(fp)
		s.False(failpoint.ExecuteSwitch(registry, intKey))

		fp.On()
		s.True(failpoint.ExecuteSwitch(registry, intKey))

		fp.Disable()
		s.False(failpoint.ExecuteSwitch(registry, intKey))
	})
}

func (s *executeSuite) TestExecuteIgnoresFailpointOfAnotherType() {
	// Typed keys keep this from happening by accident: the two keys have the same name.
	registry := failpoint.NewRegistry()
	fp := failpoint.New(failpoint.UndeclaredKey[int]("same.name"))
	fp.On()
	registry.Set(fp)

	s.False(failpoint.ExecuteSwitch(registry, failpoint.UndeclaredKey[error]("same.name")))
	s.True(failpoint.ExecuteSwitch(registry, failpoint.UndeclaredKey[int]("same.name")))
}

func (s *executeSuite) TestExecuteNilRegistry() {
	s.False(failpoint.ExecuteSwitch(nil, errorKey))
}

// Not in executeSuite because testing.AllocsPerRun cannot run in parallel with other tests.
func TestExecuteInertDoesNotAllocate(t *testing.T) {
	key := intKey.ForScope("ns")
	for _, tc := range []struct {
		name     string
		registry *failpoint.Registry
	}{
		{name: "nil registry", registry: nil},
		{name: "empty registry", registry: failpoint.NewRegistry()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			allocs := testing.AllocsPerRun(100, func() {
				_ = failpoint.ExecuteSwitch(tc.registry, key)
			})
			require.Zero(t, allocs)
		})
	}
}
