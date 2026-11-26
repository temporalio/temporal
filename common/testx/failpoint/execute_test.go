package failpoint_test

import (
	"errors"
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

func (s *executeSuite) TestExecute() {
	fromUnscoped := errors.New("unscoped")
	fromScoped := errors.New("scoped")
	newRegistry := func(keys map[failpoint.Key[error]]error) *failpoint.Registry {
		registry := failpoint.NewRegistry()
		for key, err := range keys {
			fp := failpoint.New[error](key)
			fp.Return(err)
			registry.Set(fp)
		}
		return registry
	}

	tests := []struct {
		name       string
		registered map[failpoint.Key[error]]error
		key        failpoint.Key[error]
		want       error
		wantSet    bool
	}{
		{name: "not registered", registered: nil, key: errorKey, want: nil, wantSet: false},
		{name: "unscoped", registered: map[failpoint.Key[error]]error{errorKey: fromUnscoped}, key: errorKey, want: fromUnscoped, wantSet: true},
		{
			name:       "scoped",
			registered: map[failpoint.Key[error]]error{errorKey.ForScope("ns-1"): fromScoped},
			key:        errorKey.ForScope("ns-1"),
			want:       fromScoped,
			wantSet:    true,
		},
		{
			name:       "scoped key falls back to unscoped",
			registered: map[failpoint.Key[error]]error{errorKey: fromUnscoped},
			key:        errorKey.ForScope("ns-1"),
			want:       fromUnscoped,
			wantSet:    true,
		},
		{
			name:       "scoped takes precedence over unscoped",
			registered: map[failpoint.Key[error]]error{errorKey: fromUnscoped, errorKey.ForScope("ns-1"): fromScoped},
			key:        errorKey.ForScope("ns-1"),
			want:       fromScoped,
			wantSet:    true,
		},
		{
			name:       "other scope does not fire",
			registered: map[failpoint.Key[error]]error{errorKey.ForScope("ns-1"): fromScoped},
			key:        errorKey.ForScope("ns-2"),
			want:       nil,
			wantSet:    false,
		},
		{
			name:       "unscoped key does not see scoped failpoints",
			registered: map[failpoint.Key[error]]error{errorKey.ForScope("ns-1"): fromScoped},
			key:        errorKey,
			want:       nil,
			wantSet:    false,
		},
	}
	for _, tc := range tests {
		s.Run(tc.name, func(s *executeSuite) {
			got, set := tc.key.Execute(newRegistry(tc.registered))
			s.Equal(tc.want, got)
			s.Equal(tc.wantSet, set)
		})
	}
}

func (s *executeSuite) TestExecuteIgnoresFailpointOfAnotherType() {
	// Typed keys keep this from happening by accident: the two keys have the same name.
	registry := failpoint.NewRegistry()
	fp := failpoint.New(failpoint.UndeclaredKey[int]("same.name"))
	fp.Return(1)
	registry.Set(fp)

	got, set := failpoint.UndeclaredKey[error]("same.name").Execute(registry)
	s.NoError(got)
	s.False(set)
	s.True(failpoint.UndeclaredKey[int]("same.name").Trip(registry))
}

func (s *executeSuite) TestExecuteOfErrorKey() {
	injected := errors.New("injected")
	tests := []struct {
		name      string
		configure func(fp *failpoint.Failpoint[error])
		want      error
	}{
		{name: "not configured", configure: func(*failpoint.Failpoint[error]) {}, want: nil},
		{name: "return error", configure: func(fp *failpoint.Failpoint[error]) { fp.Return(injected) }, want: injected},
		{name: "return nil", configure: func(fp *failpoint.Failpoint[error]) { fp.Return(nil) }, want: nil},
	}
	for _, tc := range tests {
		s.Run(tc.name, func(s *executeSuite) {
			registry := failpoint.NewRegistry()
			fp := failpoint.New[error](errorKey)
			tc.configure(fp)
			registry.Set(fp)
			s.Equal(tc.want, executeError(registry, errorKey))
		})
	}
}

func (s *executeSuite) TestExecuteNilRegistry() {
	got, set := errorKey.Execute(nil)
	s.False(set)
	s.NoError(got)
	s.NoError(executeError(nil, errorKey))
	s.False(errorKey.Trip(nil))
}

func (s *executeSuite) TestTrip() {
	tests := []struct {
		name      string
		configure func(*failpoint.Failpoint[int])
		want      bool
	}{
		{name: "on", configure: func(fp *failpoint.Failpoint[int]) { fp.On() }, want: true},
		{name: "return", configure: func(fp *failpoint.Failpoint[int]) { fp.Return(7) }, want: true},
		{name: "off", configure: func(fp *failpoint.Failpoint[int]) {}, want: false},
	}
	for _, tc := range tests {
		s.Run(tc.name, func(s *executeSuite) {
			fp := failpoint.New[int](intKey)
			tc.configure(fp)

			registry := failpoint.NewRegistry()
			registry.Set(fp)
			s.Equal(tc.want, intKey.Trip(registry))
		})
	}

	s.Run("not registered", func(s *executeSuite) {
		s.False(intKey.Trip(failpoint.NewRegistry()))
	})
}

// Not in executeSuite because testing.AllocsPerRun cannot run in parallel with other tests.
func TestExecuteInertDoesNotAllocate(t *testing.T) {
	key := errorKey.ForScope("ns")
	for _, tc := range []struct {
		name     string
		registry *failpoint.Registry
	}{
		{name: "nil registry", registry: nil},
		{name: "empty registry", registry: failpoint.NewRegistry()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			allocs := testing.AllocsPerRun(100, func() {
				_, _ = key.Execute(tc.registry)
				_, _ = key.Execute(tc.registry)
			})
			require.Zero(t, allocs)
		})
	}
}
