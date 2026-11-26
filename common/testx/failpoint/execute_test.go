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
	newSource := func(keys map[failpoint.Key[error]]error) failpoint.Source {
		registry := failpoint.NewRegistry()
		for key, err := range keys {
			fp := failpoint.NewFailpoint[error](key)
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
		wantFired  bool
	}{
		{name: "not registered", registered: nil, key: key1, want: nil, wantFired: false},
		{name: "unscoped", registered: map[failpoint.Key[error]]error{key1: fromUnscoped}, key: key1, want: fromUnscoped, wantFired: true},
		{
			name:       "scoped",
			registered: map[failpoint.Key[error]]error{key1.ForScope("ns-1"): fromScoped},
			key:        key1.ForScope("ns-1"),
			want:       fromScoped,
			wantFired:  true,
		},
		{
			name:       "scoped key falls back to unscoped",
			registered: map[failpoint.Key[error]]error{key1: fromUnscoped},
			key:        key1.ForScope("ns-1"),
			want:       fromUnscoped,
			wantFired:  true,
		},
		{
			name:       "scoped takes precedence over unscoped",
			registered: map[failpoint.Key[error]]error{key1: fromUnscoped, key1.ForScope("ns-1"): fromScoped},
			key:        key1.ForScope("ns-1"),
			want:       fromScoped,
			wantFired:  true,
		},
		{
			name:       "other scope does not fire",
			registered: map[failpoint.Key[error]]error{key1.ForScope("ns-1"): fromScoped},
			key:        key1.ForScope("ns-2"),
			want:       nil,
			wantFired:  false,
		},
		{
			name:       "unscoped key does not see scoped failpoints",
			registered: map[failpoint.Key[error]]error{key1.ForScope("ns-1"): fromScoped},
			key:        key1,
			want:       nil,
			wantFired:  false,
		},
	}
	for _, tc := range tests {
		s.Run(tc.name, func(s *executeSuite) {
			got, fired := failpoint.Execute[error](newSource(tc.registered), tc.key)
			s.Equal(tc.want, got)
			s.Equal(tc.wantFired, fired)
		})
	}
}

func (s *executeSuite) TestExecuteIgnoresFailpointOfAnotherType() {
	// Typed keys keep this from happening by accident: the two keys have the same name.
	registry := failpoint.NewRegistry()
	fp := failpoint.NewFailpoint(failpoint.UndeclaredKey[int]("same.name"))
	fp.Return(1)
	registry.Set(fp)

	got, set := failpoint.Execute(registry, failpoint.UndeclaredKey[error]("same.name"))
	s.NoError(got)
	s.False(set)
	s.True(failpoint.ExecuteSwitch(registry, failpoint.UndeclaredKey[int]("same.name")))
}

func (s *executeSuite) TestCheckError() {
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
			fp := failpoint.NewFailpoint[error](key1)
			tc.configure(fp)
			registry.Set(fp)
			s.Equal(tc.want, failpoint.CheckError(registry, key1))
		})
	}
}

func (s *executeSuite) TestExecuteSourceWithoutRegistry() {
	tests := []struct {
		name string
		src  failpoint.Source
	}{
		{name: "nil source", src: nil},
		{name: "nil registry", src: (*failpoint.Registry)(nil)},
	}
	for _, tc := range tests {
		s.Run(tc.name, func(s *executeSuite) {
			got, fired := failpoint.Execute[error](tc.src, key1)
			s.False(fired)
			s.NoError(got)
			s.NoError(failpoint.CheckError(tc.src, key1))
			s.False(failpoint.ExecuteSwitch(tc.src, key1))
		})
	}
}

func (s *executeSuite) TestExecuteSwitch() {
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
			fp := failpoint.NewFailpoint[int](intKey)
			tc.configure(fp)

			registry := failpoint.NewRegistry()
			registry.Set(fp)
			s.Equal(tc.want, failpoint.ExecuteSwitch(registry, intKey))
		})
	}

	s.Run("not registered", func(s *executeSuite) {
		s.False(failpoint.ExecuteSwitch(failpoint.NewRegistry(), intKey))
	})
}

// Not in executeSuite because testing.AllocsPerRun cannot run in parallel with other tests.
func TestExecuteInertDoesNotAllocate(t *testing.T) {
	key := key1.ForScope("ns")
	for _, tc := range []struct {
		name string
		src  failpoint.Source
	}{
		{name: "nil source", src: nil},
		{name: "nil registry", src: (*failpoint.Registry)(nil)},
		{name: "empty registry", src: failpoint.NewRegistry()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			allocs := testing.AllocsPerRun(100, func() {
				_ = failpoint.CheckError(tc.src, key)
				_, _ = failpoint.Execute(tc.src, key)
			})
			require.Zero(t, allocs)
		})
	}
}
