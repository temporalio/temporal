package failpoint_test

import (
	"errors"
	"testing"
	"time"

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
			got, fired := failpoint.Execute[error](newRegistry(tc.registered), tc.key)
			s.Equal(tc.want, got)
			s.Equal(tc.wantFired, fired)
		})
	}
}

func (s *executeSuite) TestExecuteInOrder() {
	errFirst := errors.New("first")
	errSecond := errors.New("second")
	errUnscoped := errors.New("unscoped")
	fired := func(err error) failpoint.Result[error] {
		return failpoint.Result[error]{Value: err, Set: true, Fired: true}
	}

	s.Run("nil registry", func(s *executeSuite) {
		s.Equal(failpoint.Result[error]{}, failpoint.ExecuteInOrder((*failpoint.Registry)(nil), failpoint.ExecuteOptions{}, key1))
	})

	s.Run("runs keys in order and passes argument", func(s *executeSuite) {
		registry := failpoint.NewRegistry()
		registry.Set(failpoint.NewFailpoint(key2))
		var args []any
		callback := failpoint.NewFailpoint(key3)
		callback.CallWithArg(func(arg any) error {
			args = append(args, arg)
			return nil
		})
		registry.Set(callback)
		fp := failpoint.NewFailpoint(key4)
		fp.Return(errSecond)
		registry.Set(fp)

		// A failpoint that fires without an error ends the execution.
		got := failpoint.ExecuteInOrder(registry, failpoint.ExecuteOptions{Arg: "arg"}, key1, key2, key3, key4)
		s.Equal(failpoint.Result[error]{Set: true, Fired: true}, got)
		s.Equal([]any{"arg"}, args)
	})

	s.Run("stops at first key that fired", func(s *executeSuite) {
		registry := failpoint.NewRegistry()
		first := failpoint.NewFailpoint(key2.ForScope("ns"))
		first.Return(errFirst)
		registry.Set(first)
		second := failpoint.NewFailpoint(key3)
		second.Return(errSecond)
		registry.Set(second)

		s.Equal(fired(errFirst), failpoint.ExecuteInOrder(registry, failpoint.ExecuteOptions{}, key1, key2.ForScope("ns"), key3))
		s.Equal(fired(errSecond), failpoint.ExecuteInOrder(registry, failpoint.ExecuteOptions{}, key1, key2.ForScope("other"), key3.ForScope("ns")))
	})

	s.Run("scope precedence within same key name", func(s *executeSuite) {
		registry := failpoint.NewRegistry()
		unscoped := failpoint.NewFailpoint(key1)
		unscoped.Return(errUnscoped)
		registry.Set(unscoped)

		keys := []failpoint.Key[error]{key1.ForScope("id"), key1.ForScope("name")}
		s.Equal(fired(errUnscoped), failpoint.ExecuteInOrder(registry, failpoint.ExecuteOptions{}, keys...))

		byName := failpoint.NewFailpoint(key1.ForScope("name"))
		byName.Return(errSecond)
		registry.Set(byName)
		s.Equal(fired(errSecond), failpoint.ExecuteInOrder(registry, failpoint.ExecuteOptions{}, keys...))

		byID := failpoint.NewFailpoint(key1.ForScope("id"))
		byID.Return(errFirst)
		registry.Set(byID)
		s.Equal(fired(errFirst), failpoint.ExecuteInOrder(registry, failpoint.ExecuteOptions{}, keys...))

		// A disabled failpoint at a higher-precedence scope shadows lower scopes of the same key.
		byID.Disable()
		s.Equal(failpoint.Result[error]{}, failpoint.ExecuteInOrder(registry, failpoint.ExecuteOptions{}, keys...))
	})

	s.Run("reports pause and block as fired without value", func(s *executeSuite) {
		registry := failpoint.NewRegistry()
		fp := failpoint.NewFailpoint(key1)
		fp.Pause(time.Nanosecond)
		registry.Set(fp)
		s.Equal(failpoint.Result[error]{Fired: true}, failpoint.ExecuteInOrder(registry, failpoint.ExecuteOptions{}, key1))

		fp.Block()
		got := make(chan failpoint.Result[error], 1)
		go func() {
			got <- failpoint.ExecuteInOrder(registry, failpoint.ExecuteOptions{}, key1)
		}()
		s.NoError(fp.Wait(s.Context()))
		fp.Disable()
		s.Equal(failpoint.Result[error]{Fired: true}, s.Rcv(got))
	})

	s.Run("ignores failpoint of another type", func(s *executeSuite) {
		registry := failpoint.NewRegistry()
		fp := failpoint.NewFailpoint(failpoint.UndeclaredKey[int]("other.type.order"))
		fp.Return(1)
		registry.Set(fp)

		got := failpoint.ExecuteInOrder(registry, failpoint.ExecuteOptions{}, failpoint.UndeclaredKey[error]("other.type.order"))
		s.Equal(failpoint.Result[error]{}, got)
	})
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
		{name: "call returning nil", configure: func(fp *failpoint.Failpoint[error]) {
			fp.Call(func() error { return nil })
		}, want: nil},
		{name: "call returning error", configure: func(fp *failpoint.Failpoint[error]) {
			fp.Call(func() error { return injected })
		}, want: injected},
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

func (s *executeSuite) TestExecuteNilRegistry() {
	got, fired := failpoint.Execute[error](nil, key1)
	s.False(fired)
	s.NoError(got)
	s.NoError(failpoint.CheckError(nil, key1))
	s.False(failpoint.ExecuteSwitch(nil, key1))
}

func (s *executeSuite) TestExecuteSwitch() {
	tests := []struct {
		name      string
		configure func(*failpoint.Failpoint[int])
		want      bool
	}{
		{name: "on", configure: func(fp *failpoint.Failpoint[int]) { fp.On() }, want: true},
		{name: "return", configure: func(fp *failpoint.Failpoint[int]) { fp.Return(7) }, want: true},
		{name: "pause", configure: func(fp *failpoint.Failpoint[int]) { fp.Pause(time.Nanosecond) }, want: false},
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
		name     string
		registry *failpoint.Registry
	}{
		{name: "nil registry", registry: nil},
		{name: "empty registry", registry: failpoint.NewRegistry()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			allocs := testing.AllocsPerRun(100, func() {
				_ = failpoint.CheckError(tc.registry, key)
				_, _ = failpoint.Execute(tc.registry, key)
			})
			require.Zero(t, allocs)
		})
	}
}
