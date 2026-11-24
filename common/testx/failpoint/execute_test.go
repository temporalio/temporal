//go:build test_dep

package failpoint_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/failpoint"
)

type registrySource struct {
	registry failpoint.Registry
}

func (s registrySource) Failpoints() failpoint.Registry { return s.registry }

func TestKey(t *testing.T) {
	t.Parallel()

	scoped := key1.ForNamespace("ns-1")
	require.Equal(t, "1", key1.Name())
	require.Empty(t, key1.NamespaceID())
	require.Equal(t, "1", key1.String())
	require.Equal(t, "1", scoped.Name())
	require.Equal(t, "ns-1", scoped.NamespaceID())
	require.Equal(t, "1@ns-1", scoped.String())
	require.NotEqual(t, key1, scoped)
}

func TestBoundaryKeys(t *testing.T) {
	t.Parallel()

	require.Equal(t, "grpc:/test.Service/Method", failpoint.GRPCKey("/test.Service/Method").Name())
	require.Equal(t, "http:POST /callback", failpoint.HTTPKey("POST /callback").Name())
	require.Equal(t, "persistence:ExecutionStore.UpdateWorkflowExecution", failpoint.PersistenceKey("ExecutionStore", "UpdateWorkflowExecution").Name())
}

func TestNewKey_PanicsOnDuplicateName(t *testing.T) {
	t.Parallel()

	require.Panics(t, func() { failpoint.NewKey("1") })
}

func TestExecute(t *testing.T) {
	t.Parallel()

	fromUnscoped := errors.New("unscoped")
	fromScoped := errors.New("scoped")
	newSource := func(keys map[failpoint.Key]error) failpoint.Source {
		registry := failpoint.NewRegistry()
		for key, err := range keys {
			fp := failpoint.NewFailpoint[error](key)
			fp.Return(err, failpoint.Always)
			registry.Set(key, fp)
		}
		return registrySource{registry: registry}
	}

	tests := []struct {
		name       string
		registered map[failpoint.Key]error
		key        failpoint.Key
		want       error
		wantFired  bool
	}{
		{name: "not registered", registered: nil, key: key1, want: nil, wantFired: false},
		{name: "unscoped", registered: map[failpoint.Key]error{key1: fromUnscoped}, key: key1, want: fromUnscoped, wantFired: true},
		{
			name:       "scoped",
			registered: map[failpoint.Key]error{key1.ForNamespace("ns-1"): fromScoped},
			key:        key1.ForNamespace("ns-1"),
			want:       fromScoped,
			wantFired:  true,
		},
		{
			name:       "scoped key falls back to unscoped",
			registered: map[failpoint.Key]error{key1: fromUnscoped},
			key:        key1.ForNamespace("ns-1"),
			want:       fromUnscoped,
			wantFired:  true,
		},
		{
			name:       "scoped takes precedence over unscoped",
			registered: map[failpoint.Key]error{key1: fromUnscoped, key1.ForNamespace("ns-1"): fromScoped},
			key:        key1.ForNamespace("ns-1"),
			want:       fromScoped,
			wantFired:  true,
		},
		{
			name:       "other namespace does not fire",
			registered: map[failpoint.Key]error{key1.ForNamespace("ns-1"): fromScoped},
			key:        key1.ForNamespace("ns-2"),
			want:       nil,
			wantFired:  false,
		},
		{
			name:       "unscoped key does not see scoped failpoints",
			registered: map[failpoint.Key]error{key1.ForNamespace("ns-1"): fromScoped},
			key:        key1,
			want:       nil,
			wantFired:  false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, fired := failpoint.Execute[error](newSource(tc.registered), tc.key)
			require.Equal(t, tc.want, got)
			require.Equal(t, tc.wantFired, fired)
		})
	}
}

func TestExecute_AcceptsFailpointOfAnotherType(t *testing.T) {
	t.Parallel()

	registry := failpoint.NewRegistry()
	registry.Set(key1, failpoint.NewFailpoint[int](key1))
	_, fired := failpoint.Execute[error](registrySource{registry: registry}, key1)
	require.False(t, fired)
}

func TestExecute_PanicsOnNonFailpoint(t *testing.T) {
	t.Parallel()

	registry := failpoint.NewRegistry()
	registry.Set(key1, "not a failpoint")
	require.Panics(t, func() { _, _ = failpoint.Execute[error](registrySource{registry: registry}, key1) })
}

func TestCheckpoint(t *testing.T) {
	t.Parallel()

	injected := errors.New("injected")
	tests := []struct {
		name      string
		configure func(fp *failpoint.Failpoint[error])
		want      error
	}{
		{name: "not configured", configure: func(*failpoint.Failpoint[error]) {}, want: nil},
		{name: "return error", configure: func(fp *failpoint.Failpoint[error]) { fp.Return(injected, failpoint.Always) }, want: injected},
		{name: "return nil", configure: func(fp *failpoint.Failpoint[error]) { fp.Return(nil, failpoint.Always) }, want: nil},
		{name: "call returning nil", configure: func(fp *failpoint.Failpoint[error]) {
			fp.Call(func() error { return nil }, failpoint.Always)
		}, want: nil},
		{name: "call returning error", configure: func(fp *failpoint.Failpoint[error]) {
			fp.Call(func() error { return injected }, failpoint.Always)
		}, want: injected},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			registry := failpoint.NewRegistry()
			fp := failpoint.NewFailpoint[error](key1)
			tc.configure(fp)
			registry.Set(key1, fp)
			require.Equal(t, tc.want, failpoint.Checkpoint(registrySource{registry: registry}, key1))
		})
	}
}
