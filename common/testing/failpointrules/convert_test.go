package failpointrules

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/failpointadapter"
	_ "go.temporal.io/server/common/testing/failpointerrors"
	"go.temporal.io/server/common/testing/serverfailpoints"
	"go.temporal.io/testx/failpoint"
)

// grpcKey is the key of a gRPC method that the rules of the tests name.
var grpcKey = serverfailpoints.GRPCKey(historyservice.HistoryService_StartWorkflowExecution_FullMethodName).Name()

// The keys that the rules of the tests name.
var (
	onKey    = failpoint.NewKey[bool]("on")
	_        = failpoint.NewKey[error]("pause")
	blockKey = failpoint.NewKey[error]("block")
	_        = failpoint.NewKey[error]("panic")
	rKey     = failpoint.NewKey[error]("r")
	_        = failpoint.NewKey[error]("p")
)

func TestConvertRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   any
		want    []failpoint.Rule
		wantErr string
	}{
		{name: "unset", value: nil, want: []failpoint.Rule{}},
		{
			name: "all fields",
			value: []any{
				map[string]any{
					"key":     grpcKey,
					"scope":   "ns-id",
					"action":  "return",
					"error":   "InvalidArgument",
					"message": "injected",
					"trigger": map[string]any{"probability": 0.5, "seed": 7, "skipFirst": 1, "maxFires": 2},
				},
				map[string]any{"key": "pause", "action": "Pause", "delay": "1.5s"},
				map[string]any{"key": "p", "action": "Pause", "delay": 5},
				map[string]any{"key": "on", "action": "On", "trigger": map[string]any{"maxFires": 1}},
				map[string]any{"key": "block", "action": "Block"},
				map[string]any{"key": "panic", "action": "Panic", "error": "Internal"},
			},
			want: []failpoint.Rule{
				{
					Key:     grpcKey,
					Scope:   "ns-id",
					Action:  failpoint.ActionReturn,
					Error:   "InvalidArgument",
					Message: "injected",
					Trigger: failpoint.Trigger{Probability: 0.5, Seed: 7, SkipFirst: 1, MaxFires: 2},
				},
				{Key: "pause", Action: failpoint.ActionPause, Delay: 1500 * time.Millisecond},
				{Key: "p", Action: failpoint.ActionPause, Delay: 5 * time.Second},
				{Key: "on", Action: failpoint.ActionOn, Trigger: failpoint.Trigger{MaxFires: 1}},
				{Key: "block", Action: failpoint.ActionBlock},
				{Key: "panic", Action: failpoint.ActionPanic, Error: "Internal"},
			},
		},
		{
			name:  "typed rules",
			value: []failpoint.Rule{{Key: "on", Action: failpoint.ActionOn}},
			want:  []failpoint.Rule{{Key: "on", Action: failpoint.ActionOn}},
		},
		{name: "not a list", value: map[string]any{"key": "on"}, wantErr: "source data must be an array or slice"},
		{name: "unknown field", value: []any{map[string]any{"key": "on", "action": "On", "actoin": "On"}}, wantErr: "invalid keys: actoin"},
		{name: "unknown trigger field", value: []any{map[string]any{"key": "on", "action": "On", "trigger": map[string]any{"chance": 1}}}, wantErr: "invalid keys: chance"},
		{
			name:  "string keys of any-keyed maps",
			value: []any{map[any]any{"key": "on", "action": "On", "trigger": map[any]any{"maxfires": 2.0}}},
			want:  []failpoint.Rule{{Key: "on", Action: failpoint.ActionOn, Trigger: failpoint.Trigger{MaxFires: 2}}},
		},
		{name: "non-string rule key", value: []any{map[any]any{"key": "on", "action": "On", 1: 2}}, wantErr: "key 1 is not a string"},
		{name: "non-string trigger key", value: []any{map[string]any{"key": "on", "action": "On", "trigger": map[any]any{1: 2}}}, wantErr: "key 1 is not a string"},
		{name: "fractional count", value: []any{map[string]any{"key": "on", "action": "On", "trigger": map[string]any{"maxfires": 0.5}}}, wantErr: "0.5 is not an integer"},
		{name: "count out of range", value: []any{map[string]any{"key": "on", "action": "On", "trigger": map[string]any{"skipfirst": 1e300}}}, wantErr: "is not an integer"},
		{name: "string count", value: []any{map[string]any{"key": "on", "action": "On", "trigger": map[string]any{"maxfires": "2"}}}, wantErr: "expected type 'int64'"},
		{
			name:  "namespace name",
			value: []any{map[string]any{"key": grpcKey, "namespace": "my-ns", "action": "On"}},
			want:  []failpoint.Rule{{Key: grpcKey, Scope: failpointadapter.NameScope("my-ns"), Action: failpoint.ActionOn}},
		},
		{
			name:  "namespace name of a rule for every operation",
			value: []any{map[string]any{"key": serverfailpoints.AnyHTTPKey.Name(), "namespace": "my-ns", "action": "On"}},
			want:  []failpoint.Rule{{Key: serverfailpoints.AnyHTTPKey.Name(), Scope: failpointadapter.NameScope("my-ns"), Action: failpoint.ActionOn}},
		},
		{
			name:    "empty namespace name",
			value:   []any{map[string]any{"key": grpcKey, "namespace": "", "action": "On"}},
			wantErr: "rule 0: namespace is empty",
		},
		{
			name:    "null namespace name",
			value:   []any{map[string]any{"key": grpcKey, "action": "On"}, map[string]any{"key": grpcKey, "Namespace": nil, "action": "On"}},
			wantErr: "rule 1: namespace is empty",
		},
		{
			name:    "empty namespace name with a scope",
			value:   []any{map[any]any{"key": grpcKey, "scope": "ns-id", "namespace": "", "action": "On"}},
			wantErr: "rule 0: namespace is empty",
		},
		{
			name:    "scope and namespace name",
			value:   []any{map[string]any{"key": grpcKey, "scope": "ns-id", "namespace": "my-ns", "action": "On"}},
			wantErr: "has a scope and a namespace",
		},
		{
			name:    "namespace name of a key that is not a transport key",
			value:   []any{map[string]any{"key": "on", "namespace": "my-ns", "action": "On"}},
			wantErr: "rule 0: rule for on has a namespace, which only applies to gRPC and HTTP failpoints",
		},
		{name: "missing action", value: []any{map[string]any{"key": "on"}}, wantErr: `rule 0: unknown action "", valid actions are Block, On, Panic, Pause, Return`},
		{name: "unknown action", value: []any{map[string]any{"key": "on", "action": "Off"}}, wantErr: `rule 0: unknown action "Off", valid actions are Block, On, Panic, Pause, Return`},
		{name: "recurrence is not a field", value: []any{map[string]any{"key": "on", "action": "On", "recurrence": "Once"}}, wantErr: "invalid keys: recurrence"},
		{name: "enabled is not a field", value: []any{map[string]any{"key": "on", "action": "On", "enabled": false}}, wantErr: "invalid keys: enabled"},
		{name: "malformed delay", value: []any{map[string]any{"key": "p", "action": "Pause", "delay": "soon"}}, wantErr: "soon"},
		{name: "unknown error", value: []any{map[string]any{"key": "r", "action": "Return", "error": "Oops"}}, wantErr: `unknown error "Oops"`},
		{name: "invalid trigger", value: []any{map[string]any{"key": "on", "action": "On", "trigger": map[string]any{"probability": 2}}}, wantErr: "probability 2 is not in [0, 1]"},
		{name: "invalid typed rule", value: []failpoint.Rule{{Key: "on"}}, wantErr: "unsupported action"},
		{name: "unknown key", value: []any{map[string]any{"key": "of", "action": "On"}}, wantErr: `unknown key "of", declared keys are `},
		{name: "unknown gRPC method", value: []any{map[string]any{"key": "grpc:/Method", "action": "On"}}, wantErr: `"/Method" is not the full name of a unary method of a gRPC service`},
		{name: "unknown gRPC response method", value: []any{map[string]any{"key": "grpc-response:/temporal.server.api.historyservice.v1.HistoryService/Oops", "action": "On"}}, wantErr: "is not the full name of a unary method of a gRPC service"},
		{
			name:    "gRPC method without the leading slash",
			value:   []any{map[string]any{"key": "grpc:temporal.server.api.historyservice.v1.HistoryService/StartWorkflowExecution", "action": "On"}},
			wantErr: "is not the full name of a unary method of a gRPC service",
		},
		{
			name:    "streaming gRPC method",
			value:   []any{map[string]any{"key": "grpc:" + historyservice.HistoryService_StreamWorkflowReplicationMessages_FullMethodName, "action": "On"}},
			wantErr: "is not the full name of a unary method of a gRPC service",
		},
		{
			name:    "streaming gRPC response method",
			value:   []any{map[string]any{"key": "grpc-response:" + historyservice.HistoryService_StreamWorkflowReplicationMessages_FullMethodName, "action": "On"}},
			wantErr: "is not the full name of a unary method of a gRPC service",
		},
		{name: "unknown gRPC method of a typed rule", value: []failpoint.Rule{{Key: "grpc:/Method", Action: failpoint.ActionOn}}, wantErr: "is not the full name of a unary method of a gRPC service"},
		{
			name: "duplicate key",
			value: []any{
				map[string]any{"key": "on", "action": "On"},
				map[string]any{"key": "on", "action": "Block"},
			},
			wantErr: "duplicate failpoint rule for on",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rules, err := ConvertRules(tc.value)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, rules)
		})
	}
}

func TestConvertRules_NamespaceNameFiresForTheRequestsOfTheNamespace(t *testing.T) {
	t.Parallel()

	rules, err := ConvertRules([]any{map[string]any{
		"key": grpcKey, "namespace": "my-ns", "action": "Return", "error": "InvalidArgument",
	}})
	require.NoError(t, err)
	registry := failpoint.NewRegistry()
	_, err = failpoint.NewRuleSet(registry).Apply(rules)
	require.NoError(t, err)

	adapter := failpointadapter.NewAdapter[any, any](registry, failpointadapter.Keys{
		Request:     serverfailpoints.GRPCKey,
		Response:    serverfailpoints.GRPCResponseKey,
		AnyRequest:  serverfailpoints.AnyGRPCKey,
		AnyResponse: serverfailpoints.AnyGRPCResponseKey,
	}, failpointadapter.Namespaces{
		ID: func(name namespace.Name) (namespace.ID, bool) { return namespace.ID("id-of-" + name.String()), true },
		Name: func(id namespace.ID) (namespace.Name, bool) {
			_, name, _ := strings.Cut(id.String(), "id-of-")
			return namespace.Name(name), true
		},
	})
	operation := strings.TrimPrefix(grpcKey, "grpc:")
	generate := func(req any) error {
		_, err := adapter.GenerateRequest(t.Context(), operation, req)
		return err
	}
	// The same rule matches a request that carries the name and one that carries the ID, also of
	// a namespace that is recreated with another ID, and not another namespace.
	byName := &workflowservice.StartWorkflowExecutionRequest{Namespace: "my-ns"}
	require.Error(t, generate(byName))
	require.Error(t, generate(&historyservice.StartWorkflowExecutionRequest{NamespaceId: "id-of-my-ns"}))
	require.Error(t, generate(&historyservice.StartWorkflowExecutionRequest{NamespaceId: "recreated-id-of-my-ns"}))
	require.NoError(t, generate(&workflowservice.StartWorkflowExecutionRequest{Namespace: "other"}))
	require.NoError(t, generate(&historyservice.StartWorkflowExecutionRequest{NamespaceId: "id-of-other"}))
}

