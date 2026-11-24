package failpointrules

import (
	"crypto/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	_ "go.temporal.io/server/common/testing/failpointerrors"
	"go.temporal.io/testx/failpoint"
)

func TestConvertRules(t *testing.T) {
	t.Parallel()
	// The behavior catalog is global, so each run registers a name of its own.
	behavior := "TestConvertRules_" + rand.Text()
	failpoint.RegisterBehavior(behavior, func() error { return nil })

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
					"key":        "grpc:/Method",
					"scope":      "ns-id",
					"action":     "Return",
					"error":      "InvalidArgument",
					"message":    "injected",
					"recurrence": "Once",
					"trigger":    map[string]any{"probability": 0.5, "seed": 7, "after": 1, "max": 2},
					"enabled":    false,
				},
				map[string]any{"key": "pause", "action": "Pause", "delay": "1.5s"},
				map[string]any{"key": "call", "action": "Call", "behavior": behavior, "recurrence": "Always"},
				map[string]any{"key": "on", "action": "On", "enabled": true},
				map[string]any{"key": "block", "action": "Block"},
				map[string]any{"key": "panic", "action": "Panic", "error": "Internal"},
			},
			want: []failpoint.Rule{
				{
					Key:        "grpc:/Method",
					Scope:      "ns-id",
					Action:     failpoint.Return,
					Error:      "InvalidArgument",
					Message:    "injected",
					Recurrence: failpoint.Once,
					Trigger:    failpoint.Trigger{Probability: 0.5, Seed: 7, After: 1, Max: 2},
					Disabled:   true,
				},
				{Key: "pause", Action: failpoint.Pause, Delay: 1500 * time.Millisecond},
				{Key: "call", Action: failpoint.Call, Behavior: behavior, Recurrence: failpoint.Always},
				{Key: "on", Action: failpoint.On},
				{Key: "block", Action: failpoint.Block},
				{Key: "panic", Action: failpoint.Panic, Error: "Internal"},
			},
		},
		{
			name:  "typed rules",
			value: []failpoint.Rule{{Key: "on", Action: failpoint.On}},
			want:  []failpoint.Rule{{Key: "on", Action: failpoint.On}},
		},
		{name: "not a list", value: map[string]any{"key": "on"}, wantErr: "source data must be an array or slice"},
		{name: "unknown field", value: []any{map[string]any{"key": "on", "action": "On", "actoin": "On"}}, wantErr: "invalid keys: actoin"},
		{name: "unknown trigger field", value: []any{map[string]any{"key": "on", "action": "On", "trigger": map[string]any{"chance": 1}}}, wantErr: "invalid keys: chance"},
		{
			name:  "string keys of any-keyed maps",
			value: []any{map[any]any{"key": "on", "action": "On", "trigger": map[any]any{"max": 2.0}}},
			want:  []failpoint.Rule{{Key: "on", Action: failpoint.On, Trigger: failpoint.Trigger{Max: 2}}},
		},
		{name: "non-string rule key", value: []any{map[any]any{"key": "on", "action": "On", 1: 2}}, wantErr: "key 1 is not a string"},
		{name: "non-string trigger key", value: []any{map[string]any{"key": "on", "action": "On", "trigger": map[any]any{1: 2}}}, wantErr: "key 1 is not a string"},
		{name: "fractional count", value: []any{map[string]any{"key": "on", "action": "On", "trigger": map[string]any{"max": 0.5}}}, wantErr: "0.5 is not an integer"},
		{name: "count out of range", value: []any{map[string]any{"key": "on", "action": "On", "trigger": map[string]any{"after": 1e300}}}, wantErr: "is not an integer"},
		{name: "string count", value: []any{map[string]any{"key": "on", "action": "On", "trigger": map[string]any{"max": "2"}}}, wantErr: "expected type 'int64'"},
		{name: "missing action", value: []any{map[string]any{"key": "on"}}, wantErr: `rule 0: unknown action ""`},
		{name: "unknown action", value: []any{map[string]any{"key": "on", "action": "Off"}}, wantErr: `rule 0: unknown action "Off"`},
		{name: "unknown recurrence", value: []any{map[string]any{"key": "on", "action": "On", "recurrence": "Twice"}}, wantErr: `unknown recurrence "Twice"`},
		{name: "numeric delay", value: []any{map[string]any{"key": "p", "action": "Pause", "delay": 5}}, wantErr: "delay must be a duration string"},
		{name: "malformed delay", value: []any{map[string]any{"key": "p", "action": "Pause", "delay": "soon"}}, wantErr: "soon"},
		{name: "unknown error", value: []any{map[string]any{"key": "r", "action": "Return", "error": "Oops"}}, wantErr: `unknown error "Oops"`},
		{name: "unknown behavior", value: []any{map[string]any{"key": "c", "action": "Call", "behavior": "Oops"}}, wantErr: `unknown behavior "Oops"`},
		{name: "invalid trigger", value: []any{map[string]any{"key": "on", "action": "On", "trigger": map[string]any{"probability": 2}}}, wantErr: "probability 2 is not in [0, 1]"},
		{name: "invalid typed rule", value: []failpoint.Rule{{Key: "on"}}, wantErr: "unsupported action"},
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
