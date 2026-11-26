package serverbuggify

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/failpoint/buggify"
)

func TestReadStatic(t *testing.T) {
	t.Parallel()
	zero, one, half, negative, nan := 0.0, 1.0, 0.5, -0.1, math.NaN()
	key := func(match string, activation, fire *float64) config.FailpointsBuggifyKey {
		return config.FailpointsBuggifyKey{Match: match, ActivationProbability: activation, FireProbability: fire}
	}

	tests := []struct {
		name    string
		block   config.FailpointsBuggify
		want    StaticConfig
		wantErr string
	}{
		{name: "empty", want: StaticConfig{}},
		{name: "trace without a seed", block: config.FailpointsBuggify{Trace: true}, want: StaticConfig{Trace: true}},
		{
			name: "seed with keys and trace",
			block: config.FailpointsBuggify{
				Seed: 42,
				Keys: []config.FailpointsBuggifyKey{
					{Match: "grpc:/grpc.health.v1.Health/*", ActivationProbability: &zero, FireProbability: &zero},
					{Match: "grpc:*", ActivationProbability: &half, FireProbability: &one, Error: "Unavailable"},
				},
				Trace: true,
			},
			want: StaticConfig{
				Config: buggify.Config{Seed: 42, Keys: []buggify.Key{
					{Match: "grpc:/grpc.health.v1.Health/*"},
					{Match: "grpc:*", ActivationProbability: 0.5, FireProbability: 1, Error: "Unavailable"},
				}},
				Trace: true,
			},
		},
		{
			name:  "keys without a seed leave BUGGIFY off",
			block: config.FailpointsBuggify{Keys: []config.FailpointsBuggifyKey{key("a", &half, &half)}},
			want:  StaticConfig{},
		},
		{name: "a seed without keys", block: config.FailpointsBuggify{Seed: 1}, wantErr: "a seed needs keys"},
		{
			name:    "an empty match",
			block:   config.FailpointsBuggify{Seed: 1, Keys: []config.FailpointsBuggifyKey{key("", &half, &half)}},
			wantErr: "match must not be empty",
		},
		{
			name:    "a duplicate match",
			block:   config.FailpointsBuggify{Seed: 1, Keys: []config.FailpointsBuggifyKey{key("a*", &half, &half), key("a*", &one, &one)}},
			wantErr: `duplicate buggify key match "a*"`,
		},
		{
			name:    "a missing activation probability",
			block:   config.FailpointsBuggify{Seed: 1, Keys: []config.FailpointsBuggifyKey{key("a", nil, &half)}},
			wantErr: `failpoints.buggify.keys[0] (match "a"): activationProbability is required`,
		},
		{
			name:    "a missing fire probability",
			block:   config.FailpointsBuggify{Seed: 1, Keys: []config.FailpointsBuggifyKey{key("a", &half, nil)}},
			wantErr: "fireProbability is required",
		},
		{
			name:    "a negative probability",
			block:   config.FailpointsBuggify{Seed: 1, Keys: []config.FailpointsBuggifyKey{key("a", &negative, &half)}},
			wantErr: `probability -0.1 for key match "a"`,
		},
		{
			name:    "a NaN probability without a seed",
			block:   config.FailpointsBuggify{Keys: []config.FailpointsBuggifyKey{key("a", &half, &nan)}},
			wantErr: "probability NaN",
		},
		{
			name: "an unregistered error",
			block: config.FailpointsBuggify{Seed: 1, Keys: []config.FailpointsBuggifyKey{
				{Match: "a", ActivationProbability: &half, FireProbability: &half, Error: "Oops"},
			}},
			wantErr: `unknown error "Oops"`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			static, err := ReadStatic(tc.block)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, static)
		})
	}
}

// BUGGIFY applies only to the keys that an entry matches: a block without an entry for the
// persistence, response and health keys never fails them, however high the other probabilities are.
func TestReadStatic_OnlyListedKeysAreBuggified(t *testing.T) {
	t.Parallel()
	zero, one := 0.0, 1.0
	static, err := ReadStatic(config.FailpointsBuggify{Seed: 1, Keys: []config.FailpointsBuggifyKey{
		{Match: "grpc:/grpc.health.v1.Health/*", ActivationProbability: &zero, FireProbability: &zero},
		{Match: "grpc:*", ActivationProbability: &one, FireProbability: &one, Error: "Unavailable"},
		{Match: "http:*", ActivationProbability: &one, FireProbability: &one, Error: "Unavailable"},
	}})
	require.NoError(t, err)

	for name, want := range map[string]bool{
		"grpc:/temporal.server.api.historyservice.v1.HistoryService/StartWorkflowExecution": true,
		"http:POST":                         true,
		"grpc:/grpc.health.v1.Health/Check": false, // the narrow entry comes first
		"grpc-response:/temporal.server.api.historyservice.v1.HistoryService/StartWorkflowExecution": false,
		"http-response:POST": false,
		"persistence:ExecutionStore.UpdateWorkflowExecution": false,
		"matching.skipSyncMatch":                             false, // not listed
	} {
		require.Equal(t, want, static.Config.Activated(name), name)
	}
}

func TestTraceLine(t *testing.T) {
	t.Parallel()
	line, ok := TraceLine(buggify.Event{Kind: buggify.EventActivation, Name: "http:POST /cb", Activated: true})
	require.True(t, ok)
	require.Equal(t, `BUGGIFY event=activation key="http:POST /cb" active=true`, line)

	line, ok = TraceLine(buggify.Event{Kind: buggify.EventActivation, Name: "matching.skipSyncMatch"})
	require.True(t, ok)
	require.Equal(t, `BUGGIFY event=activation key="matching.skipSyncMatch" active=false`, line)

	line, ok = TraceLine(buggify.Event{Kind: buggify.EventFire, Name: "grpc:/a.B/C", Scope: "ns", Action: failpoint.ActionReturn, ExecutionNumber: 3})
	require.True(t, ok)
	require.Equal(t, `BUGGIFY event=fire key="grpc:/a.B/C" scope="ns" execution=3 action=Return`, line)

	// Quoting keeps a line parseable when a name or scope has quotes or spaces.
	line, ok = TraceLine(buggify.Event{Kind: buggify.EventFire, Name: `http:GET /a "b"`, Scope: "name:a b", Action: failpoint.ActionOn, ExecutionNumber: 1})
	require.True(t, ok)
	require.Equal(t, `BUGGIFY event=fire key="http:GET /a \"b\"" scope="name:a b" execution=1 action=On`, line)

	_, ok = TraceLine(buggify.Event{Kind: buggify.EventScopeLimit})
	require.False(t, ok)
}
