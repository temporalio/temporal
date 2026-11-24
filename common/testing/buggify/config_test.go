package buggify

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/testx/failpoint"
)

func TestConvertValue(t *testing.T) {
	t.Parallel()
	fire := 0.5
	errorName := "Internal"
	include := []string{"grpc:*"}
	exclude := []string{}

	tests := []struct {
		name    string
		value   any
		want    Value
		wantErr string
	}{
		{name: "unset", value: nil, want: Value{}},
		{name: "typed", value: Value{Seed: 3}, want: Value{Seed: 3}},
		{
			name: "all fields",
			value: map[string]any{
				"seed":     42,
				"disabled": true,
				"fire":     0.5,
				"error":    "Internal",
				"prefixes": []any{map[string]any{"prefix": "grpc:", "activation": 1, "fire": 0.5, "error": "Internal"}},
				"include":  []any{"grpc:*"},
				"exclude":  []any{},
			},
			want: Value{
				Seed:     42,
				Disabled: true,
				Fire:     &fire,
				Error:    &errorName,
				Prefixes: []failpoint.BuggifyPrefix{{Prefix: "grpc:", Activation: 1, Fire: 0.5, Error: "Internal"}},
				Include:  &include,
				Exclude:  &exclude,
			},
		},
		{name: "float seed", value: map[string]any{"seed": 7.0}, want: Value{Seed: 7}},
		{name: "any-keyed map", value: map[any]any{"seed": 7}, want: Value{Seed: 7}},
		{name: "unknown field", value: map[string]any{"sead": 1}, wantErr: "invalid keys: sead"},
		{name: "unknown prefix field", value: map[string]any{"prefixes": []any{map[string]any{"prefix": "a", "chance": 1}}}, wantErr: "invalid keys: chance"},
		{name: "negative seed", value: map[string]any{"seed": -1}, wantErr: "-1 is not an unsigned integer"},
		{name: "fractional seed", value: map[string]any{"seed": 1.5}, wantErr: "1.5 is not an unsigned integer"},
		{name: "not a map", value: []any{1}, wantErr: "expected a map"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			value, err := ConvertValue(tc.value)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, value)
		})
	}
}

func env(vars map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		v, ok := vars[name]
		return v, ok
	}
}

const seedEnv = failpoint.BuggifySeedEnv

func TestResolve(t *testing.T) {
	t.Parallel()

	withSeed := func(seed uint64) failpoint.BuggifyConfig {
		config := Defaults()
		config.Seed = seed
		return config
	}
	activation := 0.5
	invalidProbability := 2.0
	exclude := []string{"history.*"}

	tests := []struct {
		name       string
		value      Value
		env        map[string]string
		want       failpoint.BuggifyConfig
		wantSource string
		wantErr    string
	}{
		{name: "no seed", want: failpoint.BuggifyConfig{}},
		{name: "setting seed", value: Value{Seed: 5}, env: map[string]string{seedEnv: "6"}, want: withSeed(5), wantSource: "dynamic config testing.buggify"},
		{name: "env seed", env: map[string]string{seedEnv: "6"}, want: withSeed(6), wantSource: "FAILPOINT_BUGGIFY_SEED=6"},
		{
			name: "env probabilities",
			env:  map[string]string{seedEnv: "6", failpoint.BuggifyActivationEnv: "0.5", failpoint.BuggifyFireEnv: "0.1"},
			want: func() failpoint.BuggifyConfig {
				config := withSeed(6)
				config.Activation, config.Fire = 0.5, 0.1
				return config
			}(),
			wantSource: "FAILPOINT_BUGGIFY_SEED=6",
		},
		{name: "gomad seed alone", env: map[string]string{failpoint.GomadSeedEnv: "7"}, want: failpoint.BuggifyConfig{}},
		{name: "disabled", value: Value{Seed: 5, Disabled: true}, env: map[string]string{seedEnv: "6"}, want: failpoint.BuggifyConfig{}},
		{name: "invalid env seed", env: map[string]string{seedEnv: "x"}, wantErr: `invalid FAILPOINT_BUGGIFY_SEED "x"`},
		{
			name: "overrides",
			value: Value{Seed: 5, Activation: &activation, Exclude: &exclude, Prefixes: []failpoint.BuggifyPrefix{
				{Prefix: "grpc:", Activation: 1, Fire: 1, Error: "Internal"},
				{Prefix: "matching.", Activation: 0},
			}},
			env: map[string]string{seedEnv: "6", failpoint.BuggifyActivationEnv: "0.9"},
			want: failpoint.BuggifyConfig{
				Seed:       5,
				Activation: 0.5,
				Fire:       0.25,
				Prefixes: []failpoint.BuggifyPrefix{
					{Prefix: "grpc:", Activation: 1, Fire: 1, Error: "Internal"},
					{Prefix: "http:", Activation: 0.1, Fire: 0.01, Error: "Unavailable"},
					{Prefix: "matching.", Activation: 0},
				},
				Exclude: []string{"history.*"},
			},
			wantSource: "dynamic config testing.buggify",
		},
		{name: "invalid override", value: Value{Seed: 5, Prefixes: []failpoint.BuggifyPrefix{{Prefix: "grpc:", Error: "Oops"}}}, wantErr: `unknown error "Oops"`},
		{name: "invalid but disabled", value: Value{Disabled: true, Fire: &invalidProbability}, wantErr: "probability 2"},
		{name: "invalid without a seed", value: Value{Prefixes: []failpoint.BuggifyPrefix{{Prefix: "grpc:", Error: "Oops"}}}, wantErr: `unknown error "Oops"`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e, err := ReadEnv(env(tc.env))
			if err == nil {
				var config failpoint.BuggifyConfig
				var source string
				config, source, err = Resolve(tc.value, e)
				if err == nil {
					require.Equal(t, tc.want, config)
					require.Equal(t, tc.wantSource, source)
				}
			}
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}

	t.Run("a random seed is picked once", func(t *testing.T) {
		t.Parallel()
		e, err := ReadEnv(env(map[string]string{seedEnv: "random"}))
		require.NoError(t, err)
		require.Equal(t, "FAILPOINT_BUGGIFY_SEED=random", e.Source)
		first, _, err := Resolve(Value{}, e)
		require.NoError(t, err)
		again, _, err := Resolve(Value{Exclude: &exclude}, e)
		require.NoError(t, err)
		require.Equal(t, first.Seed, again.Seed)
	})
}

func TestDefaults(t *testing.T) {
	t.Parallel()
	config := Defaults()
	require.NoError(t, config.Validate())
	config.Seed = 1
	// Activate everything, so the test only depends on which names the defaults select.
	for i := range config.Prefixes {
		config.Prefixes[i].Activation = 1
	}
	config.Activation = 1

	for name, want := range map[string]bool{
		"matching.skipSyncMatch": true,
		"grpc:/temporal.server.api.historyservice.v1.HistoryService/StartWorkflowExecution": true,
		"http:POST /callback": true,
		"grpc:/temporal.testx.failpoint.v1.FailpointService/InjectReturn":                            false,
		"grpc:/grpc.health.v1.Health/Check":                                                          false,
		"grpc-response:/temporal.server.api.historyservice.v1.HistoryService/StartWorkflowExecution": false,
		"http-response:POST /callback":                                                               false,
		"persistence:ExecutionStore.UpdateWorkflowExecution":                                         false,
		KnobPrefix + "history.transferTaskBatchSize":                                                 true,
	} {
		require.Equal(t, want, config.Activated(name), name)
	}
}

func TestGomadLine(t *testing.T) {
	t.Parallel()
	line, ok := GomadLine(failpoint.Event{Kind: failpoint.BuggifyActivationEvent, Key: failpoint.KeyFromName("http:POST /cb", ""), Activated: true})
	require.True(t, ok)
	require.Equal(t, `GOMAD failpoint buggify-activation key="http:POST /cb" active=true`, line)

	line, ok = GomadLine(failpoint.Event{Kind: failpoint.BuggifyFireEvent, Key: failpoint.KeyFromName("grpc:/a.B/C", "ns"), Action: failpoint.Return, Execution: 3})
	require.True(t, ok)
	require.Equal(t, `GOMAD failpoint hit key="grpc:/a.B/C" scope="ns" n=3 action=Return buggify=true`, line)

	_, ok = GomadLine(failpoint.Event{Key: failpoint.KeyFromName("a", "")})
	require.False(t, ok)
}

func TestInjectsInboundGRPCErrors(t *testing.T) {
	t.Parallel()
	require.False(t, InjectsInboundGRPCErrors(primitives.FrontendService))
	for _, service := range []primitives.ServiceName{
		primitives.InternalFrontendService, primitives.HistoryService, primitives.MatchingService, primitives.WorkerService,
	} {
		require.True(t, InjectsInboundGRPCErrors(service), service)
	}
}
