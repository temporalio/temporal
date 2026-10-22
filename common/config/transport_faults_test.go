package config

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestCallFaultInjectionValidate(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		errors  map[string]float64
		invalid bool
	}{
		{name: "empty"},
		{name: "valid", errors: map[string]float64{"Unavailable": 0.2, "Internal": 0.3, "ResourceExhausted": 0.5}},
		{name: "zero", errors: map[string]float64{"Unavailable": 0}},
		{name: "permanent error", errors: map[string]float64{"InvalidArgument": 0.1}, invalid: true},
		{name: "non-retryable deadline", errors: map[string]float64{"DeadlineExceeded": 0.1}, invalid: true},
		{name: "negative", errors: map[string]float64{"Unavailable": -0.1}, invalid: true},
		{name: "greater than one", errors: map[string]float64{"Unavailable": 1.1}, invalid: true},
		{name: "sum greater than one", errors: map[string]float64{"Unavailable": 0.6, "Internal": 0.6}, invalid: true},
		{name: "nan", errors: map[string]float64{"Unavailable": math.NaN()}, invalid: true},
		{name: "infinity", errors: map[string]float64{"Unavailable": math.Inf(1)}, invalid: true},
		{name: "negative infinity", errors: map[string]float64{"Unavailable": math.Inf(-1)}, invalid: true},
	} {
		for _, stage := range []string{"request", "response"} {
			t.Run(tc.name+"/"+stage, func(t *testing.T) {
				t.Parallel()
				cfg := &CallFaultInjection{}
				if stage == "request" {
					cfg.Request.Errors = tc.errors
				} else {
					cfg.Response.Errors = tc.errors
				}
				if tc.invalid {
					require.ErrorContains(t, cfg.Validate(), stage)
				} else {
					require.NoError(t, cfg.Validate())
				}
			})
		}
	}
	var cfg *CallFaultInjection
	require.NoError(t, cfg.Validate())
}

func TestRPCFaultInjectionYAML(t *testing.T) {
	t.Parallel()
	var cfg Config
	require.NoError(t, yaml.Unmarshal([]byte(`
faultInjection:
  grpc:
    inbound:
      request:
        errors:
          Unavailable: 0.1
        seed: 123
  http:
    outbound:
      response:
        errors:
          ResourceExhausted: 0.2
        seed: 456
`), &cfg))
	require.NoError(t, cfg.FaultInjection.Validate())
	require.InDelta(t, 0.1, cfg.FaultInjection.GRPC.Inbound.Request.Errors["Unavailable"], 1e-10)
	require.Equal(t, int64(123), cfg.FaultInjection.GRPC.Inbound.Request.Seed)
	require.InDelta(t, 0.2, cfg.FaultInjection.HTTP.Outbound.Response.Errors["ResourceExhausted"], 1e-10)
	require.Equal(t, int64(456), cfg.FaultInjection.HTTP.Outbound.Response.Seed)

	cfg.FaultInjection.HTTP.Outbound.Response.Errors["InvalidArgument"] = 0.1
	require.ErrorContains(t, cfg.Validate(), "faultInjection.http: outbound: response")
}
