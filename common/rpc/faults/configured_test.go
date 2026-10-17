package faults

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
)

func configuredOutcome(name string) *Outcome[string] {
	return &Outcome[string]{Response: name}
}

func TestConfiguredGeneratorSampling(t *testing.T) {
	t.Parallel()
	cfg := &config.CallFaultInjection{Request: config.FaultInjectionMethodConfig{
		Errors: map[string]float64{"Unavailable": 0.22, "Internal": 0.01, "ResourceExhausted": 0.11},
		Seed:   2208,
	}}
	generator, err := configuredTestGenerator[any](cfg, configuredOutcome, nil)
	require.NoError(t, err)
	for _, expected := range []*Outcome[string]{nil, {Response: "Unavailable"}, {Response: "Unavailable"}, nil} {
		require.Equal(t, expected, generator.GenerateRequest(t.Context(), "any operation", nil))
	}
}

func TestConfiguredGeneratorDisabledAndInvalid(t *testing.T) {
	t.Parallel()
	for _, cfg := range []*config.CallFaultInjection{nil, {}} {
		g, err := configuredTestGenerator[any](cfg, configuredOutcome, nil)
		require.NoError(t, err)
		require.Nil(t, g)
		fallback := NewCallbackGenerator[any, string]()
		g, err = configuredTestGenerator(cfg, configuredOutcome, Generator[any, string](fallback))
		require.NoError(t, err)
		require.Same(t, fallback, g)
	}
	g, err := configuredTestGenerator[any](&config.CallFaultInjection{
		Request: config.FaultInjectionMethodConfig{Errors: map[string]float64{"InvalidArgument": 1}},
	}, configuredOutcome, nil)
	require.Error(t, err)
	require.Nil(t, g)
	g, err = configuredTestGenerator[any](&config.CallFaultInjection{
		Request: config.FaultInjectionMethodConfig{Errors: map[string]float64{"Unavailable": 0}},
	}, configuredOutcome, nil)
	require.NoError(t, err)
	require.Nil(t, g)
}

func TestConfiguredGeneratorResponseAndFallback(t *testing.T) {
	t.Parallel()
	fallback := NewCallbackGenerator[any, string]()
	cfg := &config.CallFaultInjection{
		Request:  config.FaultInjectionMethodConfig{Errors: map[string]float64{"Internal": 1}},
		Response: config.FaultInjectionMethodConfig{Errors: map[string]float64{"Unavailable": 1}},
	}
	g, err := configuredTestGenerator(cfg, configuredOutcome, Generator[any, string](fallback))
	require.NoError(t, err)
	ctx := t.Context()
	require.Equal(t, "Internal", g.GenerateRequest(ctx, "operation", nil).Response)
	require.Equal(t, "Unavailable", g.GenerateResponse(ctx, "operation", nil, "success", nil).Response)
	require.Nil(t, g.GenerateResponse(ctx, "operation", nil, "", errors.New("original")))

	unregister := fallback.RegisterRequestCallback(Scope{}, func(context.Context, string, any) *Outcome[string] {
		return &Outcome[string]{Response: "runtime request"}
	})
	require.Equal(t, "runtime request", g.GenerateRequest(ctx, "operation", nil).Response)
	unregister()
	require.Equal(t, "Internal", g.GenerateRequest(ctx, "operation", nil).Response)
	fallback.RegisterResponseCallback(Scope{}, func(context.Context, string, any, string, error) *Outcome[string] {
		return &Outcome[string]{Response: "runtime response"}
	})
	require.Equal(t, "runtime response", g.GenerateResponse(ctx, "operation", nil, "success", nil).Response)
}

func configuredTestGenerator[Req, Resp any](cfg *config.CallFaultInjection, outcome func(string) *Outcome[Resp], fallback Generator[Req, Resp]) (Generator[Req, Resp], error) {
	generators, err := NewConfiguredGenerators(&config.TransportFaultInjection{Inbound: cfg}, outcome, Generators[Req, Resp]{Inbound: fallback}, func(_ Resp, err error) bool { return err == nil })
	return generators.Inbound, err
}

func TestConfiguredDirections(t *testing.T) {
	t.Parallel()
	fallback := NewCallbackGenerator[any, string]()
	generators, err := NewConfiguredGenerators[any, string](nil, configuredOutcome, Generators[any, string]{Inbound: fallback}, func(_ string, err error) bool { return err == nil })
	require.NoError(t, err)
	require.Same(t, fallback, generators.Inbound)
	require.Nil(t, generators.Outbound)
	cfg := &config.TransportFaultInjection{
		Inbound:  &config.CallFaultInjection{Request: config.FaultInjectionMethodConfig{Errors: map[string]float64{"Unavailable": 1}}},
		Outbound: &config.CallFaultInjection{Response: config.FaultInjectionMethodConfig{Errors: map[string]float64{"Internal": 1}}},
	}
	generators, err = NewConfiguredGenerators[any, string](cfg, configuredOutcome, Generators[any, string]{}, func(_ string, err error) bool { return err == nil })
	require.NoError(t, err)
	require.Equal(t, &Outcome[string]{Response: "Unavailable"}, generators.Inbound.GenerateRequest(t.Context(), "operation", nil))
	require.Nil(t, generators.Outbound.GenerateRequest(t.Context(), "operation", nil))
	require.Nil(t, generators.Inbound.GenerateResponse(t.Context(), "operation", nil, "success", nil))
	require.Equal(t, &Outcome[string]{Response: "Internal"}, generators.Outbound.GenerateResponse(t.Context(), "operation", nil, "success", nil))
	cfg.Outbound.Response.Errors["Unavailable"] = 1
	_, err = NewConfiguredGenerators[any, string](cfg, configuredOutcome, Generators[any, string]{}, func(_ string, err error) bool { return err == nil })
	require.ErrorContains(t, err, "outbound: response")
}
