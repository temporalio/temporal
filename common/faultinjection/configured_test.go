package faultinjection_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/faultinjection"
)

func TestInvoke(t *testing.T) {
	t.Parallel()
	injected := errors.New("injected")
	realError := errors.New("real error")
	for _, tc := range []struct {
		name         string
		request      *faultinjection.Outcome[string]
		response     *faultinjection.Outcome[string]
		operationErr error
		wantCalls    int
		wantResponse string
		wantError    error
	}{
		{name: "unchanged", wantCalls: 1, wantResponse: "real"},
		{name: "request", request: &faultinjection.Outcome[string]{Error: injected}, wantError: injected},
		{name: "request replacement", request: &faultinjection.Outcome[string]{Response: "synthetic"}, wantResponse: "synthetic"},
		{name: "response", response: &faultinjection.Outcome[string]{Error: injected}, wantCalls: 1, wantError: injected},
		{name: "execute then fail", request: &faultinjection.Outcome[string]{Error: injected, AfterSuccess: true}, wantCalls: 1, wantResponse: "real", wantError: injected},
		{name: "preserve real failure", request: &faultinjection.Outcome[string]{Error: injected, AfterSuccess: true}, operationErr: realError, wantCalls: 1, wantResponse: "real", wantError: realError},
		{name: "test hook can replace real failure", response: &faultinjection.Outcome[string]{Response: "replacement"}, operationErr: realError, wantCalls: 1, wantResponse: "replacement"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			calls := 0
			generator := faultinjection.Callbacks[string, string]{
				Request: func(context.Context, string, string) *faultinjection.Outcome[string] { return tc.request },
				Response: func(context.Context, string, string, string, error) *faultinjection.Outcome[string] {
					return tc.response
				},
			}
			resp, err := faultinjection.Invoke(t.Context(), generator, "method", "request", func() (string, error) {
				calls++
				return "real", tc.operationErr
			})
			require.Equal(t, tc.wantCalls, calls)
			require.Equal(t, tc.wantResponse, resp)
			require.ErrorIs(t, err, tc.wantError)
		})
	}
}

func TestConfiguredGeneratorAndTestHookPrecedence(t *testing.T) {
	t.Parallel()
	cfg := faultinjection.Config{Errors: map[string]float64{"fault": 0.5}, Seed: 42}
	newGenerator := func() faultinjection.Generator[string, string] {
		generator, err := faultinjection.NewConfiguredGenerator[string, string](cfg, cfg, func(string, float64) *faultinjection.Outcome[string] {
			return &faultinjection.Outcome[string]{Response: "configured"}
		}, nil)
		require.NoError(t, err)
		return generator
	}
	configured := newGenerator()
	reference := newGenerator()
	hookEnabled := true
	testHookGenerator := faultinjection.Callbacks[string, string]{Request: func(context.Context, string, string) *faultinjection.Outcome[string] {
		if hookEnabled {
			return &faultinjection.Outcome[string]{Response: "hook"}
		}
		return nil
	}}
	combined := faultinjection.WithTestHooks(configured, testHookGenerator)
	require.Equal(t, "hook", combined.GenerateRequest(t.Context(), "method", "request").Response)
	hookEnabled = false
	for range 100 {
		// A matching hook must not consume a draw from the configured sequence.
		require.Equal(t, reference.GenerateRequest(t.Context(), "method", "request"), combined.GenerateRequest(t.Context(), "method", "request"))
	}
	require.Nil(t, configured.GenerateResponse(t.Context(), "method", "request", "", errors.New("real failure")))
	for range 100 {
		// Failed responses must not consume a response draw either.
		require.Equal(t, reference.GenerateResponse(t.Context(), "method", "request", "real", nil), configured.GenerateResponse(t.Context(), "method", "request", "real", nil))
	}
}

func TestDisabledGenerator(t *testing.T) {
	t.Parallel()
	for _, cfg := range []faultinjection.Config{{}, {Errors: map[string]float64{"fault": 0}}} {
		generator, err := faultinjection.NewConfiguredGenerator[string, string](cfg, cfg, nil, nil)
		require.NoError(t, err)
		require.Nil(t, generator)
	}
	require.Nil(t, faultinjection.WithTestHooks[string, string](nil, nil))
	resp, err := faultinjection.Invoke(t.Context(), nil, "method", "request", func() (string, error) { return "real", nil })
	require.NoError(t, err)
	require.Equal(t, "real", resp)
}
