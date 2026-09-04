package faultinjection

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/await"
)

func TestFaultRegistry_NoCallbacks(t *testing.T) {
	t.Parallel()

	reg := NewFaultRegistry()
	require.Nil(t, reg.generate(Target{Method: "M"}))
	require.NoError(t, reg.Inject(Target{Method: "M"}))
}

func TestFaultRegistry_FirstCallbackWins(t *testing.T) {
	t.Parallel()

	reg := NewFaultRegistry()
	errA := errors.New("a")
	errB := errors.New("b")
	reg.RegisterCallback(func(Target) error { return errA })
	reg.RegisterCallback(func(Target) error { return errB })

	require.ErrorIs(t, reg.Inject(Target{Method: "M"}), errA)
}

func TestFaultRegistry_NilErrorFallsThrough(t *testing.T) {
	t.Parallel()

	reg := NewFaultRegistry()
	errB := errors.New("b")
	reg.RegisterCallback(func(Target) error { return nil })
	reg.RegisterCallback(func(Target) error { return errB })

	require.ErrorIs(t, reg.Inject(Target{Method: "M"}), errB)
}

func TestFaultRegistry_CleanupRemovesCallback(t *testing.T) {
	t.Parallel()

	reg := NewFaultRegistry()
	errA := errors.New("a")
	cleanup := reg.RegisterCallback(func(Target) error { return errA })
	cleanup()
	cleanup()

	require.NoError(t, reg.Inject(Target{Method: "M"}))
}

func TestFaultRegistry_CleanupDuringInject(t *testing.T) {
	t.Parallel()

	reg := NewFaultRegistry()
	errA := errors.New("a")
	started := make(chan struct{})
	proceed := make(chan struct{})
	unregister := reg.RegisterCallback(func(Target) error {
		await.Snd(t, started, struct{}{})
		await.Rcv(t, proceed)
		return errA
	})
	result := make(chan error, 1)
	go func() {
		result <- reg.Inject(Target{Method: "M"})
	}()

	await.Rcv(t, started)
	unregister()
	await.Snd(t, proceed, struct{}{})
	require.ErrorIs(t, await.Rcv(t, result), errA)
	require.NoError(t, reg.Inject(Target{Method: "M"}))
}

func TestFaultRegistry_NilRegistry(t *testing.T) {
	t.Parallel()

	var reg *FaultRegistry
	require.Nil(t, reg.generate(Target{Method: "M"}))
	require.NoError(t, reg.Inject(Target{Method: "M"}))

	unregister := reg.RegisterCallback(func(Target) error { return context.Canceled })
	unregister()
}
