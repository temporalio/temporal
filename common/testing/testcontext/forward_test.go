package testcontext

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestNameMetadata(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		opts    []Option
		timeout time.Duration
	}{
		{name: "default", timeout: DefaultTimeout()},
		{name: "custom", opts: []Option{WithTimeout(time.Second)}, timeout: time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				start := time.Now()
				ctx := For(t, tc.opts...)
				deadline, ok := ctx.Deadline()
				require.True(t, ok)
				require.Equal(t, start.Add(tc.timeout), deadline)
				for _, ctx := range []context.Context{ctx, For(t)} {
					md, ok := metadata.FromOutgoingContext(ctx)
					require.True(t, ok)
					require.Equal(t, []string{t.Name()}, md.Get("temporal-test-name"))
				}
			})
		})
	}
}

func TestEnsureRemainingPreservesTestNameMetadata(t *testing.T) {
	t.Parallel()

	ctx := For(t)
	refreshed := EnsureRemaining(ctx, t, DefaultTimeout()+10*time.Second)

	require.NotSame(t, ctx, refreshed, "context should have been replaced")
	md, ok := metadata.FromOutgoingContext(refreshed)
	require.True(t, ok)
	require.Equal(t, []string{t.Name()}, md.Get("temporal-test-name"))
}

func TestDecoratorReceivesTestNameMetadata(t *testing.T) {
	t.Parallel()

	type key struct{}
	var names []string
	AttachDecorator(t, key{}, func(ctx context.Context) context.Context {
		md, _ := metadata.FromOutgoingContext(ctx)
		names = md.Get("temporal-test-name")
		return ctx
	})

	require.Equal(t, []string{t.Name()}, names)
}
