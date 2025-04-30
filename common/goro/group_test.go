package goro_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/goro"
)

func TestMultiCancelAndWait(t *testing.T) {
	var g goro.Group
	g.Go(blockOnCtxReturnNil)
	g.Go(blockOnCtxReturnNil)
	g.Go(blockOnCtxReturnNil)
	g.Cancel()
	g.Wait()
}

func TestCancelBeforeGo(t *testing.T) {
	var g goro.Group
	g.Cancel()
	g.Go(func(ctx context.Context) error {
		require.ErrorIs(t, context.Canceled, ctx.Err())
		return nil
	})
	g.Wait()
}

func TestWaitOnNothing(t *testing.T) {
	var g goro.Group
	g.Wait() // nothing running, should return immediately
}

func TestWaitOnDifferentThread(t *testing.T) {
	var g goro.Group
	g.Go(blockOnCtxReturnNil)
	h := goro.NewHandle(context.TODO()).Go(func(context.Context) error {
		g.Wait()
		return nil
	})
	g.Cancel()
	<-h.Done()
}
