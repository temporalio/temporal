package tests

import (
	"context"
	"time"

	"go.temporal.io/server/common/testing/debugtimeout"
)

var (
	executionTimeout  = 2 * time.Second * debugtimeout.Multiplier
	visibilityTimeout = 4 * time.Second * debugtimeout.Multiplier
)

func newExecutionContext() context.Context {
	ctx := context.Background()
	ctx, _ = context.WithTimeout(ctx, executionTimeout)
	return ctx
}

func newVisibilityContext() context.Context {
	ctx := context.Background()
	ctx, _ = context.WithTimeout(ctx, visibilityTimeout)
	return ctx
}
