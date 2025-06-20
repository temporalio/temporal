package tests

import (
	"context"
	"time"

	"go.temporal.io/server/common/debug"
)

const (
	executionTimeout  = 2 * time.Second * debug.TimeoutMultiplier
	visibilityTimeout = 4 * time.Second * debug.TimeoutMultiplier
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
