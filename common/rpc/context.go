package rpc

import (
	"context"
	"time"

	"go.temporal.io/server/common/headers"
)

type (
	valueCopyCtx struct {
		context.Context

		valueCtx context.Context
	}
)

func (c *valueCopyCtx) Value(key any) any {
	if value := c.Context.Value(key); value != nil {
		return value
	}

	return c.valueCtx.Value(key)
}

// CopyContextValues copies values in source Context to destination Context.
func CopyContextValues(dst context.Context, src context.Context) context.Context {
	return &valueCopyCtx{
		Context:  dst,
		valueCtx: src,
	}
}

// ResetContextTimeout creates new context with specified timeout and copies values from source Context.
func ResetContextTimeout(ctx context.Context, newTimeout time.Duration) (context.Context, context.CancelFunc) {
	newContext, cancel := context.WithTimeout(context.Background(), newTimeout)
	return CopyContextValues(newContext, ctx), cancel
}

// NewContextWithTimeoutAndVersionHeaders creates context with timeout and version headers.
func NewContextWithTimeoutAndVersionHeaders(timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(headers.SetVersions(context.Background()), timeout)
}

// NewContextFromParentWithTimeoutAndVersionHeaders creates context from parent context with timeout and version headers.
func NewContextFromParentWithTimeoutAndVersionHeaders(parentCtx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(headers.SetVersions(parentCtx), timeout)
}
