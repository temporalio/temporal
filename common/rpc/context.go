// Copyright (c) 2020 Temporal Technologies, Inc.

package rpc

import (
	"context"
	"time"

	"github.com/temporalio/temporal/common/headers"
)

// NewContextWithTimeout creates context with timeout.
func NewContextWithTimeout(timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), timeout)
}

// NewContextWithTimeoutAndHeaders creates context with timeout and version headers.
func NewContextWithTimeoutAndHeaders(timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(headers.SetVersions(context.Background()), timeout)
}

// NewContextWithTimeoutAndHeaders creates context with timeout and version headers for CLI.
func NewContextWithTimeoutAndCLIHeaders(timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(headers.SetCLIVersions(context.Background()), timeout)
}
