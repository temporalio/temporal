// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
