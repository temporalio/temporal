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

package interceptor

import (
	"context"
	"strings"
	"sync/atomic"

	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc"

	"go.temporal.io/server/common/api"
)

type (
	HealthInterceptor struct {
		healthy atomic.Bool
	}
)

var _ grpc.UnaryServerInterceptor = (*HealthInterceptor)(nil).Intercept

var notHealthyErr = serviceerror.NewUnavailable("Frontend is not healthy yet")

// NewHealthInterceptor returns a new HealthInterceptor. It starts with state not healthy.
func NewHealthInterceptor() *HealthInterceptor {
	return &HealthInterceptor{}
}

func (i *HealthInterceptor) Intercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	// only enforce health check on WorkflowService and OperatorService
	if strings.HasPrefix(info.FullMethod, api.WorkflowServicePrefix) ||
		strings.HasPrefix(info.FullMethod, api.OperatorServicePrefix) {
		if !i.healthy.Load() {
			return nil, notHealthyErr
		}
	}
	return handler(ctx, req)
}

func (i *HealthInterceptor) SetHealthy(healthy bool) {
	i.healthy.Store(healthy)
}
