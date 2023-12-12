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

	"google.golang.org/grpc"

	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/namespace"
)

type (
	CallerInfoInterceptor struct {
		namespaceRegistry namespace.Registry
	}
)

var _ grpc.UnaryServerInterceptor = (*CallerInfoInterceptor)(nil).Intercept

func NewCallerInfoInterceptor(
	namespaceRegistry namespace.Registry,
) *CallerInfoInterceptor {
	return &CallerInfoInterceptor{
		namespaceRegistry: namespaceRegistry,
	}
}

func (i *CallerInfoInterceptor) Intercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	callerInfo := headers.GetCallerInfo(ctx)

	updateInfo := false
	if callerInfo.CallerName == "" {
		callerInfo.CallerName = string(MustGetNamespaceName(i.namespaceRegistry, req))
		updateInfo = callerInfo.CallerName != ""
	}
	if callerInfo.CallerType == "" {
		callerInfo.CallerType = headers.CallerTypeAPI
		updateInfo = true
	}
	if (callerInfo.CallerType == headers.CallerTypeAPI || callerInfo.CallerType == headers.CallerTypeOperator) &&
		callerInfo.CallOrigin == "" {
		method := api.MethodName(info.FullMethod)
		callerInfo.CallOrigin = method
		updateInfo = true
	}

	if updateInfo {
		ctx = headers.SetCallerInfo(ctx, callerInfo)
	}

	return handler(ctx, req)
}
