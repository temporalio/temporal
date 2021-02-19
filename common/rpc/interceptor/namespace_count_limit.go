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
	"sync"
	"sync/atomic"

	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc"
)

var (
	NamespaceCountLimitServerBusy = serviceerror.NewResourceExhausted("namespace count limit exceeded")
)

type (
	NamespaceCountLimitInterceptor struct {
		countFn func(namespace string) int
		tokens  map[string]int

		sync.Mutex
		namespaceToCount map[string]*int32
	}
)

var _ grpc.UnaryServerInterceptor = (*NamespaceCountLimitInterceptor)(nil).Intercept

func NewNamespaceCountLimitInterceptor(
	countFn func(namespace string) int,
	tokens map[string]int,
) *NamespaceCountLimitInterceptor {
	return &NamespaceCountLimitInterceptor{
		countFn: countFn,
		tokens:  tokens,

		namespaceToCount: make(map[string]*int32),
	}
}

func (i *NamespaceCountLimitInterceptor) Intercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	_, methodName := splitMethodName(info.FullMethod)
	// token will default to 0
	token, _ := i.tokens[methodName]
	if token != 0 {
		namespace := GetNamespace(req)
		counter := i.counter(namespace)
		count := atomic.AddInt32(counter, int32(token))
		defer atomic.AddInt32(counter, -int32(token))

		if int(count) > i.countFn(namespace) {
			return nil, NamespaceCountLimitServerBusy
		}
	}

	return handler(ctx, req)
}

func (i *NamespaceCountLimitInterceptor) counter(
	namespace string,
) *int32 {
	i.Lock()
	defer i.Unlock()

	count, ok := i.namespaceToCount[namespace]
	if !ok {
		counter := int32(0)
		count = &counter
		i.namespaceToCount[namespace] = count
	}
	return count
}
