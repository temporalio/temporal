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
	"fmt"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/quotas"
	"google.golang.org/grpc"
	"time"
)

const (
	ShardNamespaceRateLimitDefaultToken = 1
)

var (
	ErrShardNamespaceRateLimitServerBusy = serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT, "per-shard namespace rate limit exceeded")
)

type (
	ShardNamespaceRateLimitInterceptor struct {
		namespaceRegistry namespace.Registry
		rateLimiter       quotas.RequestRateLimiter
		tokens            map[string]int
		numShards         int32
	}
)

var _ grpc.UnaryServerInterceptor = (*ShardNamespaceRateLimitInterceptor)(nil).Intercept

func NewShardNamespaceRateLimitInterceptor(
	namespaceRegistry namespace.Registry,
	rateLimiter quotas.RequestRateLimiter,
	tokens map[string]int,
	numShards int32,
) *ShardNamespaceRateLimitInterceptor {
	return &ShardNamespaceRateLimitInterceptor{
		namespaceRegistry: namespaceRegistry,
		rateLimiter:       rateLimiter,
		tokens:            tokens,
		numShards:         numShards,
	}
}

func (i *ShardNamespaceRateLimitInterceptor) Intercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	_, methodName := SplitMethodName(info.FullMethod)
	token, ok := i.tokens[methodName]
	if !ok {
		token = NamespaceRateLimitDefaultToken
	}

	namespaceID := MustGetNamespaceID(i.namespaceRegistry, req)
	shardID := MustGetShardID(i.namespaceRegistry, req, i.numShards)

	if namespaceID == namespace.EmptyID || shardID == -1 {
		// skip rate limiting for requests that do not target a specific shard+namespace
		return handler(ctx, req)
	}

	if !i.rateLimiter.Allow(time.Now().UTC(), quotas.NewRequest(
		methodName,
		token,
		fmt.Sprintf("%s%d", namespaceID.String(), shardID),
		"", // this interceptor layer does not throttle based on caller type
		"", // this interceptor layer does not throttle based on call initiation
	)) {
		return nil, ErrShardNamespaceRateLimitServerBusy
	}

	return handler(ctx, req)
}
