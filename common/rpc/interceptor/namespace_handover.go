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
	"time"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/grpc"
)

const (
	ctxTailRoom = time.Millisecond * 100
)

var _ grpc.UnaryServerInterceptor = (*NamespaceHandoverInterceptor)(nil).Intercept

type (
	// NamespaceHandoverInterceptor handles the namespace in handover replication state
	NamespaceHandoverInterceptor struct {
		namespaceRegistry      namespace.Registry
		timeSource             clock.TimeSource
		enabledForNS           dynamicconfig.BoolPropertyFnWithNamespaceFilter
		nsCacheRefreshInterval dynamicconfig.DurationPropertyFn
		metricsHandler         metrics.Handler
		logger                 log.Logger
	}
)

func NewNamespaceHandoverInterceptor(
	dc *dynamicconfig.Collection,
	namespaceRegistry namespace.Registry,
	metricsHandler metrics.Handler,
	logger log.Logger,
	timeSource clock.TimeSource,
) *NamespaceHandoverInterceptor {

	return &NamespaceHandoverInterceptor{
		enabledForNS:           dynamicconfig.EnableNamespaceHandoverWait.Get(dc),
		nsCacheRefreshInterval: dynamicconfig.NamespaceCacheRefreshInterval.Get(dc),
		namespaceRegistry:      namespaceRegistry,
		metricsHandler:         metricsHandler,
		logger:                 logger,
		timeSource:             timeSource,
	}
}

func (i *NamespaceHandoverInterceptor) Intercept(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (_ any, retError error) {
	defer log.CapturePanic(i.logger, &retError)

	if !strings.HasPrefix(info.FullMethod, api.WorkflowServicePrefix) {
		return handler(ctx, req)
	}

	// review which method is allowed
	methodName := api.MethodName(info.FullMethod)
	namespaceName := MustGetNamespaceName(i.namespaceRegistry, req)

	if namespaceName != namespace.EmptyName && i.enabledForNS(namespaceName.String()) {
		var waitTime *time.Duration
		defer func() {
			if waitTime != nil {
				metrics.HandoverWaitLatency.With(i.metricsHandler).Record(*waitTime)
			}
		}()
		waitTime, err := i.waitNamespaceHandoverUpdate(ctx, namespaceName, methodName)
		if err != nil {
			return nil, err
		}
	}

	return handler(ctx, req)
}

func (i *NamespaceHandoverInterceptor) waitNamespaceHandoverUpdate(
	ctx context.Context,
	namespaceName namespace.Name,
	methodName string,
) (waitTime *time.Duration, retErr error) {
	if _, ok := allowedMethodsDuringHandover[methodName]; ok {
		return nil, nil
	}

	startTime := i.timeSource.Now()
	namespaceData, err := i.namespaceRegistry.GetNamespace(namespaceName)
	if err != nil {
		return nil, err
	}
	if namespaceData.ReplicationState() == enumspb.REPLICATION_STATE_HANDOVER {
		cbID := uuid.New()
		waitReplicationStateUpdate := make(chan struct{})
		i.namespaceRegistry.RegisterStateChangeCallback(cbID, func(ns *namespace.Namespace, deletedFromDb bool) {
			if ns.ID().String() != namespaceData.ID().String() {
				return
			}
			if ns.State() != enumspb.NAMESPACE_STATE_REGISTERED ||
				deletedFromDb ||
				ns.ReplicationState() != enumspb.REPLICATION_STATE_HANDOVER ||
				!ns.IsGlobalNamespace() {
				// Stop wait on state change if:
				// 1. namespace is deleting/deleted
				// 2. namespace is not in handover
				// 3. namespace is not global
				select {
				case <-waitReplicationStateUpdate:
				default:
					close(waitReplicationStateUpdate)
				}
			}
		})

		maxWaitDuration := i.nsCacheRefreshInterval() // cache refresh time
		if deadline, ok := ctx.Deadline(); ok {
			maxWaitDuration = max(0, time.Until(deadline)-ctxTailRoom)
		}
		returnTimer := time.NewTimer(maxWaitDuration)
		var handoverErr error
		select {
		case <-returnTimer.C:
			handoverErr = common.ErrNamespaceHandover
		case <-waitReplicationStateUpdate:
			returnTimer.Stop()
		}
		i.namespaceRegistry.UnregisterStateChangeCallback(cbID)
		waitTime := time.Since(startTime)
		return &waitTime, handoverErr
	}
	return nil, nil
}
