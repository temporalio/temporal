// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package circuitbreakerpool

import (
	"fmt"

	"go.temporal.io/server/common/circuitbreaker"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(OutboundQueueCircuitBreakerPoolProvider),
)

type OutboundQueueCircuitBreakerPool struct {
	*CircuitBreakerPool[tasks.TaskGroupNamespaceIDAndDestination]
}

func OutboundQueueCircuitBreakerPoolProvider(
	namespaceRegistry namespace.Registry,
	config *configs.Config,
) *OutboundQueueCircuitBreakerPool {
	return &OutboundQueueCircuitBreakerPool{
		CircuitBreakerPool: NewCircuitBreakerPool(
			func(key tasks.TaskGroupNamespaceIDAndDestination) circuitbreaker.TwoStepCircuitBreaker {
				// This is intentionally not failing the function in case of error. The circuit breaker is
				// agnostic to Task implementation, and thus the settings function is not expected to return
				// an error. Also, in this case, if the namespace registry fails to get the name, then the
				// task itself will fail when it is processed and tries to get the namespace name.
				nsName, _ := namespaceRegistry.GetNamespaceName(namespace.ID(key.NamespaceID))
				cb := circuitbreaker.NewTwoStepCircuitBreakerWithDynamicSettings(circuitbreaker.Settings{
					Name: fmt.Sprintf(
						"circuit_breaker:%s:%s:%s",
						key.TaskGroup,
						key.NamespaceID,
						key.Destination,
					),
				})
				initial, cancel := config.OutboundQueueCircuitBreakerSettings(
					nsName.String(),
					key.Destination,
					cb.UpdateSettings,
				)
				cb.UpdateSettings(initial)
				_ = cancel // OnceMap never deletes anything. use this if we support deletion
				return cb
			},
		),
	}
}
