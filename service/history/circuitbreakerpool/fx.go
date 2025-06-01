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
