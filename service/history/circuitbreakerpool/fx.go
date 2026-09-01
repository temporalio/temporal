package circuitbreakerpool

import (
	"fmt"

	"github.com/sony/gobreaker"
	chasmcallback "go.temporal.io/server/chasm/lib/callback"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/circuitbreaker"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/configs"
	hsmcallbacks "go.temporal.io/server/service/history/hsm/callbacks"
	hsmnexus "go.temporal.io/server/service/history/hsm/nexusoperations"
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
	logger log.SnTaggedLogger,
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
					OnStateChange: onStateChange(key, nsName.String(), logger),
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

// onStateChange logs breaker transitions.
func onStateChange(
	key tasks.TaskGroupNamespaceIDAndDestination,
	nsName string,
	logger log.Logger,
) func(name string, from gobreaker.State, to gobreaker.State) {
	logger = log.With(
		logger,
		tag.ComponentOutboundQueue,
		tag.WorkflowNamespace(nsName),
		tag.WorkflowNamespaceID(key.NamespaceID),
		tag.Destination(key.Destination),
		tag.NewStringTag("task-group", key.TaskGroup),
	)

	switch key.TaskGroup {
	case chasmnexus.TaskGroupName, hsmnexus.TaskTypeInvocation, hsmnexus.TaskTypeCancelation:
		logger = log.With(logger, tag.NexusStageCallerOutbound)
	case hsmcallbacks.TaskTypeInvocation, chasmcallback.InvocationTaskGroup:
		logger = log.With(logger, tag.NexusStageHandlerOutbound)
	default:
	}

	return func(_ string, from gobreaker.State, to gobreaker.State) {
		logger.Warn(
			"outbound queue circuit breaker state change",
			tag.NewStringTag("from-state", from.String()),
			tag.NewStringTag("to-state", to.String()),
		)
	}
}
