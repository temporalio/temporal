package queues

import (
	"sync"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

var _ Mitigator = (*mitigatorImpl)(nil)

type (
	// Mitigator generates and runs an Action for resolving the given Alert
	Mitigator interface {
		Mitigate(Alert)
	}

	actionRunner func(Action, *ReaderGroup, metrics.Handler)

	mitigatorImpl struct {
		sync.Mutex

		readerGroup    *ReaderGroup
		monitor        Monitor
		logger         log.Logger
		metricsHandler metrics.Handler
		maxReaderCount dynamicconfig.IntPropertyFn

		// this is for overriding the behavior in unit tests
		// since we don't really want to run the action in Mitigator unit tests
		actionRunner actionRunner
		grouper      Grouper
	}
)

func newMitigator(
	readerGroup *ReaderGroup,
	monitor Monitor,
	logger log.Logger,
	metricsHandler metrics.Handler,
	maxReaderCount dynamicconfig.IntPropertyFn,
	grouper Grouper,
) *mitigatorImpl {
	return &mitigatorImpl{
		readerGroup:    readerGroup,
		monitor:        monitor,
		logger:         logger,
		metricsHandler: metricsHandler,
		maxReaderCount: maxReaderCount,

		actionRunner: runAction,
		grouper:      grouper,
	}
}

func (m *mitigatorImpl) Mitigate(alert Alert) {
	m.Lock()
	defer m.Unlock()

	var action Action
	switch alert.AlertType {
	case AlertTypeQueuePendingTaskCount:
		action = newQueuePendingTaskAction(
			alert.AlertAttributesQueuePendingTaskCount,
			m.monitor,
			m.maxReaderCount(),
			m.grouper,
		)
	case AlertTypeReaderStuck:
		action = newReaderStuckAction(
			alert.AlertAttributesReaderStuck,
			m.logger,
		)
	case AlertTypeSliceCount:
		action = newSliceCountAction(
			alert.AlertAttributesSliceCount,
			m.monitor,
		)
	default:
		m.logger.Error("Unknown queue alert type", tag.QueueAlert(alert))
		return
	}

	m.actionRunner(
		action,
		m.readerGroup,
		m.metricsHandler,
	)

	m.monitor.ResolveAlert(alert.AlertType)
}

func runAction(
	action Action,
	readerGroup *ReaderGroup,
	metricsHandler metrics.Handler,
) {
	if action.Run(readerGroup) {
		return
	}

	metricsHandler = metricsHandler.WithTags(metrics.QueueActionTag(action.Name()))
	metrics.QueueActionCounter.With(metricsHandler).Record(1)
}
