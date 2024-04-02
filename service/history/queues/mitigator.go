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

	actionRunner func(Action, *ReaderGroup, metrics.Handler, log.Logger)

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
		log.With(m.logger, tag.QueueAlert(alert)),
	)

	m.monitor.ResolveAlert(alert.AlertType)
}

func runAction(
	action Action,
	readerGroup *ReaderGroup,
	metricsHandler metrics.Handler,
	logger log.Logger,
) {
	metricsHandler = metricsHandler.WithTags(metrics.QueueActionTag(action.Name()))
	metrics.QueueActionCounter.With(metricsHandler).Record(1)

	action.Run(readerGroup)
}
