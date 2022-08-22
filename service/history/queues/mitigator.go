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
	// Mitigator generates an Action for resolving the given Alert
	Mitigator interface {
		Alert(Alert) Action
	}

	mitigatorImpl struct {
		sync.Mutex

		monitor        Monitor
		logger         log.Logger
		metricsHandler metrics.MetricsHandler
		maxReaderCount dynamicconfig.IntPropertyFn

		pendingAlerts map[AlertType]Alert
	}
)

func newMitigator(
	monitor Monitor,
	logger log.Logger,
	metricsHandler metrics.MetricsHandler,
	maxReaderCount dynamicconfig.IntPropertyFn,
) *mitigatorImpl {
	return &mitigatorImpl{
		monitor:        monitor,
		logger:         logger,
		metricsHandler: metricsHandler,
		maxReaderCount: maxReaderCount,
		pendingAlerts:  make(map[AlertType]Alert),
	}
}

func (m *mitigatorImpl) Alert(alert Alert) Action {
	m.Lock()
	defer m.Unlock()

	if _, ok := m.pendingAlerts[alert.AlertType]; ok {
		return nil
	}

	var action Action
	switch alert.AlertType {
	case AlertTypeReaderStuck:
		action = newReaderStuckAction(m, alert.AlertAttributesReaderStuck, m.logger)
	default:
		m.logger.Error("Unknown queue alert type", tag.QueueAlertType(alert.AlertType.String()))
		return nil
	}

	m.pendingAlerts[alert.AlertType] = alert
	return action
}

func (m *mitigatorImpl) resolve(alertType AlertType) {
	m.Lock()
	defer m.Unlock()

	delete(m.pendingAlerts, alertType)
	m.logger.Info("Action completed for queue alert", tag.QueueAlertType(alertType.String()))
}
