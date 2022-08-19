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
	Mitigator interface {
		Alert(Alert) bool
	}

	mitigatorImpl struct {
		sync.Mutex

		monitor        Monitor
		logger         log.Logger
		metricsHandler metrics.MetricsHandler
		maxReaderCount dynamicconfig.IntPropertyFn

		pendingAlerts map[AlertType]Alert
		actionCh      chan<- action
	}
)

func newMitigator(
	monitor Monitor,
	logger log.Logger,
	metricsHandler metrics.MetricsHandler,
	maxReaderCount dynamicconfig.IntPropertyFn,
) (*mitigatorImpl, <-chan action) {
	actionCh := make(chan action, 10)

	return &mitigatorImpl{
		monitor:        monitor,
		logger:         logger,
		metricsHandler: metricsHandler,
		maxReaderCount: maxReaderCount,
		pendingAlerts:  make(map[AlertType]Alert),
		actionCh:       actionCh,
	}, actionCh
}

func (m *mitigatorImpl) Alert(alert Alert) bool {
	m.Lock()
	defer m.Unlock()

	if _, ok := m.pendingAlerts[alert.AlertType]; ok {
		return true
	}

	var action action
	switch alert.AlertType {
	case AlertTypeReaderWatermark:
		action = newReaderWatermarkAction(m, alert.AlertReaderWatermarkAttributes)
	default:
		m.logger.Error("Unknown queue alert type", tag.Value(alert.AlertType))
		return false
	}

	select {
	case m.actionCh <- action:
		m.logger.Info("Received queue alert", tag.Key("alert"), tag.Value(alert.AlertType))
		m.pendingAlerts[alert.AlertType] = alert
		return true
	default:
		m.logger.Warn("Too many pending queue actions")
		return false
	}
}

func (m *mitigatorImpl) resolve(alertType AlertType) {
	m.Lock()
	defer m.Unlock()

	delete(m.pendingAlerts, alertType)
	m.logger.Info("Action completed for queue alert", tag.Key("alert-type"), tag.Value(alertType))
}

func (m *mitigatorImpl) close() {
	close(m.actionCh)
}
