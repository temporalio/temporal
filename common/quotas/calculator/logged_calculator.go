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

package calculator

import (
	"sync"
	"sync/atomic"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

var (
	_ Calculator          = (*LoggedCalculator)(nil)
	_ NamespaceCalculator = (*LoggedNamespaceCalculator)(nil)
)

type (
	LoggedCalculator struct {
		calculator Calculator

		quotaLogger *quotaLogger[float64]
	}

	LoggedNamespaceCalculator struct {
		calculator NamespaceCalculator
		logger     log.Logger

		quotaLoggersLock sync.Mutex
		quotaLoggers     map[string]*quotaLogger[float64]
	}

	quotaLogger[T comparable] struct {
		logger       log.Logger
		currentValue atomic.Value
	}
)

func NewLoggedCalculator(
	calculator Calculator,
	logger log.Logger,
) *LoggedCalculator {
	return &LoggedCalculator{
		quotaLogger: newQuotaLogger(logger),
		calculator:  calculator,
	}
}

func (c *LoggedCalculator) GetQuota() float64 {
	quota := c.calculator.GetQuota()
	c.quotaLogger.updateQuota(quota)
	return quota
}

func NewLoggedNamespaceCalculator(
	calculator NamespaceCalculator,
	logger log.Logger,
) *LoggedNamespaceCalculator {
	return &LoggedNamespaceCalculator{
		calculator:   calculator,
		logger:       logger,
		quotaLoggers: make(map[string]*quotaLogger[float64]),
	}
}

func (c *LoggedNamespaceCalculator) GetQuota(namespace string) float64 {
	quota := c.calculator.GetQuota(namespace)
	c.getOrCreateQuotaLogger(namespace).updateQuota(quota)
	return quota
}

func (c *LoggedNamespaceCalculator) getOrCreateQuotaLogger(
	namespace string,
) *quotaLogger[float64] {
	c.quotaLoggersLock.Lock()
	defer c.quotaLoggersLock.Unlock()

	quotaLogger, ok := c.quotaLoggers[namespace]
	if !ok {
		quotaLogger = newQuotaLogger(log.With(c.logger, tag.WorkflowNamespace(namespace)))
		c.quotaLoggers[namespace] = quotaLogger
	}

	return quotaLogger
}

func newQuotaLogger(
	logger log.Logger,
) *quotaLogger[float64] {
	return &quotaLogger[float64]{
		logger: logger,
	}
}

func (l *quotaLogger[T]) updateQuota(newQuota T) {
	currentQuota := l.currentValue.Swap(newQuota)

	if currentQuota != nil && newQuota == currentQuota.(T) {
		return
	}

	l.logger.Info("Quota changed",
		tag.NewAnyTag("current-quota", currentQuota),
		tag.NewAnyTag("new-quota", newQuota),
	)
}
