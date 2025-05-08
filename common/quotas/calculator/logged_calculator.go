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
