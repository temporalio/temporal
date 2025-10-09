package log

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/log/tag"
	"go.uber.org/zap"
)

func TestDropFn(t *testing.T) {
	dropCalled := false
	throttledLogger := NewThrottledLoggerWithNotify(NewZapLogger(zap.NewExample()), func() float64 { return 0 },
		func(msg string, tags ...tag.Tag) {
			dropCalled = true
		})
	throttledLogger.Info("Should be dropped!")
	assert.True(t, dropCalled)
}
