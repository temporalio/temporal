package calculator

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.uber.org/mock/gomock"
)

type (
	testCalculator struct {
		quota float64
	}

	testNamespaceCalculator struct {
		namespaceQuota map[string]float64
	}
)

func TestQuotaLogger(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)

	mockLogger := log.NewMockLogger(controller)

	quotaLogger := newQuotaLogger(mockLogger)

	mockLogger.EXPECT().Info(gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
	quotaLogger.updateQuota(1.0)

	quotaLogger.updateQuota(1.0)

	mockLogger.EXPECT().Info(gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
	quotaLogger.updateQuota(2.0)
}

func TestLoggedCalculator(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)

	mockLogger := log.NewMockLogger(controller)
	mockCalculator := &testCalculator{}

	quota := 1.0
	mockCalculator.updateQuota(quota)

	loggedCalculator := NewLoggedCalculator(mockCalculator, mockLogger)

	mockLogger.EXPECT().Info(gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
	actualQuota := loggedCalculator.GetQuota()
	require.Equal(t, quota, actualQuota)

	actualQuota = loggedCalculator.GetQuota()
	require.Equal(t, quota, actualQuota)

	quota = 2.0
	mockCalculator.updateQuota(quota)

	mockLogger.EXPECT().Info(gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
	actualQuota = loggedCalculator.GetQuota()
	require.Equal(t, quota, actualQuota)
}

func TestLoggedNamespaceCalculator(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)

	mockLogger := log.NewMockLogger(controller)
	mockCalculator := &testNamespaceCalculator{}

	namespace1 := "test-namespace-1"
	quota1 := 1.0
	mockCalculator.updateQuota(namespace1, quota1)

	namespace2 := "test-namespace-2"
	quota2 := 2.0
	mockCalculator.updateQuota(namespace2, quota2)

	loggedCalculator := NewLoggedNamespaceCalculator(mockCalculator, mockLogger)

	mockLogger.EXPECT().Info(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
	actualQuota := loggedCalculator.GetQuota(namespace1)
	require.Equal(t, quota1, actualQuota)

	mockLogger.EXPECT().Info(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
	actualQuota = loggedCalculator.GetQuota(namespace2)
	require.Equal(t, quota2, actualQuota)

	quota2 = 3.0
	mockCalculator.updateQuota(namespace2, quota2)

	mockLogger.EXPECT().Info(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
	actualQuota = loggedCalculator.GetQuota(namespace2)
	require.Equal(t, quota2, actualQuota)

	actualQuota = loggedCalculator.GetQuota(namespace1)
	require.Equal(t, quota1, actualQuota)
}

func (c *testCalculator) GetQuota() float64 {
	return c.quota
}

func (c *testCalculator) updateQuota(newQuota float64) {
	c.quota = newQuota
}

func (c *testNamespaceCalculator) GetQuota(namespace string) float64 {
	return c.namespaceQuota[namespace]
}

func (c *testNamespaceCalculator) updateQuota(namespace string, newQuota float64) {
	if c.namespaceQuota == nil {
		c.namespaceQuota = make(map[string]float64)
	}
	c.namespaceQuota[namespace] = newQuota
}
