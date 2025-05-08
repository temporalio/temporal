package gocql

import (
	"errors"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.uber.org/mock/gomock"
)

func TestSessionEmitsMetricOnRefreshError(t *testing.T) {
	controller := gomock.NewController(t)
	metricsHandler := metrics.NewMockHandler(controller)
	s := session{
		status: common.DaemonStatusStarted,
		newClusterConfigFunc: func() (*gocql.ClusterConfig, error) {
			return nil, errors.New("mock error for failing cluster creation")
		},
		logger:         log.NewNoopLogger(),
		metricsHandler: metricsHandler,
	}

	metricsHandler.EXPECT().WithTags(metrics.FailureTag(refreshErrorTagValue)).Return(metricsHandler)
	metricsHandler.EXPECT().Counter(metrics.CassandraSessionRefreshFailures.Name()).Return(metrics.NoopCounterMetricFunc)

	s.refresh()
	controller.Finish()
}

func TestSessionEmitsMetricOnRefreshThrottle(t *testing.T) {
	controller := gomock.NewController(t)
	metricsHandler := metrics.NewMockHandler(controller)
	s := session{
		status:          common.DaemonStatusStarted,
		logger:          log.NewNoopLogger(),
		metricsHandler:  metricsHandler,
		sessionInitTime: time.Now().UTC(),
	}

	metricsHandler.EXPECT().WithTags(metrics.FailureTag(refreshThrottleTagValue)).Return(metricsHandler)
	metricsHandler.EXPECT().Counter(metrics.CassandraSessionRefreshFailures.Name()).Return(metrics.NoopCounterMetricFunc)

	s.refresh()
	controller.Finish()
}

func TestPanicCapture(t *testing.T) {
	_, err := initSession(log.NewNoopLogger(), func() (*gocql.ClusterConfig, error) {
		return &gocql.ClusterConfig{Hosts: []string{"0.0.0.0"}}, nil
	}, metrics.NoopMetricsHandler)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "panic:")
}
