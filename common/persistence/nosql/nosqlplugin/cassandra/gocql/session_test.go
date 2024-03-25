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

package gocql

import (
	"errors"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
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
