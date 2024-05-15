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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
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
