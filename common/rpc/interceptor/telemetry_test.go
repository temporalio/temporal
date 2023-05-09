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

package interceptor

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
)

func TestEmitActionMetric(t *testing.T) {
	controller := gomock.NewController(t)
	register := namespace.NewMockRegistry(controller)
	metricsHandler := metrics.NewMockHandler(controller)
	telemetry := NewTelemetryInterceptor(register, metricsHandler, log.NewNoopLogger())

	testCases := []struct {
		methodName        string
		fullName          string
		expectEmitMetrics bool
	}{
		{
			metrics.FrontendQueryWorkflowScope,
			frontendPackagePrefix + metrics.FrontendQueryWorkflowScope,
			true,
		},
		{
			metrics.FrontendQueryWorkflowScope,
			metrics.FrontendQueryWorkflowScope,
			false,
		},
		{
			metrics.MatchingClientAddWorkflowTaskScope,
			frontendPackagePrefix + metrics.FrontendQueryWorkflowScope,
			false,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.methodName, func(t *testing.T) {
			if tt.expectEmitMetrics {
				metricsHandler.EXPECT().Counter(gomock.Any()).Return(metrics.NoopCounterMetricFunc).Times(1)
			} else {
				metricsHandler.EXPECT().Counter(gomock.Any()).Return(metrics.NoopCounterMetricFunc).Times(0)
			}
			telemetry.emitActionMetric(tt.methodName, tt.fullName, nil, metricsHandler, nil)
		})
	}
}

func TestOperationOverwrite(t *testing.T) {
	controller := gomock.NewController(t)
	register := namespace.NewMockRegistry(controller)
	metricsHandler := metrics.NewMockHandler(controller)
	telemetry := NewTelemetryInterceptor(register, metricsHandler, log.NewNoopLogger())

	testCases := []struct {
		methodName        string
		fullName          string
		expectedOperation string
	}{
		{
			"DeleteWorkflowExecution",
			adminServicePrefix + "DeleteWorkflowExecution",
			"AdminDeleteWorkflowExecution",
		},
		{
			"DeleteNamespace",
			operatorServicePrefix + "DeleteNamespace",
			"OperatorDeleteNamespace",
		},
		{
			metrics.FrontendStartWorkflowExecutionScope,
			frontendPackagePrefix + metrics.FrontendStartWorkflowExecutionScope,
			metrics.FrontendStartWorkflowExecutionScope,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.methodName, func(t *testing.T) {
			operation := telemetry.overrideOperationTag(tt.fullName, tt.methodName)
			assert.Equal(t, tt.expectedOperation, operation)
		})
	}

}
