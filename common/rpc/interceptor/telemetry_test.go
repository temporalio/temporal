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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
)

const (
	startWorkflow = "StartWorkflowExecution"
	queryWorkflow = "QueryWorkflow"
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
			queryWorkflow,
			api.WorkflowServicePrefix + queryWorkflow,
			true,
		},
		{
			queryWorkflow,
			api.AdminServicePrefix + queryWorkflow,
			false,
		},
		{
			metrics.MatchingClientAddWorkflowTaskScope,
			api.WorkflowServicePrefix + queryWorkflow,
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

func TestHandleError(t *testing.T) {
	controller := gomock.NewController(t)
	registry := namespace.NewMockRegistry(controller)
	metricsHandler := metrics.NewMockHandler(controller)
	telemetry := NewTelemetryInterceptor(registry, metricsHandler, log.NewNoopLogger())

	testCases := []struct {
		name                 string
		err                  error
		expectServiceFailure bool
	}{
		{
			name:                 "serviceerror-invalid-argument",
			err:                  serviceerror.NewInvalidArgument("invalid argument"),
			expectServiceFailure: false,
		},
		{
			name:                 "statuserror-invalid-argument",
			err:                  status.Error(codes.InvalidArgument, "invalid argument"),
			expectServiceFailure: false,
		},
		{
			name:                 "serviceerror-internal",
			err:                  serviceerror.NewInternal("internal"),
			expectServiceFailure: true,
		},
		{
			name:                 "statuserror-internal",
			err:                  status.Error(codes.Internal, "internal"),
			expectServiceFailure: true,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			metricsHandler.EXPECT().Counter(metrics.ServiceErrorWithType.Name()).Return(metrics.NoopCounterMetricFunc).Times(1)
			times := 0
			if tt.expectServiceFailure {
				times = 1
			}
			metricsHandler.EXPECT().Counter(metrics.ServiceFailures.Name()).Return(metrics.NoopCounterMetricFunc).Times(times)
			telemetry.handleError(metricsHandler, []tag.Tag{}, tt.err)
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
			api.AdminServicePrefix + "DeleteWorkflowExecution",
			"AdminDeleteWorkflowExecution",
		},
		{
			"DeleteNamespace",
			api.OperatorServicePrefix + "DeleteNamespace",
			"OperatorDeleteNamespace",
		},
		{
			startWorkflow,
			api.WorkflowServicePrefix + startWorkflow,
			startWorkflow,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.methodName, func(t *testing.T) {
			operation := telemetry.overrideOperationTag(tt.fullName, tt.methodName)
			assert.Equal(t, tt.expectedOperation, operation)
		})
	}
}
