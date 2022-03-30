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

package reclaimresources

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
)

func Test_EnsureNoExecutionsActivity_NoExecutions(t *testing.T) {
	ctrl := gomock.NewController(t)
	visibilityManager := manager.NewMockVisibilityManager(ctrl)
	visibilityManager.EXPECT().GetName().Return("elasticsearch")

	visibilityManager.EXPECT().CountWorkflowExecutions(gomock.Any(), &manager.CountWorkflowExecutionsRequest{
		NamespaceID: "namespace-id",
		Namespace:   "namespace",
	}).Return(&manager.CountWorkflowExecutionsResponse{
		Count: 0,
	}, nil)

	a := &Activities{
		visibilityManager: visibilityManager,
		metadataManager:   nil,
		metricsClient:     metrics.NoopClient,
		logger:            log.NewNoopLogger(),
	}

	err := a.EnsureNoExecutionsActivity(context.Background(), "namespace-id", "namespace")
	require.NoError(t, err)

	ctrl.Finish()
}

func Test_EnsureNoExecutionsActivity_ExecutionsExist(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	ctrl := gomock.NewController(t)
	visibilityManager := manager.NewMockVisibilityManager(ctrl)
	visibilityManager.EXPECT().GetName().Return("elasticsearch")

	visibilityManager.EXPECT().CountWorkflowExecutions(gomock.Any(), &manager.CountWorkflowExecutionsRequest{
		NamespaceID: "namespace-id",
		Namespace:   "namespace",
	}).Return(&manager.CountWorkflowExecutionsResponse{
		Count: 1,
	}, nil)

	a := &Activities{
		visibilityManager: visibilityManager,
		metadataManager:   nil,
		metricsClient:     metrics.NoopClient,
		logger:            log.NewNoopLogger(),
	}
	env.RegisterActivity(a.EnsureNoExecutionsActivity)

	_, err := env.ExecuteActivity(a.EnsureNoExecutionsActivity, namespace.ID("namespace-id"), namespace.Name("namespace"))
	require.Error(t, err)
	var appErr *temporal.ApplicationError
	require.ErrorAs(t, err, &appErr)
	require.Equal(t, "ExecutionsStillExist", appErr.Type())
	ctrl.Finish()
}
