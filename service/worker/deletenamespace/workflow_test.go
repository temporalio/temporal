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

package deletenamespace

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/worker/deletenamespace/deleteexecutions"
	"go.temporal.io/server/service/worker/deletenamespace/reclaimresources"
)

func Test_DeleteNamespaceWorkflow_ByName(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	var a *activities

	env.OnActivity(a.GetNamespaceInfoActivity, mock.Anything, namespace.EmptyID, namespace.Name("namespace")).Return(
		getNamespaceInfoResult{
			NamespaceID: "namespace-id",
			Namespace:   "namespace",
		}, nil).Once()
	env.OnActivity(a.MarkNamespaceDeletedActivity, mock.Anything, namespace.Name("namespace")).Return(nil).Once()
	env.OnActivity(a.GenerateDeletedNamespaceNameActivity, mock.Anything, namespace.ID("namespace-id"), namespace.Name("namespace")).Return(namespace.Name("namespace-delete-220878"), nil).Once()
	env.OnActivity(a.RenameNamespaceActivity, mock.Anything, namespace.Name("namespace"), namespace.Name("namespace-delete-220878")).Return(nil).Once()

	env.RegisterWorkflow(reclaimresources.ReclaimResourcesWorkflow)
	env.OnWorkflow(reclaimresources.ReclaimResourcesWorkflow, mock.Anything, reclaimresources.ReclaimResourcesParams{DeleteExecutionsParams: deleteexecutions.DeleteExecutionsParams{
		Namespace:   "namespace-delete-220878",
		NamespaceID: "namespace-id",
		Config: deleteexecutions.DeleteExecutionsConfig{
			DeleteActivityRPS:                    100,
			PageSize:                             1000,
			PagesPerExecutionCount:               256,
			ConcurrentDeleteExecutionsActivities: 4,
		},
		PreviousSuccessCount: 0,
		PreviousErrorCount:   0,
	}}).Return(reclaimresources.ReclaimResourcesResult{}, nil).
		Once()

	// Delete by name.
	env.ExecuteWorkflow(DeleteNamespaceWorkflow, DeleteNamespaceWorkflowParams{
		Namespace:              "namespace",
		DeleteExecutionsConfig: deleteexecutions.DeleteExecutionsConfig{},
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result DeleteNamespaceWorkflowResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, namespace.Name("namespace-delete-220878"), result.DeletedNamespace)
	require.Equal(t, namespace.ID("namespace-id"), result.DeletedNamespaceID)
}

func Test_DeleteNamespaceWorkflow_ByID(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	var a *activities

	env.OnActivity(a.GetNamespaceInfoActivity, mock.Anything, namespace.ID("namespace-id"), namespace.EmptyName).Return(
		getNamespaceInfoResult{
			NamespaceID: "namespace-id",
			Namespace:   "namespace",
		}, nil).Once()
	env.OnActivity(a.MarkNamespaceDeletedActivity, mock.Anything, namespace.Name("namespace")).Return(nil).Once()
	env.OnActivity(a.GenerateDeletedNamespaceNameActivity, mock.Anything, namespace.ID("namespace-id"), namespace.Name("namespace")).Return(namespace.Name("namespace-delete-220878"), nil).Once()
	env.OnActivity(a.RenameNamespaceActivity, mock.Anything, namespace.Name("namespace"), namespace.Name("namespace-delete-220878")).Return(nil).Once()

	env.RegisterWorkflow(reclaimresources.ReclaimResourcesWorkflow)
	env.OnWorkflow(reclaimresources.ReclaimResourcesWorkflow, mock.Anything, reclaimresources.ReclaimResourcesParams{DeleteExecutionsParams: deleteexecutions.DeleteExecutionsParams{
		Namespace:   "namespace-delete-220878",
		NamespaceID: "namespace-id",
		Config: deleteexecutions.DeleteExecutionsConfig{
			DeleteActivityRPS:                    100,
			PageSize:                             1000,
			PagesPerExecutionCount:               256,
			ConcurrentDeleteExecutionsActivities: 4,
		},
		PreviousSuccessCount: 0,
		PreviousErrorCount:   0,
	}}).Return(reclaimresources.ReclaimResourcesResult{}, nil).
		Once()

	// Delete by name.
	env.ExecuteWorkflow(DeleteNamespaceWorkflow, DeleteNamespaceWorkflowParams{
		NamespaceID: "namespace-id",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result DeleteNamespaceWorkflowResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, namespace.Name("namespace-delete-220878"), result.DeletedNamespace)
	require.Equal(t, namespace.ID("namespace-id"), result.DeletedNamespaceID)
}

func Test_DeleteNamespaceWorkflow_ByNameAndID(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	// Delete by name and ID.
	env.ExecuteWorkflow(DeleteNamespaceWorkflow, DeleteNamespaceWorkflowParams{
		NamespaceID: "namespace-id",
		Namespace:   "namespace",
	})

	require.True(t, env.IsWorkflowCompleted())
	wfErr := env.GetWorkflowError()
	require.Error(t, wfErr)
	var applicationErr *temporal.ApplicationError
	require.ErrorAs(t, wfErr, &applicationErr)
}
