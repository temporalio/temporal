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
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/service/worker/deletenamespace/deleteexecutions"
	"go.temporal.io/server/service/worker/deletenamespace/errors"
	"go.temporal.io/server/service/worker/deletenamespace/reclaimresources"
)

func Test_DeleteNamespaceWorkflow_ByName(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	testSuite.SetLogger(log.NewSdkLogger(log.NewTestLogger()))
	env := testSuite.NewTestWorkflowEnvironment()
	var la *localActivities

	env.OnActivity(la.GetNamespaceInfoActivity, mock.Anything, namespace.EmptyID, namespace.Name("namespace")).Return(
		getNamespaceInfoResult{
			NamespaceID: "namespace-id",
			Namespace:   "namespace",
		}, nil).Once()
	env.OnActivity(la.ValidateProtectedNamespacesActivity, mock.Anything, mock.Anything).Return(nil).Once()
	env.OnActivity(la.ValidateNexusEndpointsActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	env.OnActivity(la.MarkNamespaceDeletedActivity, mock.Anything, namespace.Name("namespace")).Return(nil).Once()
	env.OnActivity(la.GenerateDeletedNamespaceNameActivity, mock.Anything, namespace.ID("namespace-id"), namespace.Name("namespace")).Return(namespace.Name("namespace-delete-220878"), nil).Once()
	env.OnActivity(la.RenameNamespaceActivity, mock.Anything, namespace.Name("namespace"), namespace.Name("namespace-delete-220878")).Return(nil).Once()

	env.RegisterWorkflow(reclaimresources.ReclaimResourcesWorkflow)
	env.OnWorkflow(reclaimresources.ReclaimResourcesWorkflow, mock.Anything, reclaimresources.ReclaimResourcesParams{DeleteExecutionsParams: deleteexecutions.DeleteExecutionsParams{
		Namespace:   "namespace-delete-220878",
		NamespaceID: "namespace-id",
		Config: deleteexecutions.DeleteExecutionsConfig{
			DeleteActivityRPS:                    100,
			PageSize:                             1000,
			PagesPerExecution:                    256,
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
	testSuite.SetLogger(log.NewSdkLogger(log.NewTestLogger()))
	env := testSuite.NewTestWorkflowEnvironment()
	var la *localActivities

	env.OnActivity(la.GetNamespaceInfoActivity, mock.Anything, namespace.ID("namespace-id"), namespace.EmptyName).Return(
		getNamespaceInfoResult{
			NamespaceID: "namespace-id",
			Namespace:   "namespace",
		}, nil).Once()
	env.OnActivity(la.ValidateProtectedNamespacesActivity, mock.Anything, mock.Anything).Return(nil).Once()
	env.OnActivity(la.ValidateNexusEndpointsActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	env.OnActivity(la.MarkNamespaceDeletedActivity, mock.Anything, namespace.Name("namespace")).Return(nil).Once()
	env.OnActivity(la.GenerateDeletedNamespaceNameActivity, mock.Anything, namespace.ID("namespace-id"), namespace.Name("namespace")).Return(namespace.Name("namespace-delete-220878"), nil).Once()
	env.OnActivity(la.RenameNamespaceActivity, mock.Anything, namespace.Name("namespace"), namespace.Name("namespace-delete-220878")).Return(nil).Once()

	env.RegisterWorkflow(reclaimresources.ReclaimResourcesWorkflow)
	env.OnWorkflow(reclaimresources.ReclaimResourcesWorkflow, mock.Anything, reclaimresources.ReclaimResourcesParams{DeleteExecutionsParams: deleteexecutions.DeleteExecutionsParams{
		Namespace:   "namespace-delete-220878",
		NamespaceID: "namespace-id",
		Config: deleteexecutions.DeleteExecutionsConfig{
			DeleteActivityRPS:                    100,
			PageSize:                             1000,
			PagesPerExecution:                    256,
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
	testSuite.SetLogger(log.NewSdkLogger(log.NewTestLogger()))
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

func Test_DeleteReplicatedNamespace(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	var la *localActivities

	env.OnActivity(la.GetNamespaceInfoActivity, mock.Anything, namespace.ID("namespace-id"), namespace.EmptyName).Return(
		getNamespaceInfoResult{
			NamespaceID: "namespace-id",
			Namespace:   "namespace",
			Clusters:    []string{"active", "passive"},
		}, nil).Once()

	env.ExecuteWorkflow(DeleteNamespaceWorkflow, DeleteNamespaceWorkflowParams{
		NamespaceID: "namespace-id",
	})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	require.Contains(t, err.Error(), "namespace namespace is replicated in several clusters [active,passive]: remove all other cluster and retry")
}

func Test_DeleteSystemNamespace(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	var la *localActivities

	env.OnActivity(la.GetNamespaceInfoActivity, mock.Anything, namespace.EmptyID, namespace.Name(primitives.SystemLocalNamespace)).Return(
		getNamespaceInfoResult{
			NamespaceID: primitives.SystemNamespaceID,
			Namespace:   primitives.SystemLocalNamespace,
		}, nil).Once()

	env.ExecuteWorkflow(DeleteNamespaceWorkflow, DeleteNamespaceWorkflowParams{
		Namespace: primitives.SystemLocalNamespace,
	})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	require.Contains(t, err.Error(), "unable to delete system namespace")
}

func Test_DeleteProtectedNamespace(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	la := &localActivities{
		protectedNamespaces: func() []string {
			return []string{"namespace"}
		},
	}

	env.OnActivity(la.GetNamespaceInfoActivity, mock.Anything, namespace.ID("namespace-id"), namespace.EmptyName).Return(
		getNamespaceInfoResult{
			NamespaceID: "namespace-id",
			Namespace:   "namespace",
		}, nil).Once()
	env.RegisterActivity(la.ValidateProtectedNamespacesActivity)

	env.ExecuteWorkflow(DeleteNamespaceWorkflow, DeleteNamespaceWorkflowParams{
		NamespaceID: "namespace-id",
	})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	var appErr *temporal.ApplicationError
	require.ErrorAs(t, err, &appErr)
	require.Equal(t, appErr.Message(), "namespace namespace is protected from deletion")
}

func Test_DeleteNamespaceUsedByNexus(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	la := &localActivities{}

	env.OnActivity(la.GetNamespaceInfoActivity, mock.Anything, namespace.ID("namespace-id"), namespace.EmptyName).Return(
		getNamespaceInfoResult{
			NamespaceID: "namespace-id",
			Namespace:   "namespace",
		}, nil).Once()
	env.OnActivity(la.ValidateProtectedNamespacesActivity, mock.Anything, mock.Anything).Return(nil).Once()
	env.OnActivity(la.ValidateNexusEndpointsActivity, mock.Anything, mock.Anything, mock.Anything).
		Return(errors.NewFailedPrecondition("cannot delete a namespace that is a target of a Nexus endpoint", nil)).
		Once()

	env.ExecuteWorkflow(DeleteNamespaceWorkflow, DeleteNamespaceWorkflowParams{
		NamespaceID: "namespace-id",
	})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	var appErr *temporal.ApplicationError
	require.ErrorAs(t, err, &appErr)
	require.Equal(t, appErr.Message(), "cannot delete a namespace that is a target of a Nexus endpoint")
}
