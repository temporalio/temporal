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
	var la *localActivities

	t.Run("namespace that active in the current cluster should be deleted", func(t *testing.T) {
		env := testSuite.NewTestWorkflowEnvironment()
		env.OnActivity(la.GetNamespaceInfoActivity, mock.Anything, namespace.ID("namespace-id"), namespace.EmptyName).Return(
			getNamespaceInfoResult{
				NamespaceID:    "namespace-id",
				Namespace:      "namespace",
				Clusters:       []string{"active", "passive"},
				ActiveCluster:  "active",
				CurrentCluster: "active",
			}, nil).Once()

		env.OnActivity(la.ValidateProtectedNamespacesActivity, mock.Anything, mock.Anything).Return(nil).Once()
		env.OnActivity(la.ValidateNexusEndpointsActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		env.OnActivity(la.MarkNamespaceDeletedActivity, mock.Anything, mock.Anything).Return(nil).Once()
		env.OnActivity(la.GenerateDeletedNamespaceNameActivity, mock.Anything, mock.Anything, mock.Anything).Return(namespace.EmptyName, nil).Once()
		env.OnActivity(la.RenameNamespaceActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		env.RegisterWorkflow(reclaimresources.ReclaimResourcesWorkflow)
		env.OnWorkflow(reclaimresources.ReclaimResourcesWorkflow, mock.Anything, mock.Anything).Return(reclaimresources.ReclaimResourcesResult{}, nil).Once()

		env.ExecuteWorkflow(DeleteNamespaceWorkflow, DeleteNamespaceWorkflowParams{
			NamespaceID: "namespace-id",
		})

		require.True(t, env.IsWorkflowCompleted())
		err := env.GetWorkflowError()
		require.NoError(t, err)
	})

	t.Run("namespace that passive in the current cluster should NOT be deleted", func(t *testing.T) {
		env := testSuite.NewTestWorkflowEnvironment()
		env.OnActivity(la.GetNamespaceInfoActivity, mock.Anything, namespace.ID("namespace-id"), namespace.EmptyName).Return(
			getNamespaceInfoResult{
				NamespaceID:    "namespace-id",
				Namespace:      "namespace",
				Clusters:       []string{"active", "passive"},
				ActiveCluster:  "active",
				CurrentCluster: "passive",
			}, nil).Once()

		env.ExecuteWorkflow(DeleteNamespaceWorkflow, DeleteNamespaceWorkflowParams{
			NamespaceID: "namespace-id",
		})

		require.True(t, env.IsWorkflowCompleted())
		err := env.GetWorkflowError()
		require.Error(t, err)
		require.Contains(t, err.Error(), "namespace namespace is passive in current cluster passive: remove cluster passive from cluster list or make namespace active in this cluster and retry")
	})

	t.Run("namespace that doesn't have current cluster in the cluster list should be deleted", func(t *testing.T) {
		env := testSuite.NewTestWorkflowEnvironment()
		env.OnActivity(la.GetNamespaceInfoActivity, mock.Anything, namespace.ID("namespace-id"), namespace.EmptyName).Return(
			getNamespaceInfoResult{
				NamespaceID:    "namespace-id",
				Namespace:      "namespace",
				Clusters:       []string{"active", "passive"},
				ActiveCluster:  "active",
				CurrentCluster: "another-cluster",
			}, nil).Once()

		env.OnActivity(la.ValidateProtectedNamespacesActivity, mock.Anything, mock.Anything).Return(nil).Once()
		env.OnActivity(la.ValidateNexusEndpointsActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		env.OnActivity(la.MarkNamespaceDeletedActivity, mock.Anything, mock.Anything).Return(nil).Once()
		env.OnActivity(la.GenerateDeletedNamespaceNameActivity, mock.Anything, mock.Anything, mock.Anything).Return(namespace.EmptyName, nil).Once()
		env.OnActivity(la.RenameNamespaceActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		env.RegisterWorkflow(reclaimresources.ReclaimResourcesWorkflow)
		env.OnWorkflow(reclaimresources.ReclaimResourcesWorkflow, mock.Anything, mock.Anything).Return(reclaimresources.ReclaimResourcesResult{}, nil).Once()

		env.ExecuteWorkflow(DeleteNamespaceWorkflow, DeleteNamespaceWorkflowParams{
			NamespaceID: "namespace-id",
		})

		require.True(t, env.IsWorkflowCompleted())
		err := env.GetWorkflowError()
		require.NoError(t, err)
	})
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
