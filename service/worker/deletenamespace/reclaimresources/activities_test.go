package reclaimresources

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.uber.org/mock/gomock"
)

func Test_EnsureNoExecutionsAdvVisibilityActivity_NoExecutions(t *testing.T) {
	ctrl := gomock.NewController(t)
	visibilityManager := manager.NewMockVisibilityManager(ctrl)

	visibilityManager.EXPECT().CountWorkflowExecutions(gomock.Any(), &manager.CountWorkflowExecutionsRequest{
		NamespaceID: "namespace-id",
		Namespace:   "namespace",
		Query:       sadefs.QueryWithAnyNamespaceDivision(""),
	}).Return(&manager.CountWorkflowExecutionsResponse{
		Count: 0,
	}, nil)

	a := &Activities{
		visibilityManager: visibilityManager,
		logger:            log.NewTestLogger(),
	}

	err := a.EnsureNoExecutionsAdvVisibilityActivity(context.Background(), "namespace-id", "namespace", 0)
	require.NoError(t, err)
}

func Test_EnsureNoExecutionsAdvVisibilityActivity_ExecutionsExist(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	ctrl := gomock.NewController(t)
	visibilityManager := manager.NewMockVisibilityManager(ctrl)

	visibilityManager.EXPECT().CountWorkflowExecutions(gomock.Any(), &manager.CountWorkflowExecutionsRequest{
		NamespaceID: "namespace-id",
		Namespace:   "namespace",
		Query:       sadefs.QueryWithAnyNamespaceDivision(""),
	}).Return(&manager.CountWorkflowExecutionsResponse{
		Count: 1,
	}, nil)

	a := &Activities{
		visibilityManager: visibilityManager,
		logger:            log.NewTestLogger(),
	}
	env.RegisterActivity(a.EnsureNoExecutionsAdvVisibilityActivity)

	_, err := env.ExecuteActivity(a.EnsureNoExecutionsAdvVisibilityActivity, namespace.ID("namespace-id"), namespace.Name("namespace"), 0)
	require.Error(t, err)
	var appErr *temporal.ApplicationError
	require.ErrorAs(t, err, &appErr)
	require.Equal(t, "ExecutionsStillExist", appErr.Type())
}

func Test_EnsureNoExecutionsAdvVisibilityActivity_NotDeletedExecutionsExist(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	ctrl := gomock.NewController(t)
	visibilityManager := manager.NewMockVisibilityManager(ctrl)

	visibilityManager.EXPECT().CountWorkflowExecutions(gomock.Any(), &manager.CountWorkflowExecutionsRequest{
		NamespaceID: "namespace-id",
		Namespace:   "namespace",
		Query:       sadefs.QueryWithAnyNamespaceDivision(""),
	}).Return(&manager.CountWorkflowExecutionsResponse{
		Count: 10,
	}, nil)

	a := &Activities{
		visibilityManager: visibilityManager,
		logger:            log.NewTestLogger(),
	}
	env.RegisterActivity(a.EnsureNoExecutionsAdvVisibilityActivity)

	_, err := env.ExecuteActivity(a.EnsureNoExecutionsAdvVisibilityActivity, namespace.ID("namespace-id"), namespace.Name("namespace"), 10)
	require.Error(t, err)
	var appErr *temporal.ApplicationError
	require.ErrorAs(t, err, &appErr)
	require.Equal(t, "NotDeletedExecutionsStillExist", appErr.Type())
}
