package tests

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/service/worker/workerdeployment"
	"go.temporal.io/server/tests/testcore"
)

const testCleanupIdentity = "test cleanup"

func newWorkflowCleanupEnv(t *testing.T, opts ...testcore.TestOption) *testcore.TestEnv {
	t.Helper()
	env := testcore.NewEnv(t, opts...)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(testcore.NewContext(), 30*time.Second)
		defer cancel()

		var executions []*commonpb.WorkflowExecution
		var nextPageToken []byte
		for {
			response, err := env.FrontendClient().ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Namespace:     env.Namespace().String(),
				PageSize:      1000,
				NextPageToken: nextPageToken,
				Query:         "ExecutionStatus = 'Running'",
			})
			if err != nil {
				if t.Failed() {
					t.Logf("workflow cleanup failed: list workflow executions: %v", err)
				} else {
					t.Errorf("workflow cleanup failed: list workflow executions: %v", err)
				}
				return
			}
			for _, execution := range response.GetExecutions() {
				executions = append(executions, execution.GetExecution())
			}
			nextPageToken = response.GetNextPageToken()
			if len(nextPageToken) == 0 {
				break
			}
		}

		var cleanupErr error
		for _, execution := range executions {
			_, err := env.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
				Namespace:         env.Namespace().String(),
				WorkflowExecution: execution,
				Reason:            "test cleanup",
				Identity:          testCleanupIdentity,
			})
			var notFoundErr *serviceerror.NotFound
			if err != nil && !errors.As(err, &notFoundErr) {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("terminate workflow %q: %w", execution.GetWorkflowId(), err))
			}
		}
		if cleanupErr != nil {
			if t.Failed() {
				t.Logf("workflow cleanup failed: %v", cleanupErr)
			} else {
				t.Errorf("workflow cleanup failed: %v", cleanupErr)
			}
		}
	})
	return env
}

func newWorkerDeploymentCleanupEnv(t *testing.T, opts ...testcore.TestOption) *testcore.TestEnv {
	t.Helper()
	env := newWorkflowCleanupEnv(t, opts...)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(testcore.NewContext(), 30*time.Second)
		defer cancel()

		var deploymentNames []string
		var nextPageToken []byte
		for {
			response, err := env.FrontendClient().ListWorkerDeployments(ctx, &workflowservice.ListWorkerDeploymentsRequest{
				Namespace:     env.Namespace().String(),
				PageSize:      1000,
				NextPageToken: nextPageToken,
			})
			if err != nil {
				if t.Failed() {
					t.Logf("worker deployment cleanup failed: list worker deployments: %v", err)
				} else {
					t.Errorf("worker deployment cleanup failed: list worker deployments: %v", err)
				}
				return
			}
			for _, deployment := range response.GetWorkerDeployments() {
				deploymentNames = append(deploymentNames, deployment.GetName())
			}
			nextPageToken = response.GetNextPageToken()
			if len(nextPageToken) == 0 {
				break
			}
		}

		var cleanupErr error
		for _, deploymentName := range deploymentNames {
			var notFoundErr *serviceerror.NotFound
			_, err := env.FrontendClient().SetWorkerDeploymentManager(ctx, &workflowservice.SetWorkerDeploymentManagerRequest{
				Namespace:      env.Namespace().String(),
				DeploymentName: deploymentName,
				NewManagerIdentity: &workflowservice.SetWorkerDeploymentManagerRequest_Self{
					Self: true,
				},
				Identity: testCleanupIdentity,
			})
			if err != nil && !errors.As(err, &notFoundErr) {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("take ownership of worker deployment %q: %w", deploymentName, err))
			}
			_, err = env.FrontendClient().SetWorkerDeploymentRampingVersion(ctx, &workflowservice.SetWorkerDeploymentRampingVersionRequest{
				Namespace:               env.Namespace().String(),
				DeploymentName:          deploymentName,
				Percentage:              0,
				Identity:                testCleanupIdentity,
				IgnoreMissingTaskQueues: true,
				AllowNoPollers:          true,
			})
			if err != nil && !errors.As(err, &notFoundErr) {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("clear ramping version for worker deployment %q: %w", deploymentName, err))
			}
			_, err = env.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
				Namespace:               env.Namespace().String(),
				DeploymentName:          deploymentName,
				Identity:                testCleanupIdentity,
				IgnoreMissingTaskQueues: true,
				AllowNoPollers:          true,
			})
			if err != nil && !errors.As(err, &notFoundErr) {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("clear current version for worker deployment %q: %w", deploymentName, err))
			}

			response, err := env.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
				Namespace:      env.Namespace().String(),
				DeploymentName: deploymentName,
			})
			if err != nil {
				if !errors.As(err, &notFoundErr) {
					cleanupErr = errors.Join(cleanupErr, fmt.Errorf("describe worker deployment %q: %w", deploymentName, err))
				}
				continue
			}
			for _, version := range response.GetWorkerDeploymentInfo().GetVersionSummaries() {
				deploymentVersion := version.GetDeploymentVersion()
				if deploymentVersion == nil {
					cleanupErr = errors.Join(cleanupErr, fmt.Errorf("worker deployment %q returned a version without an external deployment version", deploymentName))
					continue
				}
				request := &workflowservice.DeleteWorkerDeploymentVersionRequest{
					Namespace:         env.Namespace().String(),
					SkipDrainage:      true,
					Identity:          testCleanupIdentity,
					DeploymentVersion: deploymentVersion,
				}
				_, err = env.FrontendClient().DeleteWorkerDeploymentVersion(ctx, request)
				if err == nil || errors.As(err, &notFoundErr) {
					continue
				}
				_, forceDeleteErr := env.AdminClient().DeleteWorkflowExecution(ctx, &adminservice.DeleteWorkflowExecutionRequest{
					Namespace: env.Namespace().String(),
					Execution: &commonpb.WorkflowExecution{
						WorkflowId: workerdeployment.GenerateVersionWorkflowID(deploymentVersion.GetDeploymentName(), deploymentVersion.GetBuildId()),
					},
				})
				if forceDeleteErr != nil && !errors.As(forceDeleteErr, &notFoundErr) {
					cleanupErr = errors.Join(cleanupErr, fmt.Errorf("delete worker deployment version %q: %w", workerdeployment.GenerateVersionWorkflowID(deploymentVersion.GetDeploymentName(), deploymentVersion.GetBuildId()), errors.Join(err, forceDeleteErr)))
				}
			}

			_, err = env.FrontendClient().DeleteWorkerDeployment(ctx, &workflowservice.DeleteWorkerDeploymentRequest{
				Namespace:      env.Namespace().String(),
				DeploymentName: deploymentName,
				Identity:       testCleanupIdentity,
			})
			if err == nil || errors.As(err, &notFoundErr) {
				continue
			}
			_, forceDeleteErr := env.AdminClient().DeleteWorkflowExecution(ctx, &adminservice.DeleteWorkflowExecutionRequest{
				Namespace: env.Namespace().String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: workerdeployment.GenerateDeploymentWorkflowID(deploymentName),
				},
			})
			if forceDeleteErr != nil && !errors.As(forceDeleteErr, &notFoundErr) {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("delete worker deployment %q: %w", deploymentName, errors.Join(err, forceDeleteErr)))
			}
		}
		if cleanupErr != nil {
			if t.Failed() {
				t.Logf("worker deployment cleanup failed: %v", cleanupErr)
			} else {
				t.Errorf("worker deployment cleanup failed: %v", cleanupErr)
			}
		}
	})
	return env
}
