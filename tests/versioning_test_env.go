package tests

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/historyrequire"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/worker/workerdeployment"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type versionStatus int

const (
	tqTypeWf        = enumspb.TASK_QUEUE_TYPE_WORKFLOW
	tqTypeAct       = enumspb.TASK_QUEUE_TYPE_ACTIVITY
	tqTypeNexus     = enumspb.TASK_QUEUE_TYPE_NEXUS
	vbUnspecified   = enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED
	vbPinned        = enumspb.VERSIONING_BEHAVIOR_PINNED
	vbUnpinned      = enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE
	ver3MinPollTime = common.MinLongPollTimeout + time.Millisecond*200

	versionStatusNil      = versionStatus(0)
	versionStatusInactive = versionStatus(1)
	versionStatusCurrent  = versionStatus(2)
	versionStatusRamping  = versionStatus(3)
	versionStatusDraining = versionStatus(4)
	versionStatusDrained  = versionStatus(5)

	versioning3DeploymentWorkflowVersion = workerdeployment.VersionDataRevisionNumber
)

var _ = testhooks.MatchingIgnoreRoutingConfigRevisionCheck

type VersioningTestEnv struct {
	*testcore.TestEnv
	versioningTestContext *versioningTestContext
}

type versioningTestContext struct {
	t       func() *testing.T
	context func() context.Context
	await   func(func(*versioningTestContext), time.Duration, time.Duration)
	*require.Assertions
	protorequire.ProtoAssertions
	historyrequire.HistoryRequire
}

func (c *versioningTestContext) T() *testing.T {
	return c.t()
}

func (c *versioningTestContext) Context() context.Context {
	return c.context()
}

func (c *versioningTestContext) Await(fn func(*versioningTestContext), timeout, interval time.Duration) {
	c.await(fn, timeout, interval)
}

func newVersioningTestEnv(t *testing.T, opts ...testcore.TestOption) *VersioningTestEnv {
	return &VersioningTestEnv{
		TestEnv: testcore.NewEnv(t, opts...),
	}
}

func (env *VersioningTestEnv) withVersioningTestContext(ctx *versioningTestContext) *VersioningTestEnv {
	rebound := *env
	rebound.versioningTestContext = ctx
	return &rebound
}

func (env *VersioningTestEnv) waitForTaskQueueVersioningInfo(
	ctx context.Context,
	tb testing.TB,
	tq *taskqueuepb.TaskQueue,
	expectedCurrentVersion string,
	expectedRampingVersion string,
	rampingPercentage float32,
) {
	await.Require(ctx, tb, func(t *await.T) {
		resp, err := env.FrontendClient().DescribeTaskQueue(t.Context(), &workflowservice.DescribeTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: tq,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		protorequire.ProtoEqual(t, worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(expectedCurrentVersion), resp.GetVersioningInfo().GetCurrentDeploymentVersion())
		protorequire.ProtoEqual(t, worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(expectedRampingVersion), resp.GetVersioningInfo().GetRampingDeploymentVersion())
		require.Equal(t, expectedCurrentVersion, resp.GetVersioningInfo().GetCurrentVersion()) //nolint:staticcheck // SA1019: old worker versioning
		require.Equal(t, expectedRampingVersion, resp.GetVersioningInfo().GetRampingVersion()) //nolint:staticcheck // SA1019: old worker versioning
		require.InDelta(t, rampingPercentage, resp.GetVersioningInfo().GetRampingVersionPercentage(), 0.001)
	}, 10*time.Second, 200*time.Millisecond)
}

func (env *VersioningTestEnv) findVersionTaskQueue(
	taskQueues []*workflowservice.DescribeWorkerDeploymentVersionResponse_VersionTaskQueue,
	tqName string,
	tqType enumspb.TaskQueueType,
) *workflowservice.DescribeWorkerDeploymentVersionResponse_VersionTaskQueue {
	for _, tq := range taskQueues {
		if tq.GetName() == tqName && tq.GetType() == tqType {
			return tq
		}
	}
	return nil
}

func (env *VersioningTestEnv) pollAndQueryWorkflow(
	tv *testvars.TestVars,
	sticky bool,
) {
	s := env.versioningTestContext
	queryResultCh := make(chan any)
	env.pollWftAndHandleQueries(tv, sticky, queryResultCh,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondQueryTaskCompletedRequest, error) {
			return &workflowservice.RespondQueryTaskCompletedRequest{}, nil
		})

	_, err := env.queryWorkflow(tv)
	s.NoError(err)

	<-queryResultCh
}

// drainWorkflowTaskAfterSetCurrent is a helper that sets the current deployment version,
// drains the initial workflow task from the execution, and ensures the task is correctly
// routed to the appropriate build.
func (env *VersioningTestEnv) drainWorkflowTaskAfterSetCurrentWithOverride(
	tv *testvars.TestVars,
	override *workflowpb.VersioningOverride,
) (*commonpb.WorkflowExecution, string) {
	s := env.versioningTestContext
	wftCompleted := make(chan struct{})
	env.pollWftAndHandle(tv, false, wftCompleted,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			if override != nil {
				env.verifyWorkflowVersioning(tv, vbUnspecified, nil, override, nil)
			} else {
				env.verifyWorkflowVersioning(tv, vbUnspecified, nil, override, tv.DeploymentVersionTransition())
			}
			return env.respondEmptyWft(tv, false, vbUnpinned), nil
		})
	env.waitForDeploymentDataPropagation(tv, versionStatusInactive, false, tqTypeWf)
	env.setCurrentDeployment(tv)

	runID := env.startWorkflow(tv, override)
	execution := tv.WithRunID(runID).WorkflowExecution()

	env.WaitForChannel(wftCompleted)

	return execution, runID
}

// drainWorkflowTaskAfterSetCurrent is a helper that sets the current deployment version,
// drains the initial workflow task from the execution, and ensures the task is correctly
// routed to the appropriate build.
func (env *VersioningTestEnv) drainWorkflowTaskAfterSetCurrent(
	tv *testvars.TestVars,
) (*commonpb.WorkflowExecution, string) {
	s := env.versioningTestContext
	wftCompleted := make(chan struct{})
	env.pollWftAndHandle(tv, false, wftCompleted,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			env.verifyWorkflowVersioning(tv, vbUnspecified, nil, nil, tv.DeploymentVersionTransition())
			return env.respondEmptyWft(tv, false, vbUnpinned), nil
		})
	env.waitForDeploymentDataPropagation(tv, versionStatusInactive, false, tqTypeWf)
	env.setCurrentDeployment(tv)

	runID := env.startWorkflow(tv, nil)
	execution := tv.WithRunID(runID).WorkflowExecution()

	env.WaitForChannel(wftCompleted)

	return execution, runID
}

func (env *VersioningTestEnv) pollAndDispatchNexusTask(
	tv *testvars.TestVars,
	nexusRequest *matchingservice.DispatchNexusTaskRequest,
) {
	s := env.versioningTestContext
	matchingClient := env.GetTestCluster().MatchingClient()

	nexusCompleted := make(chan any)
	env.pollNexusTaskAndHandle(tv, false, nexusCompleted,
		func(task *workflowservice.PollNexusTaskQueueResponse) (*workflowservice.RespondNexusTaskCompletedRequest, error) {
			s.NotNil(task)
			return &workflowservice.RespondNexusTaskCompletedRequest{}, nil // response object gets filled during processing
		})

	_, err := matchingClient.DispatchNexusTask(s.Context(), nexusRequest)
	s.NoError(err)
	<-nexusCompleted
}

func (env *VersioningTestEnv) describeVersioningInfo(
	execution *commonpb.WorkflowExecution,
) *workflowpb.WorkflowExecutionVersioningInfo {
	s := env.versioningTestContext
	resp, err := env.FrontendClient().DescribeWorkflowExecution(
		s.Context(),
		&workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: execution,
		},
	)
	s.NoError(err)
	return resp.GetWorkflowExecutionInfo().GetVersioningInfo()
}

func (env *VersioningTestEnv) requireOneTimeOverride(
	execution *commonpb.WorkflowExecution,
	tv *testvars.TestVars,
) {
	s := env.versioningTestContext
	versioningInfo := env.describeVersioningInfo(execution)
	oneTime := versioningInfo.GetVersioningOverride().GetOneTime()
	s.NotNil(oneTime)
	s.ProtoEqual(tv.ExternalDeploymentVersion(), oneTime.GetTargetDeploymentVersion())
}

func (env *VersioningTestEnv) requireNoVersioningOverride(
	execution *commonpb.WorkflowExecution,
) {
	s := env.versioningTestContext
	versioningInfo := env.describeVersioningInfo(execution)
	s.Nil(versioningInfo.GetVersioningOverride())
}

func (env *VersioningTestEnv) updateVersioningOverride(
	execution *commonpb.WorkflowExecution,
	override *workflowpb.VersioningOverride,
) {
	s := env.versioningTestContext
	_, err := env.FrontendClient().UpdateWorkflowExecutionOptions(s.Context(), &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:                env.Namespace().String(),
		WorkflowExecution:        execution,
		WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{VersioningOverride: override},
		UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
	})
	s.NoError(err)
}

func (env *VersioningTestEnv) pollWorkflowTask(
	tv *testvars.TestVars,
) *workflowservice.PollWorkflowTaskQueueResponse {
	s := env.versioningTestContext
	task, err := env.FrontendClient().PollWorkflowTaskQueue(s.Context(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace:         env.Namespace().String(),
		Identity:          tv.WorkerIdentity(),
		TaskQueue:         tv.TaskQueue(),
		DeploymentOptions: tv.WorkerDeploymentOptions(true),
	})
	s.NoError(err)
	s.NotEmpty(task.GetTaskToken())
	return task
}

func (env *VersioningTestEnv) completeWorkflowTask(
	tv *testvars.TestVars,
	task *workflowservice.PollWorkflowTaskQueueResponse,
	request *workflowservice.RespondWorkflowTaskCompletedRequest,
) {
	s := env.versioningTestContext
	request.Namespace = env.Namespace().String()
	request.Identity = tv.WorkerIdentity()
	request.TaskToken = task.GetTaskToken()
	_, err := env.FrontendClient().RespondWorkflowTaskCompleted(s.Context(), request)
	s.NoError(err)
}

func (env *VersioningTestEnv) pollUntilChildWorkflowTask(
	tv *testvars.TestVars,
	childWorkflowID string,
	handleChild func(*workflowservice.PollWorkflowTaskQueueResponse) *workflowservice.RespondWorkflowTaskCompletedRequest,
) *commonpb.WorkflowExecution {
	s := env.versioningTestContext
	// Starting the child can create two WFTs on the same version/task queue:
	// one parent follow-up WFT for ChildWorkflowExecutionStarted, and one child
	// first WFT. Drain the parent follow-up if it arrives first.
	const maxWorkflowTasksAfterChildStart = 2
	var childExecution *commonpb.WorkflowExecution
	for i := 0; i < maxWorkflowTasksAfterChildStart && childExecution == nil; i++ {
		env.pollWftAndHandle(tv, false, nil,
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				s.NotNil(task)
				if task.GetWorkflowExecution().GetWorkflowId() == childWorkflowID {
					childExecution = task.GetWorkflowExecution()
					return handleChild(task), nil
				}
				return env.respondEmptyWft(tv, false, vbPinned), nil
			})
	}
	s.NotNil(childExecution)
	return childExecution
}

// Signal to trigger a normal WFT
func (env *VersioningTestEnv) triggerNormalWFT(tv *testvars.TestVars, execution *commonpb.WorkflowExecution) {
	s := env.versioningTestContext
	_, err := env.FrontendClient().SignalWorkflowExecution(s.Context(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: execution,
		SignalName:        tv.SignalName(),
		Input:             tv.Any().Payloads(),
		Identity:          tv.WorkerIdentity(),
	})
	s.NoError(err)
}

// Trigger a normal task and then fail the task twice to trigger a transient WFT
func (env *VersioningTestEnv) triggerTransientWFT(tv *testvars.TestVars, execution *commonpb.WorkflowExecution) {
	s := env.versioningTestContext
	env.triggerNormalWFT(tv, execution)

	// Poll and FAIL the WFT to create a transient WFT situation
	pollResp, err := env.FrontendClient().PollWorkflowTaskQueue(s.Context(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace:         env.Namespace().String(),
		TaskQueue:         tv.TaskQueue(),
		Identity:          tv.WorkerIdentity(),
		DeploymentOptions: tv.WorkerDeploymentOptions(true),
	})
	s.NoError(err)
	s.NotNil(pollResp)
	s.NotEmpty(pollResp.TaskToken)

	// Fail the workflow task - this will cause a transient WFT to be scheduled
	_, err = env.FrontendClient().RespondWorkflowTaskFailed(s.Context(), &workflowservice.RespondWorkflowTaskFailedRequest{
		Namespace: env.Namespace().String(),
		TaskToken: pollResp.TaskToken,
		Cause:     enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE,
		Identity:  tv.WorkerIdentity(),
	})
	s.NoError(err)
}

// Verify this is a speculative task - events not yet in persisted history
func (env *VersioningTestEnv) verifySpeculativeTask(execution *commonpb.WorkflowExecution) {
	s := env.versioningTestContext
	events := env.GetHistory(env.Namespace().String(), execution)
	s.EqualHistoryEvents(`
		1 WorkflowExecutionStarted
		2 WorkflowTaskScheduled
		3 WorkflowTaskStarted
		4 WorkflowTaskCompleted
		5 WorkflowExecutionSignaled
		6 WorkflowTaskScheduled
		7 WorkflowTaskStarted
		8 WorkflowTaskCompleted
		9 WorkflowTaskScheduled
		10 WorkflowTaskStarted
	`, events)
}

func (env *VersioningTestEnv) setCurrentDeployment(tv *testvars.TestVars) {
	s := env.versioningTestContext
	ctx, cancel := context.WithTimeout(s.Context(), 60*time.Second)
	defer cancel()

	failedPrecondition := serviceerror.NewFailedPreconditionf(workerdeployment.ErrCurrentVersionDoesNotHaveAllTaskQueues, tv.DeploymentVersionStringV32()).Error()
	s.Await(func(s *versioningTestContext) {
		req := &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
			Namespace:      env.Namespace().String(),
			DeploymentName: tv.DeploymentSeries(),
		}
		req.BuildId = tv.BuildID()
		_, err := env.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, req)
		if env.shouldRetryWorkerDeploymentRPC(s.Context(), err, failedPrecondition) {
			s.NoError(err)
			return
		}
		s.NoError(err)
	}, 60*time.Second, 500*time.Millisecond)

	// Wait for propagation to complete since we have tests using async entity workflows to set the current version
	env.waitForDeploymentDataPropagationQueryWorkerDeployment(tv)
}

// pollUntilRegistered registers versioned pollers for the given deployment.
// tqTypes controls which task queue types to poll; it defaults to workflow only.
// Pollers run continuously until all TQ types are registered.
func (env *VersioningTestEnv) pollUntilRegistered(tv *testvars.TestVars, tqTypes ...enumspb.TaskQueueType) {
	s := env.versioningTestContext
	if len(tqTypes) == 0 {
		tqTypes = []enumspb.TaskQueueType{tqTypeWf}
	}
	pollCtx, cancel := context.WithCancel(s.Context())
	for _, tqType := range tqTypes {
		go func() {
			for pollCtx.Err() == nil {
				switch tqType {
				case tqTypeWf:
					env.idlePollWorkflow(pollCtx, tv, true, ver3MinPollTime, "should not get any tasks yet")
				case tqTypeAct:
					env.idlePollActivity(tv, true, ver3MinPollTime, "should not get any tasks yet")
				case tqTypeNexus:
					env.idlePollNexus(pollCtx, tv, true, ver3MinPollTime, "should not get any tasks yet")
				default:
					panic("invalid task queue type")
				}
			}
		}()
	}

	// Wait until the version is visible and all requested task queue types are registered.
	s.Await(func(s *versioningTestContext) {
		resp, err := env.FrontendClient().DescribeWorkerDeploymentVersion(s.Context(), &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: env.Namespace().String(),
			Version:   tv.DeploymentVersionString(),
		})
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			s.NoError(err)
			return
		}
		s.NoError(err)
		tqName := tv.TaskQueue().GetName()
		for _, tqType := range tqTypes {
			found := false
			for _, tq := range resp.GetVersionTaskQueues() {
				if tq.GetName() == tqName && tq.GetType() == tqType {
					found = true
					break
				}
			}
			s.True(found)
		}
	}, 30*time.Second, 500*time.Millisecond)
	cancel()
}

func (env *VersioningTestEnv) unsetCurrentDeployment(tv *testvars.TestVars) {
	s := env.versioningTestContext
	ctx, cancel := context.WithTimeout(s.Context(), 60*time.Second)
	defer cancel()

	s.Await(func(s *versioningTestContext) {
		req := &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
			Namespace:      env.Namespace().String(),
			DeploymentName: tv.DeploymentSeries(),
		}
		_, err := env.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, req)
		if env.shouldRetryWorkerDeploymentRPC(s.Context(), err) {
			s.NoError(err)
			return
		}
		s.NoError(err)
	}, 60*time.Second, 500*time.Millisecond)

	// Wait for propagation to complete since we have tests using async entity workflows to set the current version
	env.waitForDeploymentDataPropagationQueryWorkerDeployment(tv)
	env.waitForDeploymentDataPropagation(tv, versionStatusDraining, false, tqTypeWf)
}

func (env *VersioningTestEnv) setRampingDeployment(
	tv *testvars.TestVars,
	percentage float32,
	rampUnversioned bool,
) {
	s := env.versioningTestContext
	ctx, cancel := context.WithTimeout(s.Context(), 60*time.Second)
	defer cancel()

	bid := tv.BuildID()
	if rampUnversioned {
		bid = ""
	}
	failedPrecondition := serviceerror.NewFailedPreconditionf(workerdeployment.ErrRampingVersionDoesNotHaveAllTaskQueues, tv.DeploymentVersionStringV32()).Error()

	s.Await(func(s *versioningTestContext) {
		req := &workflowservice.SetWorkerDeploymentRampingVersionRequest{
			Namespace:      env.Namespace().String(),
			DeploymentName: tv.DeploymentSeries(),
			Percentage:     percentage,
		}
		req.BuildId = bid
		_, err := env.FrontendClient().SetWorkerDeploymentRampingVersion(ctx, req)
		if env.shouldRetryWorkerDeploymentRPC(s.Context(), err, failedPrecondition) {
			s.NoError(err)
			return
		}
		s.NoError(err)
	}, 60*time.Second, 500*time.Millisecond)

	// Wait for propagation to complete since we have tests using async entity workflows to set the current version
	env.waitForDeploymentDataPropagationQueryWorkerDeployment(tv)
}

func (env *VersioningTestEnv) waitForDeploymentDataPropagationQueryWorkerDeployment(tv *testvars.TestVars) {
	s := env.versioningTestContext
	if versioning3DeploymentWorkflowVersion == workerdeployment.AsyncSetCurrentAndRamping {
		s.Await(func(s *versioningTestContext) {
			resp, err := env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
				Namespace:      env.Namespace().String(),
				DeploymentName: tv.DeploymentSeries(),
			})
			if env.shouldRetryWorkerDeploymentRPC(s.Context(), err) {
				s.NoError(err)
				return
			}
			s.NoError(err)
			s.Equal(enumspb.ROUTING_CONFIG_UPDATE_STATE_COMPLETED, resp.GetWorkerDeploymentInfo().GetRoutingConfigUpdateState())
		}, 10*time.Second, 500*time.Millisecond)
	}
}

func (env *VersioningTestEnv) shouldRetryWorkerDeploymentRPC(ctx context.Context, err error, retryableMessages ...string) bool {
	if err == nil || ctx.Err() != nil {
		return false
	}
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) || errors.Is(err, context.DeadlineExceeded) || common.IsRetryableRPCError(err) {
		return true
	}
	errMsg := err.Error()
	for _, msg := range retryableMessages {
		if strings.Contains(errMsg, msg) {
			return true
		}
	}
	return false
}

func (env *VersioningTestEnv) updateTaskQueueDeploymentData(
	tv *testvars.TestVars,
	isCurrent bool,
	ramp float32,
	rampUnversioned bool,
	timeSinceUpdate time.Duration,
	tqTypes ...enumspb.TaskQueueType,
) {
	env.syncTaskQueueDeploymentData(tv, isCurrent, ramp, rampUnversioned, time.Now().Add(-timeSinceUpdate), tqTypes...)
	var status versionStatus
	if isCurrent {
		status = versionStatusCurrent
	} else if ramp > 0 {
		status = versionStatusRamping
	} else {
		status = versionStatusInactive
	}
	if rampUnversioned {
		status = versionStatusNil
	}

	env.waitForDeploymentDataPropagation(tv, status, rampUnversioned, tqTypes...)

}

// updateTaskQueueDeploymentDataWithRoutingConfig updates the deployment data for the requested TQ types
// and also waits for the data to propagate to all the relevant partitions.
// TODO (Shivam): Update the name of this one.
func (env *VersioningTestEnv) updateTaskQueueDeploymentDataWithRoutingConfig(
	tv *testvars.TestVars,
	routingConfig *deploymentpb.RoutingConfig,
	upsertVersions map[string]*deploymentspb.WorkerDeploymentVersionData,
	forgetVersions []string,
	tqTypes ...enumspb.TaskQueueType,
) {
	env.syncTaskQueueDeploymentDataWithRoutingConfig(tv, routingConfig, upsertVersions, forgetVersions, tqTypes...)

	// We need to know what the status of the version we are adding/forgetting is so that we can wait for it to propagate.
	for _, version := range upsertVersions {
		if version.GetStatus() == enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT {
			env.waitForDeploymentDataPropagation(tv, versionStatusCurrent, false, tqTypes...)
		} else if version.GetStatus() == enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING {
			env.waitForDeploymentDataPropagation(tv, versionStatusRamping, false, tqTypes...)
		} else if version.GetStatus() == enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE {
			env.waitForDeploymentDataPropagation(tv, versionStatusInactive, false, tqTypes...)
		}
	}
}

// getTaskQueueDeploymentData gets the deployment data for a given TQ type. The data is always
// returned from the WF type root partition, so no need to wait for propagation before calling this
// function.
func (env *VersioningTestEnv) getTaskQueueDeploymentData(
	tv *testvars.TestVars,
	tqType enumspb.TaskQueueType,
) *persistencespb.DeploymentData {
	s := env.versioningTestContext
	ctx, cancel := context.WithTimeout(s.Context(), time.Second*5)
	defer cancel()

	resp, err := env.GetTestCluster().MatchingClient().GetTaskQueueUserData(
		ctx, &matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:   env.NamespaceID().String(),
			TaskQueue:     tv.TaskQueue().GetName(),
			TaskQueueType: tqTypeWf,
		})
	s.NoError(err)
	return resp.GetUserData().GetData().GetPerType()[int32(tqType)].GetDeploymentData()
}

func (env *VersioningTestEnv) syncTaskQueueDeploymentDataWithRoutingConfig(
	tv *testvars.TestVars,
	routingConfig *deploymentpb.RoutingConfig,
	upsertVersions map[string]*deploymentspb.WorkerDeploymentVersionData,
	forgetVersions []string,
	t ...enumspb.TaskQueueType,
) {
	s := env.versioningTestContext
	ctx, cancel := context.WithTimeout(s.Context(), time.Second*5)
	defer cancel()

	deploymentName := tv.DeploymentVersion().GetDeploymentName()
	var err error

	_, err = env.GetTestCluster().MatchingClient().SyncDeploymentUserData(
		ctx, &matchingservice.SyncDeploymentUserDataRequest{
			NamespaceId:         env.NamespaceID().String(),
			TaskQueue:           tv.TaskQueue().GetName(),
			TaskQueueTypes:      t,
			DeploymentName:      deploymentName,
			UpdateRoutingConfig: routingConfig,
			UpsertVersionsData:  upsertVersions,
			ForgetVersions:      forgetVersions,
		})

	s.NoError(err)
}

// rollbackTaskQueueToVersion simulates routing config lag by rolling back the task queue user data
// to an older version with revision number 0. This is used to test that workflows correctly use
// inherited revision numbers instead of falling back to the (stale) current task queue version.
func (env *VersioningTestEnv) rollbackTaskQueueToVersion(
	tv *testvars.TestVars,
) {
	s := env.versioningTestContext

	cleanup := env.InjectHook(testhooks.NewHook(testhooks.MatchingIgnoreRoutingConfigRevisionCheck, true))
	defer cleanup()

	rc := &deploymentpb.RoutingConfig{
		CurrentDeploymentVersion:  worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tv.DeploymentVersionString()),
		CurrentVersionChangedTime: timestamp.TimePtr(time.Now().Add(1 * time.Minute)),
		RevisionNumber:            0,
	}
	env.syncTaskQueueDeploymentDataWithRoutingConfig(tv, rc, map[string]*deploymentspb.WorkerDeploymentVersionData{tv.DeploymentVersion().GetBuildId(): {
		Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
	}}, nil, tqTypeWf)

	// Verify that the rollback propagated to all partitions
	s.Await(func(s *versioningTestContext) {
		ms, err := env.GetTestCluster().MatchingClient().GetTaskQueueUserData(s.Context(), &matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:   env.NamespaceID().String(),
			TaskQueue:     tv.TaskQueue().GetName(),
			TaskQueueType: tqTypeWf,
		})
		s.NoError(err)
		current, currentRevisionNumber, _, _, _, _, _, _ := worker_versioning.CalculateTaskQueueVersioningInfo(ms.GetUserData().GetData().GetPerType()[int32(tqTypeWf)].GetDeploymentData())
		s.Equal(tv.DeploymentVersion().GetBuildId(), current.GetBuildId())
		s.Equal(int64(0), currentRevisionNumber)
	}, 10*time.Second, 500*time.Millisecond)
}

func (env *VersioningTestEnv) syncTaskQueueDeploymentData(
	tv *testvars.TestVars,
	isCurrent bool,
	ramp float32,
	rampUnversioned bool,
	updateTime time.Time,
	t ...enumspb.TaskQueueType,
) {
	s := env.versioningTestContext
	ctx, cancel := context.WithTimeout(s.Context(), time.Second*5)
	defer cancel()

	v := tv.DeploymentVersion()
	if rampUnversioned {
		v = nil
	}

	routingUpdateTime := timestamp.TimePtr(updateTime)
	var currentSinceTime, rampingSinceTime *timestamppb.Timestamp
	if isCurrent {
		currentSinceTime = routingUpdateTime
	}
	if ramp > 0 { // todo carly / shahab: this doesn't account for setting 0 ramp, or for changing the ramp while ramping_since_time stays the same.
		rampingSinceTime = routingUpdateTime
	}

	_, err := env.GetTestCluster().MatchingClient().SyncDeploymentUserData(
		ctx, &matchingservice.SyncDeploymentUserDataRequest{
			NamespaceId:    env.NamespaceID().String(),
			TaskQueue:      tv.TaskQueue().GetName(),
			TaskQueueTypes: t,
			Operation: &matchingservice.SyncDeploymentUserDataRequest_UpdateVersionData{
				UpdateVersionData: &deploymentspb.DeploymentVersionData{
					Version:           v,
					RoutingUpdateTime: routingUpdateTime,
					CurrentSinceTime:  currentSinceTime,
					RampingSinceTime:  rampingSinceTime,
					RampPercentage:    ramp,
				},
			},
		})
	s.NoError(err)
}

func (env *VersioningTestEnv) forgetDeploymentVersionsFromDeploymentData(
	tv *testvars.TestVars,
	deploymentName string,
	forgetUnversionedRamp bool,
	revisionNumber int64,
	t ...enumspb.TaskQueueType,
) {
	s := env.versioningTestContext
	ctx, cancel := context.WithTimeout(s.Context(), time.Second*5)
	defer cancel()

	v := tv.DeploymentVersion()
	if forgetUnversionedRamp {
		v.BuildId = ""
	}
	_, err := env.GetTestCluster().MatchingClient().SyncDeploymentUserData(
		ctx, &matchingservice.SyncDeploymentUserDataRequest{
			NamespaceId:    env.NamespaceID().String(),
			TaskQueue:      tv.TaskQueue().GetName(),
			TaskQueueTypes: t,
			DeploymentName: deploymentName,
			ForgetVersions: []string{tv.BuildID()},
		})
	s.NoError(err)
}

func (env *VersioningTestEnv) forgetTaskQueueDeploymentVersion(
	tv *testvars.TestVars,
	t enumspb.TaskQueueType,
	forgetUnversionedRamp bool,
) {
	s := env.versioningTestContext
	ctx, cancel := context.WithTimeout(s.Context(), time.Second*5)
	defer cancel()

	v := tv.DeploymentVersion()
	if forgetUnversionedRamp {
		v.BuildId = ""
	}
	_, err := env.GetTestCluster().MatchingClient().SyncDeploymentUserData(
		ctx, &matchingservice.SyncDeploymentUserDataRequest{
			NamespaceId:    env.NamespaceID().String(),
			TaskQueue:      tv.TaskQueue().GetName(),
			TaskQueueTypes: []enumspb.TaskQueueType{t},
			Operation: &matchingservice.SyncDeploymentUserDataRequest_ForgetVersion{
				ForgetVersion: v,
			},
		})
	s.NoError(err)
}

func (env *VersioningTestEnv) verifyWorkflowVersioning(tv *testvars.TestVars,
	behavior enumspb.VersioningBehavior,
	deployment *deploymentpb.Deployment,
	override *workflowpb.VersioningOverride,
	transition *workflowpb.DeploymentVersionTransition,
) {
	s := env.versioningTestContext
	dwf, err := env.FrontendClient().DescribeWorkflowExecution(
		s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: tv.WorkflowID(),
			},
		})
	s.NoError(err)

	versioningInfo := dwf.WorkflowExecutionInfo.GetVersioningInfo()
	s.Equal(behavior.String(), versioningInfo.GetBehavior().String())
	var v *deploymentspb.WorkerDeploymentVersion
	if versioningInfo.GetVersion() != "" { //nolint:staticcheck // SA1019: worker versioning v0.31
		//nolint:staticcheck // SA1019: worker versioning v0.31
		v, err = worker_versioning.WorkerDeploymentVersionFromStringV31(versioningInfo.GetVersion())
		s.NoError(err)
		s.NotNil(versioningInfo.GetDeploymentVersion()) // make sure we are always populating this whenever Version string is populated
	}
	if dv := versioningInfo.GetDeploymentVersion(); dv != nil {
		v = worker_versioning.DeploymentVersionFromDeployment(worker_versioning.DeploymentFromExternalDeploymentVersion(dv))
	}
	actualDeployment := worker_versioning.DeploymentFromDeploymentVersion(v)
	if !deployment.Equal(actualDeployment) {
		s.Fail(fmt.Sprintf("deployment version mismatch. expected: {%s}, actual: {%s}",
			deployment,
			actualDeployment,
		))
	}

	// v0.32 override
	s.Equal(override.GetAutoUpgrade(), versioningInfo.GetVersioningOverride().GetAutoUpgrade())
	s.Equal(override.GetPinned().GetVersion().GetBuildId(), versioningInfo.GetVersioningOverride().GetPinned().GetVersion().GetBuildId())
	s.Equal(override.GetPinned().GetVersion().GetDeploymentName(), versioningInfo.GetVersioningOverride().GetPinned().GetVersion().GetDeploymentName())
	s.Equal(override.GetPinned().GetBehavior(), versioningInfo.GetVersioningOverride().GetPinned().GetBehavior())
	if worker_versioning.OverrideIsPinned(override) {
		s.Equal(override.GetPinned().GetVersion().GetDeploymentName(), dwf.WorkflowExecutionInfo.GetWorkerDeploymentName())
	}

	if !versioningInfo.GetVersionTransition().Equal(transition) {
		s.Fail(fmt.Sprintf("version transition mismatch. expected: {%s}, actual: {%s}",
			transition,
			versioningInfo.GetVersionTransition(),
		))
	}
}

func (env *VersioningTestEnv) startWorkflow(
	tv *testvars.TestVars,
	override *workflowpb.VersioningOverride,
) string {
	s := env.versioningTestContext
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          tv.Any().String(),
		Namespace:          env.Namespace().String(),
		WorkflowId:         tv.WorkflowID(),
		WorkflowType:       tv.WorkflowType(),
		TaskQueue:          tv.TaskQueue(),
		Identity:           tv.WorkerIdentity(),
		VersioningOverride: override,
	}

	we, err0 := env.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)
	return we.GetRunId()
}

func (env *VersioningTestEnv) queryWorkflow(
	tv *testvars.TestVars,
) (*workflowservice.QueryWorkflowResponse, error) {
	request := &workflowservice.QueryWorkflowRequest{
		Namespace: env.Namespace().String(),
		Execution: tv.WorkflowExecution(),
		Query:     tv.Query(),
	}

	shortCtx, cancel := context.WithTimeout(testcore.NewContext(), common.MinLongPollTimeout)
	defer cancel()
	response, err := env.FrontendClient().QueryWorkflow(shortCtx, request)
	return response, err
}

// pollWftAndHandle can be used in sync and async mode. For async mode pass the async channel. It
// will be closed when the task is handled.
// Returns the poller and poll response only in sync mode (can be used to process new wft in the response)
func (env *VersioningTestEnv) pollWftAndHandle(
	tv *testvars.TestVars,
	sticky bool,
	async chan<- struct{},
	handler func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error),
) (*taskpoller.TaskPoller, *workflowservice.RespondWorkflowTaskCompletedResponse) {
	return env.doPollWftAndHandle(tv, true, sticky, async, handler)
}

func (env *VersioningTestEnv) unversionedPollWftAndHandle(
	tv *testvars.TestVars,
	sticky bool,
	async chan<- struct{},
	handler func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error),
) (*taskpoller.TaskPoller, *workflowservice.RespondWorkflowTaskCompletedResponse) {
	return env.doPollWftAndHandle(tv, false, sticky, async, handler)
}

// doPollWftAndHandle can be used in sync and async mode. For async mode pass the async channel. It
// will be closed when the task is handled.
// Returns the poller and poll response only in sync mode (can be used to process new wft in the response)
func (env *VersioningTestEnv) doPollWftAndHandle(
	tv *testvars.TestVars,
	versioned bool,
	sticky bool,
	async chan<- struct{},
	handler func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error),
) (*taskpoller.TaskPoller, *workflowservice.RespondWorkflowTaskCompletedResponse) {
	s := env.versioningTestContext
	poller := taskpoller.New(s.T(), env.FrontendClient(), env.Namespace().String())
	f := func() (*workflowservice.RespondWorkflowTaskCompletedResponse, error) {
		tq := tv.TaskQueue()
		if sticky {
			tq = tv.StickyTaskQueue()
		}
		return poller.PollWorkflowTask(
			&workflowservice.PollWorkflowTaskQueueRequest{
				DeploymentOptions: tv.WorkerDeploymentOptions(versioned),
				TaskQueue:         tq,
			},
		).HandleTask(tv, handler, taskpoller.WithTimeout(time.Minute))
	}
	if async == nil {
		resp, err := f()
		s.NoError(err)
		return poller, resp
	}
	go func() {
		_, _ = f() // errors are surfaced via test context timeout on WaitForChannel
		close(async)
	}()
	return nil, nil
}

func (env *VersioningTestEnv) pollWftAndHandleQueries(
	tv *testvars.TestVars,
	sticky bool,
	async chan<- any,
	handler func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondQueryTaskCompletedRequest, error),
) (*taskpoller.TaskPoller, *workflowservice.RespondQueryTaskCompletedResponse) {
	s := env.versioningTestContext
	poller := taskpoller.New(s.T(), env.FrontendClient(), env.Namespace().String())
	f := func() (*workflowservice.RespondQueryTaskCompletedResponse, error) {
		tq := tv.TaskQueue()
		if sticky {
			tq = tv.StickyTaskQueue()
		}
		return poller.PollWorkflowTask(
			&workflowservice.PollWorkflowTaskQueueRequest{
				DeploymentOptions: tv.WorkerDeploymentOptions(true),
				TaskQueue:         tq,
			},
		).HandleLegacyQuery(tv, handler)
	}
	if async == nil {
		resp, err := f()
		s.NoError(err)
		return poller, resp
	}
	go func() {
		_, _ = f() // errors are surfaced via test context timeout on WaitForChannel
		close(async)
	}()
	return nil, nil
}

func (env *VersioningTestEnv) pollNexusTaskAndHandle(
	tv *testvars.TestVars,
	sticky bool,
	async chan<- any,
	handler func(task *workflowservice.PollNexusTaskQueueResponse) (*workflowservice.RespondNexusTaskCompletedRequest, error),
) (*taskpoller.TaskPoller, *workflowservice.RespondNexusTaskCompletedResponse) {
	s := env.versioningTestContext
	poller := taskpoller.New(s.T(), env.FrontendClient(), env.Namespace().String())
	f := func() (*workflowservice.RespondNexusTaskCompletedResponse, error) {
		tq := tv.TaskQueue()
		if sticky {
			tq = tv.StickyTaskQueue()
		}
		return poller.PollNexusTask(
			&workflowservice.PollNexusTaskQueueRequest{
				DeploymentOptions: tv.WorkerDeploymentOptions(true),
				TaskQueue:         tq,
			},
		).HandleTask(tv, handler, taskpoller.WithTimeout(10*time.Second))
	}
	if async == nil {
		resp, err := f()
		s.NoError(err)
		return poller, resp
	}
	go func() {
		_, _ = f() // errors are surfaced via test context timeout on WaitForChannel
		close(async)
	}()
	return nil, nil
}

func (env *VersioningTestEnv) unversionedPollActivityAndHandle(
	tv *testvars.TestVars,
	async chan<- struct{},
	handler func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error),
) {
	env.doPollActivityAndHandle(tv, false, async, handler)
}

func (env *VersioningTestEnv) pollActivityAndHandle(
	tv *testvars.TestVars,
	async chan<- struct{},
	handler func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error),
) {
	env.doPollActivityAndHandle(tv, true, async, handler)
}

func (env *VersioningTestEnv) pollActivityAndHandleErr(
	tv *testvars.TestVars,
	handler func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error),
) error {
	return env.doPollActivityAndHandleErr(tv, true, handler)
}

func (env *VersioningTestEnv) doPollActivityAndHandle(
	tv *testvars.TestVars,
	versioned bool,
	async chan<- struct{},
	handler func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error),
) {
	s := env.versioningTestContext
	f := func() error {
		return env.doPollActivityAndHandleErr(tv, versioned, handler)
	}
	if async == nil {
		s.NoError(f())
	} else {
		go func() {
			_ = f() // errors are surfaced via test context timeout on WaitForChannel
			close(async)
		}()
	}
}

func (env *VersioningTestEnv) doPollActivityAndHandleErr(
	tv *testvars.TestVars,
	versioned bool,
	handler func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error),
) error {
	s := env.versioningTestContext
	poller := taskpoller.New(s.T(), env.FrontendClient(), env.Namespace().String())
	_, err := poller.PollActivityTask(
		&workflowservice.PollActivityTaskQueueRequest{
			DeploymentOptions: tv.WorkerDeploymentOptions(versioned),
		}).HandleTask(tv, handler, taskpoller.WithTimeout(time.Minute))
	return err
}

//nolint:revive // Polling helpers consistently take the environment assertion context first.
func (env *VersioningTestEnv) idlePollWorkflow(
	ctx context.Context,
	tv *testvars.TestVars,
	versioned bool,
	timeout time.Duration,
	unexpectedTaskMessage string,
) {
	s := env.versioningTestContext
	poller := taskpoller.New(s.T(), env.FrontendClient(), env.Namespace().String())
	_, _ = poller.PollWorkflowTask(
		&workflowservice.PollWorkflowTaskQueueRequest{
			DeploymentOptions: tv.WorkerDeploymentOptions(versioned),
		}).HandleTask(
		tv,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.T().Error(unexpectedTaskMessage)
			return nil, nil
		},
		taskpoller.WithTimeout(timeout),
		taskpoller.WithContext(ctx),
	)
}

func (env *VersioningTestEnv) idlePollUnversionedActivity(
	tv *testvars.TestVars,
	timeout time.Duration,
	unexpectedTaskMessage string,
) {
	s := env.versioningTestContext
	poller := taskpoller.New(s.T(), env.FrontendClient(), env.Namespace().String())
	_, _ = poller.PollActivityTask(
		&workflowservice.PollActivityTaskQueueRequest{}).HandleTask(
		tv,
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			if task != nil {
				env.Logger.Error(fmt.Sprintf("Unexpected activity task received, ID: %s", task.ActivityId))
				s.Fail(unexpectedTaskMessage)
			}
			return nil, nil
		},
		taskpoller.WithTimeout(timeout),
	)
}

func (env *VersioningTestEnv) idlePollActivity(
	tv *testvars.TestVars,
	versioned bool,
	timeout time.Duration,
	unexpectedTaskMessage string,
) {
	s := env.versioningTestContext
	poller := taskpoller.New(s.T(), env.FrontendClient(), env.Namespace().String())
	_, _ = poller.PollActivityTask(
		&workflowservice.PollActivityTaskQueueRequest{
			DeploymentOptions: tv.WorkerDeploymentOptions(versioned),
		}).HandleTask(
		tv,
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			if task != nil {
				env.Logger.Error(fmt.Sprintf("Unexpected activity task received, ID: %s", task.ActivityId))
				s.T().Error(unexpectedTaskMessage)
			}
			return nil, nil
		},
		taskpoller.WithTimeout(timeout),
		taskpoller.WithContext(s.Context()),
	)
}

//nolint:revive // Polling helpers consistently take the environment assertion context first.
func (env *VersioningTestEnv) idlePollNexus(
	ctx context.Context,
	tv *testvars.TestVars,
	versioned bool,
	timeout time.Duration,
	unexpectedTaskMessage string,
) {
	s := env.versioningTestContext
	poller := taskpoller.New(s.T(), env.FrontendClient(), env.Namespace().String())
	_, _ = poller.PollNexusTask(
		&workflowservice.PollNexusTaskQueueRequest{
			DeploymentOptions: tv.WorkerDeploymentOptions(versioned),
		}).HandleTask(
		tv,
		func(task *workflowservice.PollNexusTaskQueueResponse) (*workflowservice.RespondNexusTaskCompletedRequest, error) {
			if task != nil {
				s.T().Error(unexpectedTaskMessage)
			}
			return nil, nil
		},
		taskpoller.WithTimeout(timeout),
		taskpoller.WithContext(ctx),
	)
}

func (env *VersioningTestEnv) verifyWorkflowStickyQueue(
	tv *testvars.TestVars,
) {
	s := env.versioningTestContext
	ms, err := env.GetTestCluster().HistoryClient().GetMutableState(
		s.Context(), &historyservice.GetMutableStateRequest{
			NamespaceId: env.NamespaceID().String(),
			Execution:   tv.WorkflowExecution(),
		})
	s.NoError(err)
	s.Equal(tv.StickyTaskQueue().GetName(), ms.StickyTaskQueue.GetName())
}

// Sticky queue needs to be created in server before tasks can schedule in it. Call to this method
// create the sticky queue by polling it.
func (env *VersioningTestEnv) warmUpSticky(
	tv *testvars.TestVars,
) {
	s := env.versioningTestContext
	poller := taskpoller.New(s.T(), env.FrontendClient(), env.Namespace().String())
	_, _ = poller.PollWorkflowTask(
		&workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: tv.StickyTaskQueue(),
		}).HandleTask(
		tv,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.Fail("sticky task is not expected")
			return nil, nil
		},
		taskpoller.WithTimeout(ver3MinPollTime),
	)
}

// TODO (Shivam): Clean up this function once sync entity workflows have been removed.
func (env *VersioningTestEnv) waitForDeploymentDataPropagation(
	tv *testvars.TestVars,
	status versionStatus,
	unversionedRamp bool,
	tqTypes ...enumspb.TaskQueueType,
) {
	s := env.versioningTestContext
	v := env.GetTestCluster().Host().DcClient().GetValue(dynamicconfig.MatchingNumTaskqueueReadPartitions.Key())
	s.NotEmpty(v, "versioning tests require setting explicit number of partitions")
	count, ok := v[0].Value.(int)
	s.True(ok, "partition count is not an int")
	partitionCount := count

	type partAndType struct {
		part int
		tp   enumspb.TaskQueueType
	}
	remaining := make(map[partAndType]struct{})
	for i := range partitionCount {
		for _, tqt := range tqTypes {
			remaining[partAndType{i, tqt}] = struct{}{}
		}
	}
	f, err := tqid.NewTaskQueueFamily(env.NamespaceID().String(), tv.TaskQueue().GetName())
	s.Await(func(s *versioningTestContext) {
		for pt := range remaining {
			s.NoError(err)
			partition := f.TaskQueue(pt.tp).NormalPartition(pt.part)
			// Use lower-level GetTaskQueueUserData instead of GetWorkerBuildIdCompatibility
			// here so that we can target activity queues.
			res, err := env.GetTestCluster().MatchingClient().GetTaskQueueUserData(
				s.Context(),
				&matchingservice.GetTaskQueueUserDataRequest{
					NamespaceId:   env.NamespaceID().String(),
					TaskQueue:     partition.RpcName(),
					TaskQueueType: partition.TaskType(),
				})
			s.NoError(err)
			perTypes := res.GetUserData().GetData().GetPerType()
			if perTypes != nil {
				deploymentsData := perTypes[int32(pt.tp)].GetDeploymentData().GetDeploymentsData()
				workerDeploymentData := deploymentsData[tv.DeploymentVersion().GetDeploymentName()]

				if unversionedRamp {
					if perTypes[int32(pt.tp)].GetDeploymentData().GetUnversionedRampData() != nil { //nolint:staticcheck // SA1019: legacy deployment data remains part of the compatibility check
						delete(remaining, pt)
					}

					// Using the new internal task-queue persistence schema that we have now defined.
					if workerDeploymentData != nil {
						routingConfig := workerDeploymentData.GetRoutingConfig()
						if routingConfig.GetRampingDeploymentVersion() == nil && routingConfig.GetRampingVersionPercentage() > 0 {
							delete(remaining, pt)
						}
					}
					continue
				}
				versions := perTypes[int32(pt.tp)].GetDeploymentData().GetVersions() //nolint:staticcheck // SA1019: legacy deployment data remains part of the compatibility check
				for _, d := range versions {
					if d.GetVersion().Equal(tv.DeploymentVersion()) {
						switch status { //nolint:revive,exhaustive // Legacy statuses outside these cases require no propagation check.
						case versionStatusInactive:
							if d.GetRampingSinceTime() == nil && d.GetCurrentSinceTime() == nil {
								delete(remaining, pt)
							}
						case versionStatusRamping:
							if d.GetRampingSinceTime() != nil {
								delete(remaining, pt)
							}
						case versionStatusCurrent:
							if d.GetCurrentSinceTime() != nil {
								delete(remaining, pt)
							}
						}
					}
				}

				// Using the new internal task-queue persistence schema that we have now defined.
				if workerDeploymentData != nil {
					versions := workerDeploymentData.GetVersions()
					for buildID, versionData := range versions {
						if buildID == tv.DeploymentVersion().GetBuildId() && status == versionStatus(versionData.GetStatus()) {
							delete(remaining, pt)
						}
					}
				}
			}
		}
		s.Empty(remaining)
	}, 30*time.Second, 500*time.Millisecond)
}

func (env *VersioningTestEnv) validateBacklogCount(
	tv *testvars.TestVars,
	tqType enumspb.TaskQueueType,
	expectedCount int64,
) {
	s := env.versioningTestContext
	ctx, cancel := context.WithTimeout(s.Context(), 10*time.Second)
	defer cancel()

	var resp *workflowservice.DescribeTaskQueueResponse
	var err error

	s.Await(func(s *versioningTestContext) {
		resp, err = env.FrontendClient().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
			Namespace:     env.Namespace().String(),
			TaskQueue:     tv.TaskQueue(),
			TaskQueueType: tqType,
			ReportStats:   true,
		})
		s.NoError(err)
		s.NotNil(resp)
		priorityStats, ok := resp.GetStatsByPriorityKey()[3]
		s.True(ok)
		s.Equal(expectedCount, priorityStats.GetApproximateBacklogCount())
	}, 6*time.Second, 500*time.Millisecond)
}

func (env *VersioningTestEnv) verifyVersioningSAs(
	tv *testvars.TestVars,
	behavior enumspb.VersioningBehavior,
	executionStatus enumspb.WorkflowExecutionStatus,
	usedBuilds ...*testvars.TestVars,
) {
	s := env.versioningTestContext
	ctx, cancel := context.WithTimeout(s.Context(), 10*time.Second)
	defer cancel()

	s.Await(func(s *versioningTestContext) {
		var query string
		if behavior != vbUnspecified {
			query = fmt.Sprintf("WorkflowId = '%s' AND TemporalWorkerDeployment = '%s' AND TemporalWorkerDeploymentVersion= '%s' AND TemporalWorkflowVersioningBehavior = '%s' AND ExecutionStatus = '%s'",
				tv.WorkflowID(), tv.DeploymentSeries(), tv.DeploymentVersionStringV32(), behavior.String(), executionStatus)
		} else {
			query = fmt.Sprintf("WorkflowId = '%s' AND TemporalWorkerDeploymentVersion is null AND TemporalWorkflowVersioningBehavior is null AND ExecutionStatus = '%s'",
				tv.WorkflowID(), executionStatus)
		}
		resp, err := env.FrontendClient().ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: env.Namespace().String(),
			Query:     query,
		})
		s.NoError(err)
		s.NotEmpty(resp.GetExecutions())
		if len(resp.GetExecutions()) > 0 {
			w := resp.GetExecutions()[0]
			if behavior == vbPinned {
				payload, ok := w.GetSearchAttributes().GetIndexedFields()["BuildIds"]
				s.True(ok)
				searchAttrAny, err := sadefs.DecodeValue(payload, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, false)
				s.NoError(err)
				var searchAttr []string
				if searchAttrAny != nil {
					searchAttr = searchAttrAny.([]string)
				}
				if behavior == enumspb.VERSIONING_BEHAVIOR_PINNED {
					s.Contains(searchAttr, worker_versioning.PinnedBuildIdSearchAttribute(tv.DeploymentVersionStringV32()))
				}
			}

			if len(usedBuilds) > 0 {
				// Validate TemporalUsedWorkerDeploymentVersions search attribute
				versionPayload, ok := w.GetSearchAttributes().GetIndexedFields()["TemporalUsedWorkerDeploymentVersions"]
				s.True(ok)
				versionAttrAny, err := sadefs.DecodeValue(versionPayload, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, false)
				s.NoError(err)
				var versionAttr []string
				if versionAttrAny != nil {
					versionAttr = versionAttrAny.([]string)
				}
				for _, b := range usedBuilds {
					s.Contains(versionAttr, b.DeploymentVersionStringV32())
				}
			}

			fmt.Println(resp.GetExecutions()[0])
		}
	}, 5*time.Second, 50*time.Millisecond)
}

// validatePinnedVersionExistsInTaskQueue validates that the version, to be pinned, exists in the task queue.
// TODO (future improvement): This can be further extended to validate the presence of any version instead of using the GetTaskQueueUserData RPC.
func (env *VersioningTestEnv) validatePinnedVersionExistsInTaskQueue(tv *testvars.TestVars) {
	s := env.versioningTestContext
	s.Await(func(s *versioningTestContext) {
		resp, err := env.GetTestCluster().MatchingClient().CheckTaskQueueVersionMembership(s.Context(), &matchingservice.CheckTaskQueueVersionMembershipRequest{
			NamespaceId:   env.NamespaceID().String(),
			TaskQueue:     tv.TaskQueue().GetName(),
			TaskQueueType: tqTypeWf,
			Version:       worker_versioning.DeploymentVersionFromDeployment(tv.Deployment()),
		})
		s.NoError(err)
		s.True(resp.GetIsMember())
	}, 10*time.Second, 500*time.Millisecond)

}

func (env *VersioningTestEnv) startChildWorkflowCommand(tv *testvars.TestVars) *commandpb.Command {
	attributes := &commandpb.StartChildWorkflowExecutionCommandAttributes{
		WorkflowId:   tv.WorkflowID(),
		WorkflowType: tv.WorkflowType(),
		TaskQueue:    tv.TaskQueue(),
		Input:        tv.Any().Payloads(),
	}

	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{
			StartChildWorkflowExecutionCommandAttributes: attributes,
		},
	}
}

func (env *VersioningTestEnv) respondActivity() *workflowservice.RespondActivityTaskCompletedRequest {
	return &workflowservice.RespondActivityTaskCompletedRequest{}
}

func (env *VersioningTestEnv) respondWftWithActivities(
	tvWf *testvars.TestVars,
	tvAct *testvars.TestVars,
	sticky bool,
	behavior enumspb.VersioningBehavior,
	activityIds ...string,
) *workflowservice.RespondWorkflowTaskCompletedRequest {
	var stickyAttr *taskqueuepb.StickyExecutionAttributes
	if sticky {
		stickyAttr = &taskqueuepb.StickyExecutionAttributes{
			WorkerTaskQueue:        tvWf.StickyTaskQueue(),
			ScheduleToStartTimeout: durationpb.New(5 * time.Second),
		}
	}
	var commands []*commandpb.Command
	for _, a := range activityIds {
		commands = append(commands, &commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
			Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
				ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:   a,
					ActivityType: tvAct.ActivityType(),
					TaskQueue:    tvAct.TaskQueue(),
					Input:        tvAct.Any().Payloads(),
					// TODO(carlydf): tests with forced task forward take multiple seconds. Need to know why?
					ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
					ScheduleToStartTimeout: durationpb.New(10 * time.Second),
					StartToCloseTimeout:    durationpb.New(3 * time.Second),
					HeartbeatTimeout:       durationpb.New(3 * time.Second),
					RequestEagerExecution:  false,
				},
			},
		})
	}
	versioningMode := enumspb.WORKER_VERSIONING_MODE_VERSIONED
	if behavior == vbUnspecified {
		versioningMode = enumspb.WORKER_VERSIONING_MODE_UNVERSIONED
	}
	return &workflowservice.RespondWorkflowTaskCompletedRequest{
		Commands:                   commands,
		StickyAttributes:           stickyAttr,
		ForceCreateNewWorkflowTask: false,
		VersioningBehavior:         behavior,
		DeploymentOptions: &deploymentpb.WorkerDeploymentOptions{
			BuildId:              tvWf.BuildID(),
			DeploymentName:       tvWf.DeploymentSeries(),
			WorkerVersioningMode: versioningMode,
		},
		// TODO(carlydf): remove stamp once build ID is added to wftc event
		WorkerVersionStamp: &commonpb.WorkerVersionStamp{ //nolint:staticcheck // SA1019: worker versioning v0.20
			BuildId:       tvWf.BuildID(),
			UseVersioning: versioningMode == enumspb.WORKER_VERSIONING_MODE_VERSIONED,
		},
	}
}

func (env *VersioningTestEnv) respondEmptyWft(
	tv *testvars.TestVars,
	sticky bool,
	behavior enumspb.VersioningBehavior,
) *workflowservice.RespondWorkflowTaskCompletedRequest {
	return env.respondWftWithActivities(tv, tv, sticky, behavior)
}

func (env *VersioningTestEnv) respondCompleteWorkflow(
	tv *testvars.TestVars,
	behavior enumspb.VersioningBehavior,
) *workflowservice.RespondWorkflowTaskCompletedRequest {
	return &workflowservice.RespondWorkflowTaskCompletedRequest{
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: tv.Any().Payloads(),
					},
				},
			},
		},
		ForceCreateNewWorkflowTask: false,
		VersioningBehavior:         behavior,
		DeploymentOptions: &deploymentpb.WorkerDeploymentOptions{
			BuildId:              tv.BuildID(),
			DeploymentName:       tv.DeploymentSeries(),
			WorkerVersioningMode: enumspb.WORKER_VERSIONING_MODE_VERSIONED,
		},
	}
}

func (env *VersioningTestEnv) respondCompleteWorkflowUnversioned(
	tv *testvars.TestVars,
) *workflowservice.RespondWorkflowTaskCompletedRequest {
	return &workflowservice.RespondWorkflowTaskCompletedRequest{
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: tv.Any().Payloads(),
					},
				},
			},
		},
		ForceCreateNewWorkflowTask: false,
	}
}
