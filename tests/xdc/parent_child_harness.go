package xdc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

var (
	errParentChildReplicationGateClosed = errors.New("parent-child replication gate is closed")
	errParentChildTaskAlreadyResolved   = errors.New("parent-child replication task is already resolved")
)

type parentChildXDCTestSuite struct{ xdcBaseSuite }

type (
	parentChildWorkflow int

	parentChildCluster int

	parentChildReplicationTaskAction int

	parentChildScenario struct {
		steps        []parentChildScenarioStep
		expectations []parentChildExpectation
	}

	parentChildScenarioStep struct {
		name string
		run  func(context.Context, *parentChildScenarioRuntime) error
	}

	parentChildExpectation struct {
		name  string
		check func(context.Context, *parentChildScenarioRuntime) error
	}

	parentChildScenarioRuntime struct {
		suite *parentChildXDCTestSuite

		namespace      string
		namespaceID    string
		parentID       string
		parentRunID    string
		childID        string
		childRunID     string
		parentTestVars *testvars.TestVars
		childTestVars  *testvars.TestVars

		activeClusterIndex int
		gates              [2]*parentChildReplicationGate
		removeHooks        []func()
		cleanups           []func()
		delayedTasks       map[parentChildReplicationLane]*parentChildReplicationTask
		metricCaptures     [2]parentChildMetricCapture
		trace              []string
	}

	parentChildMetricCapture struct {
		handler *metricstest.CaptureHandler
		capture *metricstest.Capture
	}

	parentChildReplicationLane struct {
		targetClusterIndex int
		workflow           parentChildWorkflow
	}

	parentChildReplicationGate struct {
		namespaceID string
		workflowIDs map[string]struct{}
		pending     chan *parentChildReplicationTask
		buffered    map[string][]*parentChildReplicationTask
		stop        chan struct{}
		stopOnce    sync.Once
	}

	parentChildReplicationTask struct {
		task     *replicationspb.ReplicationTask
		metadata parentChildReplicationTaskMetadata
		execute  func() error
		result   chan error

		mu       sync.Mutex
		resolved bool
	}

	parentChildReplicationTaskMetadata struct {
		namespaceID string
		workflowID  string
		runID       string
	}
)

const (
	parentWorkflow parentChildWorkflow = iota
	childWorkflow
)

const (
	initialActiveCluster parentChildCluster = iota
	initialStandbyCluster
)

const (
	applyReplicationTask parentChildReplicationTaskAction = iota
	delayReplicationTaskApply
	ackReplicationTaskWithoutApplying
)

const (
	historyClientVerifyChildCompletion   = "HistoryClientVerifyChildExecutionCompletionRecorded"
	historyClientVerifyFirstWorkflowTask = "HistoryClientVerifyFirstWorkflowTaskScheduled"
)

func (s *parentChildXDCTestSuite) runParentChildScenario(scenario parentChildScenario) {
	runtime := newParentChildScenarioRuntime(s)
	defer runtime.close()
	defer func() {
		if s.T().Failed() {
			s.T().Logf("parent-child scenario trace:\n%s", strings.Join(runtime.trace, "\n"))
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	err := runtime.initialize(ctx)
	cancel()
	s.Require().NoError(err)

	for index, step := range scenario.steps {
		runtime.tracef("step %d: %s", index+1, step.name)
		ctx, cancel = context.WithTimeout(context.Background(), testTimeout)
		err = step.run(ctx, runtime)
		cancel()
		s.Require().NoError(err, step.name)
	}

	for index, expectation := range scenario.expectations {
		runtime.tracef("expectation %d: %s", index+1, expectation.name)
		await.Require(context.Background(), s.T(), func(t *await.T) {
			require.NoError(t, expectation.check(t.Context(), runtime))
		}, testTimeout, replicationCheckInterval)
	}
}

func newParentChildScenarioRuntime(s *parentChildXDCTestSuite) *parentChildScenarioRuntime {
	return &parentChildScenarioRuntime{
		suite:        s,
		delayedTasks: make(map[parentChildReplicationLane]*parentChildReplicationTask),
	}
}

func (r *parentChildScenarioRuntime) initialize(ctx context.Context) error {
	if len(r.suite.clusters) != len(r.gates) {
		return fmt.Errorf("parent-child scenarios require exactly %d clusters, got %d", len(r.gates), len(r.suite.clusters))
	}

	r.namespace = r.suite.createGlobalNamespace()
	nsResp, err := r.suite.clusters[0].FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: r.namespace,
	})
	if err != nil {
		return err
	}
	r.namespaceID = nsResp.GetNamespaceInfo().GetId()
	if r.suite.numHistoryShards < 2 {
		return fmt.Errorf("parent-child scenarios require at least 2 history shards, got %d", r.suite.numHistoryShards)
	}

	var parentShardID int32
	var childShardID int32
	r.parentID, r.childID, parentShardID, childShardID = workflowIDsOnDifferentShards(r.namespaceID, r.suite.numHistoryShards)
	r.tracef("topology: parent shard=%d, child shard=%d", parentShardID, childShardID)

	for clusterIndex, cluster := range r.suite.clusters {
		gate := newParentChildReplicationGate(r.namespaceID, r.parentID, r.childID)
		r.gates[clusterIndex] = gate
		r.removeHooks = append(r.removeHooks, cluster.InjectHook(
			r.suite.T(),
			testhooks.NewHook(testhooks.HistoryReplicationTaskInterceptor, gate.intercept),
			testhooks.GlobalScope,
		))

		metricsHandler, ok := cluster.Host().GetMetricsHandler().(*metricstest.CaptureHandler)
		if !ok {
			return fmt.Errorf("cluster %d metrics handler does not support capture", clusterIndex)
		}
		r.metricCaptures[clusterIndex] = parentChildMetricCapture{
			handler: metricsHandler,
			capture: metricsHandler.StartCapture(),
		}
	}

	r.parentTestVars = testvars.New(r.suite.T()).WithTaskQueue("parent-child-xdc-parent-task-queue")
	r.childTestVars = testvars.New(r.suite.T()).WithTaskQueue("parent-child-xdc-child-task-queue")
	return nil
}

func (r *parentChildScenarioRuntime) close() {
	for _, gate := range r.gates {
		if gate != nil {
			gate.close()
		}
	}
	for index := len(r.removeHooks) - 1; index >= 0; index-- {
		r.removeHooks[index]()
	}
	for _, capture := range r.metricCaptures {
		if capture.handler != nil && capture.capture != nil {
			capture.handler.StopCapture(capture.capture)
		}
	}
	for index := len(r.cleanups) - 1; index >= 0; index-- {
		r.cleanups[index]()
	}
}

func useLegacyHistoryReplication() parentChildScenarioStep {
	return parentChildScenarioStep{
		name: "use legacy history replication for this scenario",
		run: func(_ context.Context, runtime *parentChildScenarioRuntime) error {
			for _, cluster := range runtime.suite.clusters {
				runtime.cleanups = append(runtime.cleanups, cluster.OverrideDynamicConfig(
					runtime.suite.T(),
					dynamicconfig.EnableTransitionHistory,
					false,
				))
			}
			return nil
		},
	}
}

func setLocalParentVerificationGrace(
	cluster parentChildCluster,
	duration time.Duration,
) parentChildScenarioStep {
	return parentChildScenarioStep{
		name: fmt.Sprintf("set local parent verification grace on %s to %s", cluster, duration),
		run: func(_ context.Context, runtime *parentChildScenarioRuntime) error {
			clusterIndex := int(cluster)
			if clusterIndex < 0 || clusterIndex >= len(runtime.suite.clusters) {
				return fmt.Errorf("unknown parent-child cluster %d", cluster)
			}
			runtime.cleanups = append(runtime.cleanups, runtime.suite.clusters[clusterIndex].OverrideDynamicConfig(
				runtime.suite.T(),
				dynamicconfig.MaxLocalParentWorkflowVerificationDuration,
				duration,
			))
			return nil
		},
	}
}

func setStandbyClusterDelay(
	cluster parentChildCluster,
	duration time.Duration,
) parentChildScenarioStep {
	return parentChildScenarioStep{
		name: fmt.Sprintf("set standby cluster delay on %s to %s", cluster, duration),
		run: func(_ context.Context, runtime *parentChildScenarioRuntime) error {
			clusterIndex := int(cluster)
			if clusterIndex < 0 || clusterIndex >= len(runtime.suite.clusters) {
				return fmt.Errorf("unknown parent-child cluster %d", cluster)
			}
			runtime.cleanups = append(runtime.cleanups, runtime.suite.clusters[clusterIndex].OverrideDynamicConfig(
				runtime.suite.T(),
				dynamicconfig.StandbyClusterDelay,
				duration,
			))
			return nil
		},
	}
}

func setStandbyTaskDiscardDelay(
	cluster parentChildCluster,
	taskType enumsspb.TaskType,
	duration time.Duration,
) parentChildScenarioStep {
	return parentChildScenarioStep{
		name: fmt.Sprintf("set standby %s discard delay on %s to %s", taskType, cluster, duration),
		run: func(_ context.Context, runtime *parentChildScenarioRuntime) error {
			clusterIndex := int(cluster)
			if clusterIndex < 0 || clusterIndex >= len(runtime.suite.clusters) {
				return fmt.Errorf("unknown parent-child cluster %d", cluster)
			}
			runtime.cleanups = append(runtime.cleanups, runtime.suite.clusters[clusterIndex].OverrideDynamicConfig(
				runtime.suite.T(),
				dynamicconfig.StandbyTaskMissingEventsDiscardDelay,
				[]dynamicconfig.ConstrainedValue{{
					Constraints: dynamicconfig.Constraints{TaskType: taskType},
					Value:       duration,
				}},
			))
			return nil
		},
	}
}

func startParentWorkflow() parentChildScenarioStep {
	return parentChildScenarioStep{
		name: "start parent workflow on the active cluster",
		run: func(ctx context.Context, runtime *parentChildScenarioRuntime) error {
			return runtime.startParentWorkflow(ctx)
		},
	}
}

// Event checkpoints identify a replication task; each action applies to the entire task.
func applyReplicationThroughTaskContainingEvent(
	targetCluster parentChildCluster,
	workflow parentChildWorkflow,
	eventType enumspb.EventType,
) parentChildScenarioStep {
	return replicationTaskStep(applyReplicationTask, targetCluster, workflow, eventType)
}

func delayReplicationAtTaskContainingEvent(
	targetCluster parentChildCluster,
	workflow parentChildWorkflow,
	eventType enumspb.EventType,
) parentChildScenarioStep {
	return replicationTaskStep(delayReplicationTaskApply, targetCluster, workflow, eventType)
}

func applyDelayedReplication(
	targetCluster parentChildCluster,
	workflow parentChildWorkflow,
) parentChildScenarioStep {
	return parentChildScenarioStep{
		name: fmt.Sprintf("apply delayed %s replication to %s", workflow, targetCluster),
		run: func(_ context.Context, runtime *parentChildScenarioRuntime) error {
			return runtime.applyDelayedReplication(targetCluster, workflow)
		},
	}
}

func acknowledgeReplicationTaskContainingEventWithoutApplying(
	targetCluster parentChildCluster,
	workflow parentChildWorkflow,
	eventType enumspb.EventType,
) parentChildScenarioStep {
	return replicationTaskStep(ackReplicationTaskWithoutApplying, targetCluster, workflow, eventType)
}

func replicationTaskStep(
	action parentChildReplicationTaskAction,
	targetCluster parentChildCluster,
	workflow parentChildWorkflow,
	eventType enumspb.EventType,
) parentChildScenarioStep {
	return parentChildScenarioStep{
		name: fmt.Sprintf("%s %s replication to %s at task containing %s", action, workflow, targetCluster, eventType),
		run: func(ctx context.Context, runtime *parentChildScenarioRuntime) error {
			return runtime.processReplicationThroughTaskContainingEvent(ctx, targetCluster, workflow, eventType, action)
		},
	}
}

func completeParentWorkflowTaskWithStartChildCommand() parentChildScenarioStep {
	return parentChildScenarioStep{
		name: "complete parent workflow task with a StartChild command",
		run: func(ctx context.Context, runtime *parentChildScenarioRuntime) error {
			return runtime.completeParentWorkflowTaskWithStartChildCommand(ctx)
		},
	}
}

func completeChildWorkflowTask() parentChildScenarioStep {
	return parentChildScenarioStep{
		name: "complete the child workflow task",
		run: func(ctx context.Context, runtime *parentChildScenarioRuntime) error {
			return runtime.completeChildWorkflowTask(ctx)
		},
	}
}

func waitForWorkflowEventOnCluster(
	cluster parentChildCluster,
	workflow parentChildWorkflow,
	eventType enumspb.EventType,
) parentChildScenarioStep {
	expectation := workflowHasEventOnCluster(cluster, workflow, eventType)
	return parentChildScenarioStep{
		name: "wait until " + expectation.name,
		run: func(ctx context.Context, runtime *parentChildScenarioRuntime) error {
			return runtime.waitForExpectation(ctx, expectation)
		},
	}
}

func confirmWorkflowIsMissingOnCluster(
	cluster parentChildCluster,
	workflow parentChildWorkflow,
) parentChildScenarioStep {
	return parentChildScenarioStep{
		name: fmt.Sprintf("confirm %s is missing on %s", workflow, cluster),
		run: func(ctx context.Context, runtime *parentChildScenarioRuntime) error {
			return runtime.confirmWorkflowMissing(ctx, cluster, workflow)
		},
	}
}

func forceFailoverNamespaceTo(targetCluster parentChildCluster) parentChildScenarioStep {
	return parentChildScenarioStep{
		name: fmt.Sprintf("force fail over the namespace to %s", targetCluster),
		run: func(ctx context.Context, runtime *parentChildScenarioRuntime) error {
			return runtime.forceFailover(ctx, targetCluster)
		},
	}
}

func currentWorkflowHasStatusOnCluster(
	cluster parentChildCluster,
	workflow parentChildWorkflow,
	status enumspb.WorkflowExecutionStatus,
) parentChildExpectation {
	return parentChildExpectation{
		name: fmt.Sprintf("current %s has status %s on %s", workflow, status, cluster),
		check: func(ctx context.Context, runtime *parentChildScenarioRuntime) error {
			clusterIndex := int(cluster)
			if clusterIndex < 0 || clusterIndex >= len(runtime.suite.clusters) {
				return fmt.Errorf("unknown parent-child cluster %d", cluster)
			}
			workflowID, err := runtime.workflowID(workflow)
			if err != nil {
				return err
			}
			resp, err := runtime.suite.clusters[clusterIndex].FrontendClient().DescribeWorkflowExecution(
				ctx,
				&workflowservice.DescribeWorkflowExecutionRequest{
					Namespace: runtime.namespace,
					Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
				},
			)
			if err != nil {
				return err
			}
			info := resp.GetWorkflowExecutionInfo()
			if runID := info.GetExecution().GetRunId(); runID != runtime.workflowRunID(workflow) {
				return fmt.Errorf("%s run is %q, want %q", workflow, runID, runtime.workflowRunID(workflow))
			}
			if actualStatus := info.GetStatus(); actualStatus != status {
				return fmt.Errorf("%s status is %s, want %s", workflow, actualStatus, status)
			}
			return nil
		},
	}
}

func workflowIsMissingOnCluster(
	cluster parentChildCluster,
	workflow parentChildWorkflow,
) parentChildExpectation {
	return parentChildExpectation{
		name: fmt.Sprintf("%s is missing on %s", workflow, cluster),
		check: func(ctx context.Context, runtime *parentChildScenarioRuntime) error {
			return runtime.confirmWorkflowMissing(ctx, cluster, workflow)
		},
	}
}

func workflowHasEventOnCluster(
	cluster parentChildCluster,
	workflow parentChildWorkflow,
	eventType enumspb.EventType,
) parentChildExpectation {
	return parentChildExpectation{
		name: fmt.Sprintf("%s contains %s on %s", workflow, eventType, cluster),
		check: func(ctx context.Context, runtime *parentChildScenarioRuntime) error {
			events, err := runtime.workflowHistoryOnCluster(ctx, cluster, workflow)
			if err != nil {
				return err
			}
			if !historyContainsEvent(events, eventType) {
				return fmt.Errorf("%s has no %s on %s", workflow, eventType, cluster)
			}
			return nil
		},
	}
}

func historyVerificationFailedOnCluster(
	cluster parentChildCluster,
	operation string,
	expectedError error,
) parentChildExpectation {
	errorType := metrics.ServiceErrorTypeTag(expectedError).Value
	return parentChildExpectation{
		name: fmt.Sprintf("%s fails with %s on %s", operation, errorType, cluster),
		check: func(_ context.Context, runtime *parentChildScenarioRuntime) error {
			namespaceTag := metrics.NamespaceTag(runtime.namespace)
			serviceRoleTag := metrics.ServiceRoleTag(metrics.HistoryRoleTagValue)
			return runtime.requireCapturedMetric(cluster, metrics.ClientFailures.Name(), map[string]string{
				metrics.OperationTagName: operation,
				metrics.ErrorTypeTagName: errorType,
				namespaceTag.Key:         namespaceTag.Value,
				serviceRoleTag.Key:       serviceRoleTag.Value,
			})
		},
	}
}

func waitForHistoryVerificationFailureOnCluster(
	cluster parentChildCluster,
	operation string,
	expectedError error,
) parentChildScenarioStep {
	expectation := historyVerificationFailedOnCluster(cluster, operation, expectedError)
	return parentChildScenarioStep{
		name: "wait until " + expectation.name,
		run: func(ctx context.Context, runtime *parentChildScenarioRuntime) error {
			return runtime.waitForExpectation(ctx, expectation)
		},
	}
}

func taskWasDiscardedOnCluster(
	cluster parentChildCluster,
	taskType string,
) parentChildExpectation {
	return parentChildExpectation{
		name: fmt.Sprintf("%s is discarded on %s", taskType, cluster),
		check: func(_ context.Context, runtime *parentChildScenarioRuntime) error {
			namespaceTag := metrics.NamespaceTag(runtime.namespace)
			return runtime.requireCapturedMetric(cluster, metrics.TaskDiscarded.Name(), map[string]string{
				metrics.OperationTagName: taskType,
				metrics.TaskTypeTagName:  taskType,
				namespaceTag.Key:         namespaceTag.Value,
			})
		},
	}
}

func (r *parentChildScenarioRuntime) startParentWorkflow(ctx context.Context) error {
	if r.parentRunID != "" {
		return fmt.Errorf("parent workflow is already started with run ID %q", r.parentRunID)
	}
	startResp, err := r.activeCluster().FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:           r.namespace,
		WorkflowId:          r.parentID,
		WorkflowType:        &commonpb.WorkflowType{Name: "parent-workflow"},
		TaskQueue:           r.parentTestVars.TaskQueue(),
		RequestId:           uuid.NewString(),
		WorkflowRunTimeout:  durationpb.New(time.Minute),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            r.parentTestVars.WorkerIdentity(),
	})
	if err != nil {
		return err
	}
	r.parentRunID = startResp.GetRunId()
	r.tracef("  started parent %s/%s on cluster %d", r.parentID, r.parentRunID, r.activeClusterIndex)
	return nil
}

func (r *parentChildScenarioRuntime) processReplicationThroughTaskContainingEvent(
	ctx context.Context,
	targetCluster parentChildCluster,
	workflow parentChildWorkflow,
	eventType enumspb.EventType,
	targetAction parentChildReplicationTaskAction,
) error {
	targetClusterIndex := int(targetCluster)
	if targetClusterIndex < 0 || targetClusterIndex >= len(r.gates) {
		return fmt.Errorf("unknown parent-child cluster %d", targetCluster)
	}
	lane := parentChildReplicationLane{targetClusterIndex: targetClusterIndex, workflow: workflow}
	workflowID, err := r.workflowID(workflow)
	if err != nil {
		return err
	}

	for {
		task, delayed := r.delayedTasks[lane]
		if !delayed {
			task, err = r.gates[targetClusterIndex].nextForWorkflow(ctx, workflowID)
			if err != nil {
				return err
			}
		}

		events, err := decodeParentChildReplicationEvents(task.task)
		if err != nil {
			return err
		}
		containsCheckpoint := historyContainsEvent(events, eventType)
		action := applyReplicationTask
		if containsCheckpoint {
			action = targetAction
		}

		r.tracef(
			"  %s task %d to cluster %d for %s [%s]",
			action,
			task.task.GetSourceTaskId(),
			targetClusterIndex,
			workflow,
			formatParentChildReplicationTask(task.task, events),
		)
		switch action {
		case applyReplicationTask:
			delete(r.delayedTasks, lane)
			if err := task.apply(); err != nil {
				return err
			}
			r.recordChildRunIDFromAppliedTask(workflow, task, events)
		case delayReplicationTaskApply:
			r.delayedTasks[lane] = task
		case ackReplicationTaskWithoutApplying:
			delete(r.delayedTasks, lane)
			if err := task.acknowledgeWithoutApplying(); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown replication task action %d", action)
		}

		if containsCheckpoint {
			return nil
		}
	}
}

func (r *parentChildScenarioRuntime) applyDelayedReplication(
	targetCluster parentChildCluster,
	workflow parentChildWorkflow,
) error {
	targetClusterIndex := int(targetCluster)
	if targetClusterIndex < 0 || targetClusterIndex >= len(r.gates) {
		return fmt.Errorf("unknown parent-child cluster %d", targetCluster)
	}
	if _, err := r.workflowID(workflow); err != nil {
		return err
	}

	lane := parentChildReplicationLane{targetClusterIndex: targetClusterIndex, workflow: workflow}
	task, delayed := r.delayedTasks[lane]
	if !delayed {
		return fmt.Errorf("no delayed %s replication task to %s", workflow, targetCluster)
	}
	events, err := decodeParentChildReplicationEvents(task.task)
	if err != nil {
		return err
	}

	r.tracef(
		"  apply delayed task %d to cluster %d for %s [%s]",
		task.task.GetSourceTaskId(),
		targetClusterIndex,
		workflow,
		formatParentChildReplicationTask(task.task, events),
	)
	delete(r.delayedTasks, lane)
	if err := task.apply(); err != nil {
		return err
	}
	r.recordChildRunIDFromAppliedTask(workflow, task, events)
	return nil
}

func (r *parentChildScenarioRuntime) completeParentWorkflowTaskWithStartChildCommand(ctx context.Context) error {
	if r.parentRunID == "" {
		return errors.New("parent workflow is not started")
	}
	poller := taskpoller.New(r.suite.T(), r.activeCluster().FrontendClient(), r.namespace)
	_, err := poller.PollAndHandleWorkflowTask(r.parentTestVars, func(
		task *workflowservice.PollWorkflowTaskQueueResponse,
	) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		if task.GetWorkflowExecution().GetWorkflowId() != r.parentID || task.GetWorkflowExecution().GetRunId() != r.parentRunID {
			return nil, fmt.Errorf(
				"polled workflow %s/%s, want parent %s/%s",
				task.GetWorkflowExecution().GetWorkflowId(),
				task.GetWorkflowExecution().GetRunId(),
				r.parentID,
				r.parentRunID,
			)
		}
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{
					StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
						WorkflowId:          r.childID,
						WorkflowType:        &commonpb.WorkflowType{Name: "child-workflow"},
						TaskQueue:           r.childTestVars.TaskQueue(),
						WorkflowRunTimeout:  durationpb.New(time.Minute),
						WorkflowTaskTimeout: durationpb.New(10 * time.Second),
						ParentClosePolicy:   enumspb.PARENT_CLOSE_POLICY_ABANDON,
					},
				},
			}},
		}, nil
	}, taskpoller.WithTimeout(testTimeout))
	return err
}

func (r *parentChildScenarioRuntime) completeChildWorkflowTask(ctx context.Context) error {
	poller := taskpoller.New(r.suite.T(), r.activeCluster().FrontendClient(), r.namespace)
	_, err := poller.PollAndHandleWorkflowTask(r.childTestVars, func(
		task *workflowservice.PollWorkflowTaskQueueResponse,
	) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		execution := task.GetWorkflowExecution()
		if execution.GetWorkflowId() != r.childID {
			return nil, fmt.Errorf("polled workflow %s/%s, want child %s", execution.GetWorkflowId(), execution.GetRunId(), r.childID)
		}
		if r.childRunID == "" {
			r.childRunID = execution.GetRunId()
		}
		if execution.GetRunId() != r.childRunID {
			return nil, fmt.Errorf("polled child run %s, want %s", execution.GetRunId(), r.childRunID)
		}
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
				},
			}},
		}, nil
	}, taskpoller.WithTimeout(testTimeout))
	return err
}

func (r *parentChildScenarioRuntime) workflowHistoryOnCluster(
	ctx context.Context,
	cluster parentChildCluster,
	workflow parentChildWorkflow,
) ([]*historypb.HistoryEvent, error) {
	clusterIndex := int(cluster)
	if clusterIndex < 0 || clusterIndex >= len(r.suite.clusters) {
		return nil, fmt.Errorf("unknown parent-child cluster %d", cluster)
	}
	workflowID, err := r.workflowID(workflow)
	if err != nil {
		return nil, err
	}
	runID := r.workflowRunID(workflow)
	resp, err := r.suite.clusters[clusterIndex].FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: r.namespace,
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetHistory().GetEvents(), nil
}

func (r *parentChildScenarioRuntime) workflowMutableState(
	ctx context.Context,
	cluster parentChildCluster,
	workflow parentChildWorkflow,
) (*persistencespb.WorkflowMutableState, error) {
	clusterIndex := int(cluster)
	if clusterIndex < 0 || clusterIndex >= len(r.suite.clusters) {
		return nil, fmt.Errorf("unknown parent-child cluster %d", cluster)
	}
	workflowID, err := r.workflowID(workflow)
	if err != nil {
		return nil, err
	}
	resp, err := r.suite.clusters[clusterIndex].HistoryClient().DescribeMutableState(ctx, &historyservice.DescribeMutableStateRequest{
		NamespaceId: r.namespaceID,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      r.workflowRunID(workflow),
		},
		ArchetypeId: chasm.WorkflowArchetypeID,
	})
	if err != nil {
		return nil, err
	}
	if resp.GetDatabaseMutableState() == nil {
		return nil, fmt.Errorf("%s database mutable state is nil on %s", workflow, cluster)
	}
	return resp.GetDatabaseMutableState(), nil
}

func (r *parentChildScenarioRuntime) confirmWorkflowMissing(
	ctx context.Context,
	cluster parentChildCluster,
	workflow parentChildWorkflow,
) error {
	_, err := r.workflowMutableState(ctx, cluster, workflow)
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		return nil
	}
	if err == nil {
		return fmt.Errorf("%s unexpectedly exists on %s", workflow, cluster)
	}
	return err
}

func (r *parentChildScenarioRuntime) requireCapturedMetric(
	cluster parentChildCluster,
	metricName string,
	tags map[string]string,
) error {
	clusterIndex := int(cluster)
	if clusterIndex < 0 || clusterIndex >= len(r.metricCaptures) {
		return fmt.Errorf("unknown parent-child cluster %d", cluster)
	}
	capture := r.metricCaptures[clusterIndex].capture
	if capture == nil {
		return fmt.Errorf("metrics capture is not initialized for %s", cluster)
	}
	recordings := capture.Snapshot()[metricName]
	for _, recording := range recordings {
		matches := true
		for key, value := range tags {
			if recording.Tags[key] != value {
				matches = false
				break
			}
		}
		if matches {
			return nil
		}
	}
	recordedTags := make([]map[string]string, 0, len(recordings))
	for _, recording := range recordings {
		recordedTags = append(recordedTags, recording.Tags)
	}
	return fmt.Errorf("metric %q with tags %v was not captured on %s; recorded tags: %v", metricName, tags, cluster, recordedTags)
}

func (r *parentChildScenarioRuntime) waitForExpectation(
	ctx context.Context,
	expectation parentChildExpectation,
) error {
	ticker := time.NewTicker(replicationCheckInterval)
	defer ticker.Stop()
	for {
		err := expectation.check(ctx, r)
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for %s: %w; last observation: %v", expectation.name, ctx.Err(), err)
		case <-ticker.C:
		}
	}
}

func (r *parentChildScenarioRuntime) forceFailover(ctx context.Context, target parentChildCluster) error {
	targetClusterIndex := int(target)
	if targetClusterIndex < 0 || targetClusterIndex >= len(r.suite.clusters) {
		return fmt.Errorf("unknown parent-child cluster %d", target)
	}
	if targetClusterIndex == r.activeClusterIndex {
		return fmt.Errorf("cluster %s is already active", target)
	}
	targetCluster := r.suite.clusters[targetClusterIndex].ClusterName()
	_, err := r.activeCluster().FrontendClient().UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: r.namespace,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: targetCluster,
		},
	})
	if err != nil {
		return err
	}

	await.Require(ctx, r.suite.T(), func(t *await.T) {
		for _, cluster := range r.suite.clusters {
			resp, describeErr := cluster.FrontendClient().DescribeNamespace(t.Context(), &workflowservice.DescribeNamespaceRequest{
				Namespace: r.namespace,
			})
			require.NoError(t, describeErr)
			require.Equal(t, targetCluster, resp.GetReplicationConfig().GetActiveClusterName())
		}
	}, replicationWaitTime, replicationCheckInterval)
	r.suite.waitForNamespaceCacheRefresh()
	r.activeClusterIndex = targetClusterIndex
	r.tracef("  active cluster is now %d (%s)", targetClusterIndex, targetCluster)
	return nil
}

func (r *parentChildScenarioRuntime) activeWorkflowHistory(
	ctx context.Context,
	workflow parentChildWorkflow,
) ([]*historypb.HistoryEvent, error) {
	return r.workflowHistoryOnCluster(ctx, parentChildCluster(r.activeClusterIndex), workflow)
}

func (r *parentChildScenarioRuntime) activeCluster() *testcore.TestCluster {
	return r.suite.clusters[r.activeClusterIndex]
}

func (r *parentChildScenarioRuntime) workflowID(workflow parentChildWorkflow) (string, error) {
	switch workflow {
	case parentWorkflow:
		return r.parentID, nil
	case childWorkflow:
		return r.childID, nil
	default:
		return "", fmt.Errorf("unknown parent-child workflow %d", workflow)
	}
}

func (r *parentChildScenarioRuntime) workflowRunID(workflow parentChildWorkflow) string {
	switch workflow {
	case parentWorkflow:
		return r.parentRunID
	case childWorkflow:
		return r.childRunID
	default:
		return ""
	}
}

func (r *parentChildScenarioRuntime) recordChildRunIDFromAppliedTask(
	workflow parentChildWorkflow,
	task *parentChildReplicationTask,
	events []*historypb.HistoryEvent,
) {
	if workflow == childWorkflow && historyContainsEvent(events, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED) {
		r.childRunID = task.metadata.runID
	}
}

func (r *parentChildScenarioRuntime) tracef(format string, args ...any) {
	r.trace = append(r.trace, fmt.Sprintf(format, args...))
}

func newParentChildReplicationGate(namespaceID string, workflowIDs ...string) *parentChildReplicationGate {
	gate := &parentChildReplicationGate{
		namespaceID: namespaceID,
		workflowIDs: make(map[string]struct{}, len(workflowIDs)),
		pending:     make(chan *parentChildReplicationTask, 32),
		buffered:    make(map[string][]*parentChildReplicationTask),
		stop:        make(chan struct{}),
	}
	for _, workflowID := range workflowIDs {
		gate.workflowIDs[workflowID] = struct{}{}
	}
	return gate
}

func (g *parentChildReplicationGate) intercept(
	task *replicationspb.ReplicationTask,
	execute func() error,
) error {
	metadata, ok := getParentChildReplicationTaskMetadata(task)
	if !ok || metadata.namespaceID != g.namespaceID {
		return execute()
	}
	if _, workflowSelected := g.workflowIDs[metadata.workflowID]; !workflowSelected {
		return execute()
	}

	bufferedTask := &parentChildReplicationTask{
		task:     task,
		metadata: metadata,
		execute:  execute,
		result:   make(chan error, 1),
	}
	select {
	case g.pending <- bufferedTask:
	case <-g.stop:
		return nil
	}
	select {
	case err := <-bufferedTask.result:
		return err
	case <-g.stop:
		return nil
	}
}

func (g *parentChildReplicationGate) nextForWorkflow(
	ctx context.Context,
	workflowID string,
) (*parentChildReplicationTask, error) {
	select {
	case <-g.stop:
		return nil, errParentChildReplicationGateClosed
	default:
	}

	if tasks := g.buffered[workflowID]; len(tasks) > 0 {
		task := tasks[0]
		g.buffered[workflowID] = tasks[1:]
		return task, nil
	}

	for {
		select {
		case task := <-g.pending:
			taskWorkflowID := task.metadata.workflowID
			if taskWorkflowID == workflowID {
				return task, nil
			}
			g.buffered[taskWorkflowID] = append(g.buffered[taskWorkflowID], task)
		case <-g.stop:
			return nil, errParentChildReplicationGateClosed
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (g *parentChildReplicationGate) close() {
	g.stopOnce.Do(func() {
		close(g.stop)
	})
}

func (t *parentChildReplicationTask) apply() error {
	return t.resolve(true)
}

func (t *parentChildReplicationTask) acknowledgeWithoutApplying() error {
	return t.resolve(false)
}

func (t *parentChildReplicationTask) resolve(shouldApply bool) error {
	t.mu.Lock()
	if t.resolved {
		t.mu.Unlock()
		return errParentChildTaskAlreadyResolved
	}
	t.resolved = true
	t.mu.Unlock()

	var err error
	if shouldApply {
		err = t.execute()
	}
	t.result <- err
	return err
}

func getParentChildReplicationTaskMetadata(
	task *replicationspb.ReplicationTask,
) (parentChildReplicationTaskMetadata, bool) {
	if task == nil {
		return parentChildReplicationTaskMetadata{}, false
	}
	if info := task.GetRawTaskInfo(); info != nil {
		return parentChildReplicationTaskMetadata{
			namespaceID: info.GetNamespaceId(),
			workflowID:  info.GetWorkflowId(),
			runID:       info.GetRunId(),
		}, true
	}

	var metadata parentChildReplicationTaskMetadata
	switch attrs := task.GetAttributes().(type) {
	case *replicationspb.ReplicationTask_SyncWorkflowStateTaskAttributes:
		state := attrs.SyncWorkflowStateTaskAttributes.GetWorkflowState()
		metadata.namespaceID = state.GetExecutionInfo().GetNamespaceId()
		metadata.workflowID = state.GetExecutionInfo().GetWorkflowId()
		metadata.runID = state.GetExecutionState().GetRunId()
	case *replicationspb.ReplicationTask_SyncActivityTaskAttributes:
		metadata.namespaceID = attrs.SyncActivityTaskAttributes.GetNamespaceId()
		metadata.workflowID = attrs.SyncActivityTaskAttributes.GetWorkflowId()
		metadata.runID = attrs.SyncActivityTaskAttributes.GetRunId()
	case *replicationspb.ReplicationTask_HistoryTaskAttributes:
		metadata.namespaceID = attrs.HistoryTaskAttributes.GetNamespaceId()
		metadata.workflowID = attrs.HistoryTaskAttributes.GetWorkflowId()
		metadata.runID = attrs.HistoryTaskAttributes.GetRunId()
	case *replicationspb.ReplicationTask_SyncHsmAttributes:
		metadata.namespaceID = attrs.SyncHsmAttributes.GetNamespaceId()
		metadata.workflowID = attrs.SyncHsmAttributes.GetWorkflowId()
		metadata.runID = attrs.SyncHsmAttributes.GetRunId()
	case *replicationspb.ReplicationTask_BackfillHistoryTaskAttributes:
		metadata.namespaceID = attrs.BackfillHistoryTaskAttributes.GetNamespaceId()
		metadata.workflowID = attrs.BackfillHistoryTaskAttributes.GetWorkflowId()
		metadata.runID = attrs.BackfillHistoryTaskAttributes.GetRunId()
	case *replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes:
		metadata.namespaceID = attrs.VerifyVersionedTransitionTaskAttributes.GetNamespaceId()
		metadata.workflowID = attrs.VerifyVersionedTransitionTaskAttributes.GetWorkflowId()
		metadata.runID = attrs.VerifyVersionedTransitionTaskAttributes.GetRunId()
	case *replicationspb.ReplicationTask_SyncVersionedTransitionTaskAttributes:
		metadata.namespaceID = attrs.SyncVersionedTransitionTaskAttributes.GetNamespaceId()
		metadata.workflowID = attrs.SyncVersionedTransitionTaskAttributes.GetWorkflowId()
		metadata.runID = attrs.SyncVersionedTransitionTaskAttributes.GetRunId()
	default:
		return parentChildReplicationTaskMetadata{}, false
	}
	return metadata, true
}

func decodeParentChildReplicationEvents(task *replicationspb.ReplicationTask) ([]*historypb.HistoryEvent, error) {
	var blobs []*commonpb.DataBlob
	switch attrs := task.GetAttributes().(type) {
	case *replicationspb.ReplicationTask_HistoryTaskAttributes:
		blobs = attrs.HistoryTaskAttributes.GetEventsBatches()
		if attrs.HistoryTaskAttributes.GetEvents() != nil {
			blobs = []*commonpb.DataBlob{attrs.HistoryTaskAttributes.GetEvents()}
		}
		if len(blobs) == 0 {
			return nil, errors.New("history replication task has no event batch")
		}
	case *replicationspb.ReplicationTask_SyncVersionedTransitionTaskAttributes:
		artifact := attrs.SyncVersionedTransitionTaskAttributes.GetVersionedTransitionArtifact()
		if artifact == nil {
			return nil, errors.New("sync versioned transition task has no artifact")
		}
		blobs = artifact.GetEventBatches()
	case *replicationspb.ReplicationTask_BackfillHistoryTaskAttributes:
		blobs = attrs.BackfillHistoryTaskAttributes.GetEventBatches()
		if len(blobs) == 0 {
			return nil, errors.New("backfill history replication task has no event batch")
		}
	case *replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes,
		*replicationspb.ReplicationTask_SyncWorkflowStateTaskAttributes,
		*replicationspb.ReplicationTask_SyncActivityTaskAttributes,
		*replicationspb.ReplicationTask_SyncHsmAttributes:
		return nil, nil
	default:
		if _, ok := getParentChildReplicationTaskMetadata(task); ok {
			return nil, nil
		}
		return nil, errors.New("replication task has no workflow metadata")
	}

	var events []*historypb.HistoryEvent
	for _, blob := range blobs {
		batch, err := serialization.DefaultDecoder.DeserializeEvents(blob)
		if err != nil {
			return nil, err
		}
		events = append(events, batch...)
	}
	return events, nil
}

func historyContainsEvent(events []*historypb.HistoryEvent, eventType enumspb.EventType) bool {
	for _, event := range events {
		if event.GetEventType() == eventType {
			return true
		}
	}
	return false
}

func findHistoryEvent(
	events []*historypb.HistoryEvent,
	eventType enumspb.EventType,
	matches func(*historypb.HistoryEvent) bool,
) *historypb.HistoryEvent {
	for _, event := range events {
		if event.GetEventType() == eventType && (matches == nil || matches(event)) {
			return event
		}
	}
	return nil
}

func formatParentChildEventTypes(events []*historypb.HistoryEvent) string {
	eventTypes := make([]string, 0, len(events))
	for _, event := range events {
		eventTypes = append(eventTypes, strings.TrimPrefix(event.GetEventType().String(), "EVENT_TYPE_"))
	}
	return strings.Join(eventTypes, ", ")
}

func formatParentChildReplicationTask(
	task *replicationspb.ReplicationTask,
	events []*historypb.HistoryEvent,
) string {
	if len(events) != 0 {
		return formatParentChildEventTypes(events)
	}
	switch task.GetAttributes().(type) {
	case *replicationspb.ReplicationTask_SyncWorkflowStateTaskAttributes:
		return "SyncWorkflowState"
	case *replicationspb.ReplicationTask_SyncActivityTaskAttributes:
		return "SyncActivity"
	case *replicationspb.ReplicationTask_SyncHsmAttributes:
		return "SyncHSM"
	case *replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes:
		return "VerifyVersionedTransition"
	case *replicationspb.ReplicationTask_SyncVersionedTransitionTaskAttributes:
		return "SyncVersionedTransition"
	default:
		return task.GetTaskType().String()
	}
}

func workflowIDsOnDifferentShards(
	namespaceID string,
	numHistoryShards int32,
) (parentID string, childID string, parentShardID int32, childShardID int32) {
	parentID = "parent-" + uuid.NewString()
	parentShardID = common.WorkflowIDToHistoryShard(namespaceID, parentID, numHistoryShards)
	for {
		childID = "child-" + uuid.NewString()
		childShardID = common.WorkflowIDToHistoryShard(namespaceID, childID, numHistoryShards)
		if childShardID != parentShardID {
			return parentID, childID, parentShardID, childShardID
		}
	}
}

func (workflow parentChildWorkflow) String() string {
	switch workflow {
	case parentWorkflow:
		return "parent"
	case childWorkflow:
		return "child"
	default:
		return fmt.Sprintf("workflow(%d)", workflow)
	}
}

func (cluster parentChildCluster) String() string {
	switch cluster {
	case initialActiveCluster:
		return "initial active cluster"
	case initialStandbyCluster:
		return "initial standby cluster"
	default:
		return fmt.Sprintf("cluster(%d)", cluster)
	}
}

func (action parentChildReplicationTaskAction) String() string {
	switch action {
	case applyReplicationTask:
		return "apply"
	case delayReplicationTaskApply:
		return "delay-apply"
	case ackReplicationTaskWithoutApplying:
		return "ack-without-apply"
	default:
		return fmt.Sprintf("action(%d)", action)
	}
}
