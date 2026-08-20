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
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
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

type parentChildXDCTestSuite struct {
	xdcBaseSuite
}

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

		activeClusterIndex     int
		gates                  [2]*parentChildReplicationGate
		removeHooks            []func()
		heldTasks              map[parentChildReplicationLane]*parentChildReplicationTask
		eventsFromAppliedTasks map[parentChildWorkflow]map[enumspb.EventType][]*historypb.HistoryEvent
		trace                  []string
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
	holdReplicationTask
	ackReplicationTaskWithoutApplying
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
		suite:                  s,
		heldTasks:              make(map[parentChildReplicationLane]*parentChildReplicationTask),
		eventsFromAppliedTasks: make(map[parentChildWorkflow]map[enumspb.EventType][]*historypb.HistoryEvent),
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
	if parentShardID == childShardID {
		return fmt.Errorf("parent and child must use different shards, both mapped to %d", parentShardID)
	}
	r.tracef("topology: parent shard=%d, child shard=%d", parentShardID, childShardID)

	for clusterIndex, cluster := range r.suite.clusters {
		gate := newParentChildReplicationGate(r.namespaceID, r.parentID, r.childID)
		r.gates[clusterIndex] = gate
		r.removeHooks = append(r.removeHooks, cluster.InjectHook(
			r.suite.T(),
			testhooks.NewHook(testhooks.HistoryReplicationTaskInterceptor, gate.intercept),
			testhooks.GlobalScope,
		))
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

func holdReplicationAtTaskContainingEvent(
	targetCluster parentChildCluster,
	workflow parentChildWorkflow,
	eventType enumspb.EventType,
) parentChildScenarioStep {
	return replicationTaskStep(holdReplicationTask, targetCluster, workflow, eventType)
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

func failoverNamespaceTo(targetCluster parentChildCluster) parentChildScenarioStep {
	return parentChildScenarioStep{
		name: fmt.Sprintf("force fail over the namespace to %s", targetCluster),
		run: func(ctx context.Context, runtime *parentChildScenarioRuntime) error {
			return runtime.failover(ctx, targetCluster)
		},
	}
}

func refreshParentWorkflowTasks() parentChildScenarioStep {
	return parentChildScenarioStep{
		name: "refresh parent workflow tasks on the new active cluster",
		run: func(ctx context.Context, runtime *parentChildScenarioRuntime) error {
			return runtime.refreshParentWorkflowTasks(ctx)
		},
	}
}

func parentStartChildFailed(
	cause enumspb.StartChildWorkflowExecutionFailedCause,
) parentChildExpectation {
	return parentChildExpectation{
		name: fmt.Sprintf("parent StartChild fails with %s", cause),
		check: func(ctx context.Context, runtime *parentChildScenarioRuntime) error {
			events, err := runtime.parentHistory(ctx)
			if err != nil {
				return err
			}
			for _, event := range events {
				attrs := event.GetStartChildWorkflowExecutionFailedEventAttributes()
				if attrs == nil || attrs.GetWorkflowId() != runtime.childID || attrs.GetCause() != cause {
					continue
				}
				for _, initiatedEvent := range events {
					initiatedAttrs := initiatedEvent.GetStartChildWorkflowExecutionInitiatedEventAttributes()
					if initiatedAttrs != nil &&
						initiatedAttrs.GetWorkflowId() == runtime.childID &&
						initiatedEvent.GetEventId() == attrs.GetInitiatedEventId() {
						return nil
					}
				}
			}
			return fmt.Errorf("parent has no StartChild failure for child %q with cause %s", runtime.childID, cause)
		},
	}
}

func childIsOrphaned() parentChildExpectation {
	return parentChildExpectation{
		name: "child belongs to the losing parent branch",
		check: func(ctx context.Context, runtime *parentChildScenarioRuntime) error {
			if runtime.childRunID == "" {
				return errors.New("child WorkflowExecutionStarted was not applied")
			}
			childEvents, err := runtime.childHistory(ctx)
			if err != nil {
				return err
			}
			var childStartedEvent *historypb.HistoryEvent
			for _, event := range childEvents {
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
					childStartedEvent = event
					break
				}
			}
			if childStartedEvent == nil {
				return fmt.Errorf("child run %q has no persisted WorkflowExecutionStarted event", runtime.childRunID)
			}
			childStartedAttrs := childStartedEvent.GetWorkflowExecutionStartedEventAttributes()
			if childStartedAttrs == nil {
				return errors.New("persisted child start event has no attributes")
			}
			parentExecution := childStartedAttrs.GetParentWorkflowExecution()
			if parentExecution.GetWorkflowId() != runtime.parentID || parentExecution.GetRunId() != runtime.parentRunID {
				return fmt.Errorf(
					"child parent is %s/%s, want %s/%s",
					parentExecution.GetWorkflowId(),
					parentExecution.GetRunId(),
					runtime.parentID,
					runtime.parentRunID,
				)
			}

			parentEvents, err := runtime.parentHistory(ctx)
			if err != nil {
				return err
			}
			var currentInitiatedEvent *historypb.HistoryEvent
			for _, event := range parentEvents {
				attrs := event.GetStartChildWorkflowExecutionInitiatedEventAttributes()
				if attrs != nil && attrs.GetWorkflowId() == runtime.childID && event.GetEventId() == childStartedAttrs.GetParentInitiatedEventId() {
					currentInitiatedEvent = event
				}
				startedAttrs := event.GetChildWorkflowExecutionStartedEventAttributes()
				if startedAttrs != nil &&
					startedAttrs.GetWorkflowExecution().GetWorkflowId() == runtime.childID &&
					startedAttrs.GetWorkflowExecution().GetRunId() == runtime.childRunID {
					return errors.New("current parent branch owns the applied child")
				}
			}
			if currentInitiatedEvent == nil {
				return fmt.Errorf(
					"current parent branch has no StartChild event %d for child %q",
					childStartedAttrs.GetParentInitiatedEventId(),
					runtime.childID,
				)
			}
			if currentInitiatedEvent.GetVersion() == childStartedAttrs.GetParentInitiatedEventVersion() {
				return fmt.Errorf(
					"child and current parent branch have the same initiation version %d",
					currentInitiatedEvent.GetVersion(),
				)
			}
			runtime.tracef(
				"orphan confirmed: child parent pointer=(%d, v%d), current parent initiation=(%d, v%d)",
				childStartedAttrs.GetParentInitiatedEventId(),
				childStartedAttrs.GetParentInitiatedEventVersion(),
				currentInitiatedEvent.GetEventId(),
				currentInitiatedEvent.GetVersion(),
			)
			return nil
		},
	}
}

func childHasStatus(status enumspb.WorkflowExecutionStatus) parentChildExpectation {
	return parentChildExpectation{
		name: fmt.Sprintf("child has status %s", status),
		check: func(ctx context.Context, runtime *parentChildScenarioRuntime) error {
			resp, err := runtime.activeCluster().FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: runtime.namespace,
				Execution: &commonpb.WorkflowExecution{WorkflowId: runtime.childID},
			})
			if err != nil {
				return err
			}
			info := resp.GetWorkflowExecutionInfo()
			if info.GetExecution().GetRunId() != runtime.childRunID {
				return fmt.Errorf("current child run is %q, want %q", info.GetExecution().GetRunId(), runtime.childRunID)
			}
			if info.GetStatus() != status {
				return fmt.Errorf("child status is %s, want %s", info.GetStatus(), status)
			}
			return nil
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
		task, held := r.heldTasks[lane]
		if !held {
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
			delete(r.heldTasks, lane)
			if err := task.apply(); err != nil {
				return err
			}
			r.recordAppliedTaskEvents(workflow, task, events)
		case holdReplicationTask:
			r.heldTasks[lane] = task
		case ackReplicationTaskWithoutApplying:
			delete(r.heldTasks, lane)
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

func (r *parentChildScenarioRuntime) refreshParentWorkflowTasks(ctx context.Context) error {
	if r.parentRunID == "" {
		return errors.New("parent workflow is not started")
	}
	_, err := r.activeCluster().AdminClient().RefreshWorkflowTasks(ctx, &adminservice.RefreshWorkflowTasksRequest{
		NamespaceId: r.namespaceID,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: r.parentID,
			RunId:      r.parentRunID,
		},
	})
	return err
}

func (r *parentChildScenarioRuntime) failover(ctx context.Context, target parentChildCluster) error {
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

func (r *parentChildScenarioRuntime) parentHistory(ctx context.Context) ([]*historypb.HistoryEvent, error) {
	resp, err := r.activeCluster().FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: r.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: r.parentID,
			RunId:      r.parentRunID,
		},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetHistory().GetEvents(), nil
}

func (r *parentChildScenarioRuntime) childHistory(ctx context.Context) ([]*historypb.HistoryEvent, error) {
	resp, err := r.activeCluster().FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: r.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: r.childID,
			RunId:      r.childRunID,
		},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetHistory().GetEvents(), nil
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

func (r *parentChildScenarioRuntime) recordAppliedTaskEvents(
	workflow parentChildWorkflow,
	task *parentChildReplicationTask,
	events []*historypb.HistoryEvent,
) {
	if r.eventsFromAppliedTasks[workflow] == nil {
		r.eventsFromAppliedTasks[workflow] = make(map[enumspb.EventType][]*historypb.HistoryEvent)
	}
	for _, event := range events {
		eventType := event.GetEventType()
		r.eventsFromAppliedTasks[workflow][eventType] = append(r.eventsFromAppliedTasks[workflow][eventType], event)
		if workflow == childWorkflow && eventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
			r.childRunID = task.metadata.runID
		}
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
	case holdReplicationTask:
		return "hold"
	case ackReplicationTaskWithoutApplying:
		return "ack-without-apply"
	default:
		return fmt.Sprintf("action(%d)", action)
	}
}
