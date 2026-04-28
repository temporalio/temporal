package standalonenexusop

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/tests/testcore/umpire"
	"google.golang.org/protobuf/types/known/durationpb"
)

const defaultMaxOperations = 2

// driver implements umpire.ModelBehavior for the standalone Nexus operation
// lifecycle: Start, Poll-and-Complete, Terminate, Cancel-Request, Describe.
type driver struct {
	u             *umpire.Umpire
	store         *umpire.EntityStore[*Entity]
	ctx           context.Context
	client        workflowservice.WorkflowServiceClient
	namespace     string
	endpointName  string
	taskQueue     string
	prefix        string
	maxOperations int
	nextOpSeq     int
}

var _ umpire.ModelBehavior = (*driver)(nil)

func newDriver(deps Deps) *driver {
	max := deps.MaxOperations
	if max == 0 {
		max = defaultMaxOperations
	}
	return &driver{
		u:             deps.Umpire,
		store:         deps.Store,
		ctx:           deps.Context,
		client:        deps.Client,
		namespace:     deps.Namespace,
		endpointName:  deps.EndpointName,
		taskQueue:     deps.TaskQueue,
		prefix:        deps.Prefix,
		maxOperations: max,
	}
}

func (d *driver) Actions() []umpire.Action {
	return []umpire.Action{
		{
			Name:    "StandaloneNexusStartOperation",
			Enabled: func() bool { return d.nextOpSeq < d.maxOperations },
			Run:     d.startOperation,
		},
		{
			Name:    "StandaloneNexusPollAndComplete",
			Enabled: d.hasRunningOp,
			Run:     d.pollAndComplete,
		},
		{
			Name:    "StandaloneNexusTerminateOperation",
			Enabled: d.hasRunningOp,
			Run:     d.terminateOperation,
		},
		{
			Name:    "StandaloneNexusCancelOperation",
			Enabled: d.hasCancelableOp,
			Run:     d.cancelOperation,
		},
		{
			Name:    "StandaloneNexusDescribeOperation",
			Enabled: func() bool { return d.store.Len() != 0 },
			Run:     d.describeOperation,
		},
	}
}

func (d *driver) startOperation(t *umpire.T) {
	opID := fmt.Sprintf("%s-op-%d", d.prefix, d.nextOpSeq)
	d.nextOpSeq++

	resp, err := d.client.StartNexusOperationExecution(d.ctx, &workflowservice.StartNexusOperationExecutionRequest{
		Namespace:              d.namespace,
		Service:                "test-service",
		Operation:              "test-operation",
		OperationId:            opID,
		RequestId:              opID,
		Endpoint:               d.endpointName,
		ScheduleToCloseTimeout: durationpb.New(10 * time.Minute),
	})
	t.NoError(err)
	t.True(resp.GetStarted(), "expected operation %s to start", opID)
	t.NotEmpty(opID, "started operation must have an operation ID")
	// World mutation happens server-side via the observer.
}

func (d *driver) pollAndComplete(t *umpire.T) {
	resp := d.pollTask(t)

	if start := resp.GetRequest().GetStartOperation(); start != nil {
		op := d.opForTask(TaskKindStart, start.GetRequestId())
		t.NotNil(op, "unknown operation %s", start.GetRequestId())
		_, err := d.client.RespondNexusTaskCompleted(d.ctx, &workflowservice.RespondNexusTaskCompletedRequest{
			Namespace: d.namespace,
			Identity:  uuid.NewString(),
			TaskToken: resp.TaskToken,
			Response: &nexuspb.Response{
				Variant: &nexuspb.Response_StartOperation{
					StartOperation: &nexuspb.StartOperationResponse{
						Variant: &nexuspb.StartOperationResponse_SyncSuccess{
							SyncSuccess: &nexuspb.StartOperationResponse_Sync{
								Payload: &commonpb.Payload{},
							},
						},
					},
				},
			},
		})
		t.NoError(err)
		d.recordTask(op.OperationID, TaskKindStart, TaskOutcomeCompleted)
		d.setStatus(op, StatusCompleted)
		return
	}

	if cancel := resp.GetRequest().GetCancelOperation(); cancel != nil {
		op := d.opForTask(TaskKindCancel, cancel.GetOperationToken())
		t.NotNil(op, "unknown operation %s", cancel.GetOperationToken())
		_, err := d.client.RespondNexusTaskCompleted(d.ctx, &workflowservice.RespondNexusTaskCompletedRequest{
			Namespace: d.namespace,
			Identity:  uuid.NewString(),
			TaskToken: resp.TaskToken,
			Response: &nexuspb.Response{
				Variant: &nexuspb.Response_CancelOperation{
					CancelOperation: &nexuspb.CancelOperationResponse{},
				},
			},
		})
		t.NoError(err)
		d.recordTask(op.OperationID, TaskKindCancel, TaskOutcomeCompleted)
		d.setStatus(op, StatusCanceled)
	}
}

func (d *driver) terminateOperation(t *umpire.T) {
	op := d.store.Pick(t, "standaloneNexusRunningOp", isRunning)

	_, err := d.client.TerminateNexusOperationExecution(d.ctx, &workflowservice.TerminateNexusOperationExecutionRequest{
		Namespace:   d.namespace,
		OperationId: op.OperationID,
		RunId:       op.RunID,
		RequestId:   fmt.Sprintf("term-%s", op.OperationID),
		Reason:      "prop-test termination",
	})
	t.NoError(err)
	// Status transition recorded server-side via the observer.
}

func (d *driver) cancelOperation(t *umpire.T) {
	op := d.store.Pick(t, "standaloneNexusCancelableOp", isCancelable)

	_, err := d.client.RequestCancelNexusOperationExecution(d.ctx, &workflowservice.RequestCancelNexusOperationExecutionRequest{
		Namespace:   d.namespace,
		OperationId: op.OperationID,
		RunId:       op.RunID,
		RequestId:   fmt.Sprintf("cancel-%s", op.OperationID),
	})
	t.NoError(err)
	// CancelRequested flag set server-side via the observer.
}

func (d *driver) describeOperation(t *umpire.T) {
	op := d.store.Pick(t, "standaloneNexusAnyOp", anyOp)

	resp, err := d.client.DescribeNexusOperationExecution(d.ctx, &workflowservice.DescribeNexusOperationExecutionRequest{
		Namespace:   d.namespace,
		OperationId: op.OperationID,
		RunId:       op.RunID,
	})
	if isDeletedOpErr(err, op) {
		return
	}
	t.NoError(err)
	t.Equal(ToProto(op.Status), resp.GetInfo().GetStatus(),
		"operation %s: expected %v, got %v", op.OperationID, ToProto(op.Status), resp.GetInfo().GetStatus())
	t.Equal(op.RunID, resp.RunId, "operation %s: run ID mismatch", op.OperationID)
}

func (d *driver) CheckInvariant(t *umpire.T) {
	for _, op := range d.store.All() {
		resp, err := d.client.DescribeNexusOperationExecution(d.ctx, &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:   d.namespace,
			OperationId: op.OperationID,
			RunId:       op.RunID,
		})
		if isDeletedOpErr(err, op) {
			continue
		}
		t.NoError(err)
		t.Equal(ToProto(op.Status), resp.GetInfo().GetStatus(),
			"invariant: operation %s status mismatch", op.OperationID)
	}
}

func (d *driver) Cleanup(t *umpire.T) {
	for _, op := range d.store.All() {
		if op.Status == StatusRunning {
			_, _ = d.client.TerminateNexusOperationExecution(d.ctx, &workflowservice.TerminateNexusOperationExecutionRequest{
				Namespace:   d.namespace,
				OperationId: op.OperationID,
				RunId:       op.RunID,
				RequestId:   fmt.Sprintf("cleanup-term-%s", op.OperationID),
				Reason:      "prop-test cleanup",
			})
		}
		t.Eventually(func() bool {
			_, _ = d.client.DeleteNexusOperationExecution(d.ctx, &workflowservice.DeleteNexusOperationExecutionRequest{
				Namespace:   d.namespace,
				OperationId: op.OperationID,
				RunId:       op.RunID,
			})
			_, err := d.client.DescribeNexusOperationExecution(d.ctx, &workflowservice.DescribeNexusOperationExecutionRequest{
				Namespace:   d.namespace,
				OperationId: op.OperationID,
				RunId:       op.RunID,
			})
			var notFoundErr *serviceerror.NotFound
			return errors.As(err, &notFoundErr)
		}, 10*time.Second, 200*time.Millisecond, "operation %s was not deleted during cleanup", op.OperationID)
	}
}

func (d *driver) recordTask(operationID string, kind TaskKind, outcome TaskOutcome) {
	d.u.Record(&TaskEvent{OperationID: operationID, Kind: kind, Outcome: outcome})
}

func (d *driver) setStatus(op *Entity, to Status) {
	umpire.RecordTransition(d.u, op.OperationID, &op.Status, to)
}

func (d *driver) opForTask(kind TaskKind, taskOperationID string) *Entity {
	for _, op := range d.store.All() {
		switch kind {
		case TaskKindCancel:
			if op.OperationID == taskOperationID {
				return op
			}
		default:
			if op.RequestID == taskOperationID {
				return op
			}
		}
	}
	return nil
}

func (d *driver) pollTask(t *umpire.T) *workflowservice.PollNexusTaskQueueResponse {
	t.True(d.hasRunningOp(), "poll requires a running operation")

	pollTimeout := 3 * time.Second
	deadline := time.Now().Add(2 * pollTimeout)
	for time.Now().Before(deadline) {
		pollCtx, cancel := context.WithTimeout(d.ctx, pollTimeout)
		resp, err := d.client.PollNexusTaskQueue(pollCtx, &workflowservice.PollNexusTaskQueueRequest{
			Namespace: d.namespace,
			Identity:  uuid.NewString(),
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: d.taskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
		})
		timedOut := pollCtx.Err() != nil
		cancel()
		if timedOut {
			break
		}
		t.NoError(err)
		if resp.GetTaskToken() == nil {
			continue
		}
		operationID, kind, ok := taskInfoFromPoll(resp)
		t.True(ok, "unknown nexus task")
		if op := d.opForTask(kind, operationID); op != nil && op.Status == StatusRunning {
			d.recordTask(op.OperationID, kind, TaskOutcomeDispatched)
			return resp
		}
	}
	t.Skip("no task available")
	return nil
}

func (d *driver) hasRunningOp() bool    { return d.store.Any(isRunning) }
func (d *driver) hasCancelableOp() bool { return d.store.Any(isCancelable) }

func isRunning(op *Entity) bool { return op.Status == StatusRunning }

func isCancelable(op *Entity) bool {
	return op.Status == StatusRunning && !op.CancelRequested
}

func anyOp(*Entity) bool { return true }

func isDeletedOpErr(err error, op *Entity) bool {
	if !IsTerminal(op.Status) {
		return false
	}
	var notFoundErr *serviceerror.NotFound
	return errors.As(err, &notFoundErr)
}

func taskInfoFromPoll(resp *workflowservice.PollNexusTaskQueueResponse) (string, TaskKind, bool) {
	if start := resp.GetRequest().GetStartOperation(); start != nil {
		return start.GetRequestId(), TaskKindStart, true
	}
	if cancel := resp.GetRequest().GetCancelOperation(); cancel != nil {
		return cancel.GetOperationToken(), TaskKindCancel, true
	}
	return "", "", false
}
