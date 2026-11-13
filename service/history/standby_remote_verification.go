package history

import (
	"context"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/tasks"
)

func (t *transferQueueStandbyTaskExecutor) verifyPendingExternalTaskOnSource(
	executable queues.Executable,
) standbyPostActionFn {
	return func(ctx context.Context, task tasks.Task, postActionInfo any, logger log.Logger) error {
		if postActionInfo == nil {
			return nil
		}

		registry := t.shardContext.GetNamespaceRegistry()
		entry, err := registry.GetNamespaceByID(namespace.ID(task.GetNamespaceID()))
		if err != nil {
			return consts.ErrTaskRetry
		}
		source := entry.ActiveClusterName(namespace.RoutingKey{ID: task.GetWorkflowID()})
		if source == t.clusterName {
			return consts.ErrTaskRetry
		}

		state := executable.RemoteVerificationState()
		timeSource := t.shardContext.GetTimeSource()
		if timeSource.Now().Before(state.NextAttemptTime) {
			return consts.ErrTaskRetry
		}

		failed := true
		retry := true
		defer func() {
			if !retry {
				executable.SetRemoteVerificationState(queues.RemoteVerificationState{})
				return
			}
			t.deferRemoteVerification(executable, state, failed)
		}()

		remote, err := t.clientBean.GetRemoteAdminClient(source)
		if err != nil {
			return consts.ErrTaskRetry
		}
		response, err := remote.DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
			Namespace: entry.Name().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: task.GetWorkflowID(),
				RunId:      task.GetRunID(),
			},
			Archetype:       chasm.WorkflowArchetype,
			SkipForceReload: true,
		})

		// A response from the previous active must not authorize discarding a task after another failover.
		current, lookupErr := registry.GetNamespaceByID(namespace.ID(task.GetNamespaceID()))
		if lookupErr != nil || current.ActiveClusterName(namespace.RoutingKey{ID: task.GetWorkflowID()}) != source ||
			current.FailoverVersion(task.GetWorkflowID()) != entry.FailoverVersion(task.GetWorkflowID()) {
			return consts.ErrTaskRetry
		}
		if common.IsNotFoundError(err) {
			retry = false
			return nil
		}
		if err != nil {
			logger.Warn("Unable to verify overdue standby transfer task on source.", tag.Task(task), tag.Error(err))
			return consts.ErrTaskRetry
		}

		pending, err := externalTransferTaskPending(task, response.GetDatabaseMutableState())
		if err != nil {
			return consts.ErrTaskRetry
		}
		failed = false
		if pending {
			return consts.ErrTaskRetry
		}
		retry = false
		return nil
	}
}

func (t *transferQueueStandbyTaskExecutor) deferRemoteVerification(
	executable queues.Executable,
	state queues.RemoteVerificationState,
	failed bool,
) {
	interval := max(time.Second, t.config.StandbyTaskRemoteVerificationInterval(executable.GetType()))
	if failed {
		state.Failures = min(state.Failures+1, 4)
		interval *= 1 << (state.Failures - 1)
	} else {
		state.Failures = 0
	}
	// Keep the original task timestamp for task age and queue ordering. Only remote checks are deferred.
	state.NextAttemptTime = t.shardContext.GetTimeSource().Now().Add(interval + backoff.FullJitter(interval/5))
	executable.SetRemoteVerificationState(state)
}

func externalTransferTaskPending(task tasks.Task, state *persistencespb.WorkflowMutableState) (bool, error) {
	if state.GetExecutionState() == nil {
		return false, serviceerror.NewInternal("remote mutable state has no execution state")
	}
	switch state.GetExecutionState().GetState() {
	case enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED:
		return false, nil
	case enumsspb.WORKFLOW_EXECUTION_STATE_CREATED, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING:
	default:
		return false, consts.ErrTaskRetry
	}

	switch task := task.(type) {
	case *tasks.SignalExecutionTask:
		if state.GetNextEventId() <= task.InitiatedEventID {
			return false, consts.ErrTaskRetry
		}
		info, ok := state.GetSignalInfos()[task.InitiatedEventID]
		return ok && info.GetVersion() == task.Version, nil
	case *tasks.CancelExecutionTask:
		if state.GetNextEventId() <= task.InitiatedEventID {
			return false, consts.ErrTaskRetry
		}
		info, ok := state.GetRequestCancelInfos()[task.InitiatedEventID]
		return ok && info.GetVersion() == task.Version, nil
	default:
		return false, serviceerror.NewInternal("unsupported task for remote pending-work verification")
	}
}
