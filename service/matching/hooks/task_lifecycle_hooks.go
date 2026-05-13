package hooks

import (
	"context"

	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/tqid"
)

// SyncMatchOutcome describes the outcome of a sync match attempt from the hook's perspective.
type SyncMatchOutcome int

const (
	// Default zero value; should not be used explicitly.
	SyncMatchOutcomeUnspecified SyncMatchOutcome = iota
	// Sync match was not attempted because the backlog is too deep. The system skipped sync
	// match to preserve dispatch ordering — this is normal behavior, not a capacity problem.
	SyncMatchOutcomeBacklogPresent
	// Sync match was attempted but no poller was available.
	SyncMatchOutcomeNoPoller
	// The task was sync-matched successfully.
	SyncMatchOutcomeSuccess
	// A poller was available but rate limiting blocked the match.
	SyncMatchOutcomeRateLimited
	// A poller was available and the task was matched, but the dispatch failed internally
	// (e.g. RecordTaskStarted rejected by history due to busy workflow). This is not a poller
	// shortage — worker capacity was sufficient, but the task could not be started for reasons
	// unrelated to scaling.
	SyncMatchOutcomeStartFailed
)

type (
	// TaskQueuePartition is a simplified version of tqid.Partition that removes details
	// the hooks should not concern themselves with
	TaskQueuePartition interface {
		NamespaceId() string
		TaskQueue() *tqid.TaskQueue
		TaskType() enumspb.TaskQueueType
		Kind() enumspb.TaskQueueKind
	}

	TaskHookFactoryCreateDetails struct {
		Namespace *namespace.Namespace
		Partition TaskQueuePartition
	}
	TaskAddHookDetails struct {
		DeploymentVersion *deploymentpb.WorkerDeploymentVersion
		IsSyncMatch       bool // Deprecated: use SyncMatchOutcome instead.
		SyncMatchOutcome  SyncMatchOutcome
	}

	TaskHookFactory interface {
		// Create returns a TaskHook instance that will be leveraged as part
		// of the specific task queue partition (as specified in the details).
		// This might also return nil, if no hooking into that task queue
		// partition is desired.
		Create(details *TaskHookFactoryCreateDetails) TaskHook
	}
	TaskHook interface {
		// Start is called when the task queue partition manager for the hooks partition is started
		Start()
		// Stop is called when the task queue partition manager for the hooks partition is stopped
		Stop()
		// ProcessTaskAdd is called for each Task addition (whether sync or async matching)
		ProcessTaskAdd(ctx context.Context, event *TaskAddHookDetails)
	}
)
