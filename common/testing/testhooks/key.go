package testhooks

type Key int

const (
	MatchingDisableSyncMatch Key = iota
	MatchingLBForceReadPartition
	MatchingLBForceWritePartition
	UpdateWithStartInBetweenLockAndStart
	UpdateWithStartOnClosingWorkflowRetry
	TaskQueuesInDeploymentSyncBatchSize
	MatchingIgnoreRoutingConfigRevisionCheck
	MatchingDeploymentRegisterErrorBackoff
	// MatchingMigrationDrainTasksLoaded is called when a draining backlog manager
	// finishes loading its initial batch of tasks into the matcher.
	// Used by tests to synchronize before polling to ensure draining tasks are available.
	MatchingMigrationDrainTasksLoaded
)
