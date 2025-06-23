package testhooks

type Key int

const (
	MatchingDisableSyncMatch Key = iota
	MatchingLBForceReadPartition
	MatchingLBForceWritePartition
	UpdateWithStartInBetweenLockAndStart
	UpdateWithStartOnClosingWorkflowRetry
	TaskQueuesInDeploymentSyncBatchSize
)
