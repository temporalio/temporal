package wideevents

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParentChildLifecycleEventName(t *testing.T) {
	require.Equal(t, "parent_child_lifecycle", ParentChildLifecyclePayload{}.EventName())
}

func TestParentResendLifecycleVocabulary(t *testing.T) {
	require.Equal(t, "parent_resend", string(ParentChildPhaseParentResend))
	require.Equal(t, "scheduled", string(ParentChildOutcomeScheduled))
	require.Equal(t, "started", string(ParentChildOutcomeStarted))
	require.Equal(t, "verified", string(ParentChildOutcomeVerified))
	require.Equal(t, "ignored", string(ParentChildOutcomeIgnored))
	require.Equal(t, "succeeded", string(ParentChildOutcomeSucceeded))
	require.Equal(t, "source_not_found", string(ParentChildOutcomeSourceNotFound))
	require.Equal(t, "deduplicated", string(ParentChildOutcomeDeduplicated))
	require.Equal(t, "limited", string(ParentChildOutcomeLimited))
}

func TestParentChildLifecycleFieldSetLocked(t *testing.T) {
	want := map[string]any{
		"phase":                    "verify_child_completion",
		"outcome":                  "completion_missing",
		"local_cluster":            "cluster-b",
		"local_shard":              int64(2),
		"parent_namespace_id":      "parent-ns-id",
		"parent_workflow_id":       "parent-wf",
		"parent_run_id":            "parent-run",
		"child_namespace_id":       "child-ns-id",
		"child_workflow_id":        "child-wf",
		"child_run_id":             "child-run",
		"parent_initiated_id":      int64(7),
		"parent_initiated_version": int64(11),
		"local_task_id":            int64(42),
		"local_task_type":          "TRANSFER_TASK_TYPE_CLOSE_EXECUTION",
		"local_task_version":       int64(13),
		"error":                    "parent completion is missing",
		"error_type":               "WorkflowNotReady",
		"details":                  `{"resend_parent_requested":true,"verification_scope":"passive"}`,
	}

	got := valueMap(ParentChildLifecyclePayload{
		Phase:                  ParentChildPhaseVerifyChildCompletion,
		Outcome:                ParentChildOutcomeCompletionMissing,
		LocalCluster:           "cluster-b",
		LocalShard:             2,
		ParentNamespaceID:      "parent-ns-id",
		ParentWorkflowID:       "parent-wf",
		ParentRunID:            "parent-run",
		ChildNamespaceID:       "child-ns-id",
		ChildWorkflowID:        "child-wf",
		ChildRunID:             "child-run",
		ParentInitiatedID:      7,
		ParentInitiatedVersion: 11,
		LocalTaskID:            42,
		LocalTaskType:          "TRANSFER_TASK_TYPE_CLOSE_EXECUTION",
		LocalTaskVersion:       13,
		Error:                  "parent completion is missing",
		ErrorType:              "WorkflowNotReady",
		Details: map[string]any{
			"resend_parent_requested": true,
			"verification_scope":      "passive",
		},
	}.Attributes())

	require.Equal(t, want, got)
}

func TestEmitParentChildLifecycleNilSafe(t *testing.T) {
	require.NotPanics(t, func() {
		Emit(nil, ParentChildLifecyclePayload{Phase: ParentChildPhaseChildStart})
	})
}
