package persistencetests

import (
	"testing"

	"go.temporal.io/server/common/persistence"
)

func TestGarbageCleanupInfo(t *testing.T) {
	namespaceID := "10000000-5000-f000-f000-000000000000"
	workflowID := "workflow-id"
	runID := "10000000-5000-f000-f000-000000000002"

	info := persistence.BuildHistoryGarbageCleanupInfo(namespaceID, workflowID, runID)
	namespaceID2, workflowID2, runID2, err := persistence.SplitHistoryGarbageCleanupInfo(info)
	if err != nil || namespaceID != namespaceID2 || workflowID != workflowID2 || runID != runID2 {
		t.Fail()
	}
}

func TestGarbageCleanupInfo_WithColonInWorklfowID(t *testing.T) {
	namespaceID := "10000000-5000-f000-f000-000000000000"
	workflowID := "workflow-id:2"
	runID := "10000000-5000-f000-f000-000000000002"

	info := persistence.BuildHistoryGarbageCleanupInfo(namespaceID, workflowID, runID)
	namespaceID2, workflowID2, runID2, err := persistence.SplitHistoryGarbageCleanupInfo(info)
	if err != nil || namespaceID != namespaceID2 || workflowID != workflowID2 || runID != runID2 {
		t.Fail()
	}
}
