package archiver

import (
	"bytes"
	"encoding/gob"
	"time"

	"github.com/dgryski/go-farm"
	"go.temporal.io/temporal/activity"

	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
)

// MaxArchivalIterationTimeout returns the max allowed timeout for a single iteration of archival workflow
func MaxArchivalIterationTimeout() time.Duration {
	return workflowStartToCloseTimeout / 2
}

func hash(i interface{}) uint64 {
	var b bytes.Buffer
	gob.NewEncoder(&b).Encode(i) //nolint:errcheck
	return farm.Fingerprint64(b.Bytes())
}

func hashesEqual(a []uint64, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	aMap := make(map[uint64]int)
	for _, elem := range a {
		aMap[elem] = aMap[elem] + 1
	}
	for _, elem := range b {
		count := aMap[elem]
		if count == 0 {
			return false
		}
		aMap[elem] = aMap[elem] - 1
	}
	return true
}

func tagLoggerWithHistoryRequest(logger log.Logger, request *ArchiveRequest) log.Logger {
	return logger.WithTags(
		tag.ShardID(request.ShardID),
		tag.ArchivalRequestNamespaceID(request.NamespaceID),
		tag.ArchivalRequestNamespace(request.Namespace),
		tag.ArchivalRequestWorkflowID(request.WorkflowID),
		tag.ArchivalRequestRunID(request.RunID),
		tag.ArchivalRequestBranchToken(request.BranchToken),
		tag.ArchivalRequestNextEventID(request.NextEventID),
		tag.ArchivalRequestCloseFailoverVersion(request.CloseFailoverVersion),
		tag.ArchivalURI(request.URI),
	)
}

func tagLoggerWithVisibilityRequest(logger log.Logger, request *ArchiveRequest) log.Logger {
	return logger.WithTags(
		tag.ArchivalRequestNamespaceID(request.NamespaceID),
		tag.ArchivalRequestNamespace(request.Namespace),
		tag.ArchivalRequestWorkflowID(request.WorkflowID),
		tag.ArchivalRequestRunID(request.RunID),
		tag.ArchivalURI(request.URI),
	)
}

func tagLoggerWithActivityInfo(logger log.Logger, activityInfo activity.Info) log.Logger {
	return logger.WithTags(
		tag.WorkflowID(activityInfo.WorkflowExecution.ID),
		tag.WorkflowRunID(activityInfo.WorkflowExecution.RunID),
		tag.Attempt(activityInfo.Attempt))
}

func convertSearchAttributesToString(searchAttr map[string][]byte) map[string]string {
	searchAttrStr := make(map[string]string)
	for k, v := range searchAttr {
		searchAttrStr[k] = string(v)
	}
	return searchAttrStr
}
