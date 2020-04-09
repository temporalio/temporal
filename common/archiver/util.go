package archiver

import (
	"errors"

	archivergenpb "github.com/temporalio/temporal/.gen/proto/archiver"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
)

var (
	errEmptyNamespaceID      = errors.New("NamespaceId is empty")
	errEmptyNamespace        = errors.New("Namespace is empty")
	errEmptyWorkflowID       = errors.New("WorkflowId is empty")
	errEmptyRunID            = errors.New("RunId is empty")
	errInvalidPageSize       = errors.New("PageSize should be greater than 0")
	errEmptyWorkflowTypeName = errors.New("WorkflowTypeName is empty")
	errEmptyStartTime        = errors.New("StartTimestamp is empty")
	errEmptyCloseTime        = errors.New("CloseTimestamp is empty")
	errEmptyQuery            = errors.New("Query string is empty")
)

// TagLoggerWithArchiveHistoryRequestAndURI tags logger with fields in the archive history request and the URI
func TagLoggerWithArchiveHistoryRequestAndURI(logger log.Logger, request *ArchiveHistoryRequest, URI string) log.Logger {
	return logger.WithTags(
		tag.ShardID(request.ShardID),
		tag.ArchivalRequestNamespaceID(request.NamespaceID),
		tag.ArchivalRequestNamespace(request.Namespace),
		tag.ArchivalRequestWorkflowID(request.WorkflowID),
		tag.ArchivalRequestRunID(request.RunID),
		tag.ArchivalRequestBranchToken(request.BranchToken),
		tag.ArchivalRequestNextEventID(request.NextEventID),
		tag.ArchivalRequestCloseFailoverVersion(request.CloseFailoverVersion),
		tag.ArchivalURI(URI),
	)
}

// TagLoggerWithArchiveVisibilityRequestAndURI tags logger with fields in the archive visibility request and the URI
func TagLoggerWithArchiveVisibilityRequestAndURI(logger log.Logger, request *archivergenpb.ArchiveVisibilityRequest, URI string) log.Logger {
	return logger.WithTags(
		tag.ArchivalRequestNamespaceID(request.GetNamespaceId()),
		tag.ArchivalRequestNamespace(request.GetNamespace()),
		tag.ArchivalRequestWorkflowID(request.GetWorkflowId()),
		tag.ArchivalRequestRunID(request.GetRunId()),
		tag.ArchvialRequestWorkflowType(request.GetWorkflowTypeName()),
		tag.ArchivalRequestCloseTimestamp(request.GetCloseTimestamp()),
		tag.ArchivalRequestStatus(request.GetStatus().String()),
		tag.ArchivalURI(URI),
	)
}

// ValidateHistoryArchiveRequest validates the archive history request
func ValidateHistoryArchiveRequest(request *ArchiveHistoryRequest) error {
	if request.NamespaceID == "" {
		return errEmptyNamespaceID
	}
	if request.WorkflowID == "" {
		return errEmptyWorkflowID
	}
	if request.RunID == "" {
		return errEmptyRunID
	}
	if request.Namespace == "" {
		return errEmptyNamespace
	}
	return nil
}

// ValidateGetRequest validates the get archived history request
func ValidateGetRequest(request *GetHistoryRequest) error {
	if request.NamespaceID == "" {
		return errEmptyNamespaceID
	}
	if request.WorkflowID == "" {
		return errEmptyWorkflowID
	}
	if request.RunID == "" {
		return errEmptyRunID
	}
	if request.PageSize == 0 {
		return errInvalidPageSize
	}
	return nil
}

// ValidateVisibilityArchivalRequest validates the archive visibility request
func ValidateVisibilityArchivalRequest(request *archivergenpb.ArchiveVisibilityRequest) error {
	if request.GetNamespaceId() == "" {
		return errEmptyNamespaceID
	}
	if request.GetNamespace() == "" {
		return errEmptyNamespace
	}
	if request.GetWorkflowId() == "" {
		return errEmptyWorkflowID
	}
	if request.GetRunId() == "" {
		return errEmptyRunID
	}
	if request.GetWorkflowTypeName() == "" {
		return errEmptyWorkflowTypeName
	}
	if request.GetStartTimestamp() == 0 {
		return errEmptyStartTime
	}
	if request.GetCloseTimestamp() == 0 {
		return errEmptyCloseTime
	}
	return nil
}

// ValidateQueryRequest validates the query visibility request
func ValidateQueryRequest(request *QueryVisibilityRequest) error {
	if request.NamespaceID == "" {
		return errEmptyNamespaceID
	}
	if request.PageSize == 0 {
		return errInvalidPageSize
	}
	if request.Query == "" {
		return errEmptyQuery
	}
	return nil
}

// ConvertSearchAttrToBytes converts search attribute value from string back to byte array
func ConvertSearchAttrToBytes(searchAttrStr map[string]string) map[string][]byte {
	searchAttr := make(map[string][]byte)
	for k, v := range searchAttrStr {
		searchAttr[k] = []byte(v)
	}
	return searchAttr
}
