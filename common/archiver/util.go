// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package archiver

import (
	"errors"

	commonpb "go.temporal.io/temporal-proto/common/v1"

	archiverspb "go.temporal.io/server/api/archiver/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payload"
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
func TagLoggerWithArchiveVisibilityRequestAndURI(logger log.Logger, request *archiverspb.ArchiveVisibilityRequest, URI string) log.Logger {
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
func ValidateVisibilityArchivalRequest(request *archiverspb.ArchiveVisibilityRequest) error {
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

// ConvertSearchAttrToPayload converts search attribute value from string back to byte array
func ConvertSearchAttrToPayload(searchAttrStr map[string]string) map[string]*commonpb.Payload {
	searchAttr := make(map[string]*commonpb.Payload)
	for k, v := range searchAttrStr {
		searchAttr[k] = payload.EncodeBytes([]byte(v))
	}
	return searchAttr
}
