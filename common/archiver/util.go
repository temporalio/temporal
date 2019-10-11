// Copyright (c) 2019 Uber Technologies, Inc.
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

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
)

var (
	errEmptyDomainID         = errors.New("DomainID is empty")
	errEmptyDomainName       = errors.New("Domain name is empty")
	errEmptyWorkflowID       = errors.New("WorkflowID is empty")
	errEmptyRunID            = errors.New("RunID is empty")
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
		tag.ArchivalRequestDomainID(request.DomainID),
		tag.ArchivalRequestDomainName(request.DomainName),
		tag.ArchivalRequestWorkflowID(request.WorkflowID),
		tag.ArchivalRequestRunID(request.RunID),
		tag.ArchivalRequestBranchToken(request.BranchToken),
		tag.ArchivalRequestNextEventID(request.NextEventID),
		tag.ArchivalRequestCloseFailoverVersion(request.CloseFailoverVersion),
		tag.ArchivalURI(URI),
	)
}

// TagLoggerWithArchiveVisibilityRequestAndURI tags logger with fields in the archive visibility request and the URI
func TagLoggerWithArchiveVisibilityRequestAndURI(logger log.Logger, request *ArchiveVisibilityRequest, URI string) log.Logger {
	return logger.WithTags(
		tag.ArchivalRequestDomainID(request.DomainID),
		tag.ArchivalRequestDomainName(request.DomainName),
		tag.ArchivalRequestWorkflowID(request.WorkflowID),
		tag.ArchivalRequestRunID(request.RunID),
		tag.ArchvialRequestWorkflowType(request.WorkflowTypeName),
		tag.ArchivalRequestCloseTimestamp(request.CloseTimestamp),
		tag.ArchivalRequestCloseStatus(request.CloseStatus.String()),
		tag.ArchivalURI(URI),
	)
}

// ValidateHistoryArchiveRequest validates the archive history request
func ValidateHistoryArchiveRequest(request *ArchiveHistoryRequest) error {
	if request.DomainID == "" {
		return errEmptyDomainID
	}
	if request.WorkflowID == "" {
		return errEmptyWorkflowID
	}
	if request.RunID == "" {
		return errEmptyRunID
	}
	if request.DomainName == "" {
		return errEmptyDomainName
	}
	return nil
}

// ValidateGetRequest validates the get archived history request
func ValidateGetRequest(request *GetHistoryRequest) error {
	if request.DomainID == "" {
		return errEmptyDomainID
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
func ValidateVisibilityArchivalRequest(request *ArchiveVisibilityRequest) error {
	if request.DomainID == "" {
		return errEmptyDomainID
	}
	if request.DomainName == "" {
		return errEmptyDomainName
	}
	if request.WorkflowID == "" {
		return errEmptyWorkflowID
	}
	if request.RunID == "" {
		return errEmptyRunID
	}
	if request.WorkflowTypeName == "" {
		return errEmptyWorkflowTypeName
	}
	if request.StartTimestamp == 0 {
		return errEmptyStartTime
	}
	if request.CloseTimestamp == 0 {
		return errEmptyCloseTime
	}
	return nil
}

// ValidateQueryRequest validates the query visibility request
func ValidateQueryRequest(request *QueryVisibilityRequest) error {
	if request.DomainID == "" {
		return errEmptyDomainID
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
