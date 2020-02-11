// Copyright (c) 2017 Uber Technologies, Inc.
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

package frontend

import (
	"errors"

	"github.com/pborman/uuid"

	"github.com/temporalio/temporal/.gen/go/admin"
	gen "github.com/temporalio/temporal/.gen/go/shared"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/resource"
	"github.com/temporalio/temporal/common/service"
)

type (
	// AdminHandler - Thrift handler interface for admin service
	AdminHandler struct {
		resource.Resource

		numberOfHistoryShards int
		params                *service.BootstrapParams
		config                *Config
	}

	getWorkflowRawHistoryV2Token struct {
		DomainName        string
		WorkflowID        string
		RunID             string
		StartEventID      int64
		StartEventVersion int64
		EndEventID        int64
		EndEventVersion   int64
		PersistenceToken  []byte
		VersionHistories  *gen.VersionHistories
	}
)

// NewAdminHandler creates a thrift handler for the cadence admin service
func NewAdminHandler(
	resource resource.Resource,
	params *service.BootstrapParams,
	config *Config,
) *AdminHandler {
	return &AdminHandler{
		Resource:              resource,
		numberOfHistoryShards: params.PersistenceConfig.NumHistoryShards,
		params:                params,
		config:                config,
	}
}

func (adh *AdminHandler) validateGetWorkflowExecutionRawHistoryV2Request(
	request *admin.GetWorkflowExecutionRawHistoryV2Request,
) error {

	execution := request.Execution
	if len(execution.GetWorkflowId()) == 0 {
		return &gen.BadRequestError{Message: "Invalid WorkflowID."}
	}
	// TODO currently, this API is only going to be used by re-send history events
	// to remote cluster if kafka is lossy again, in the future, this API can be used
	// by CLI and client, then empty runID (meaning the current workflow) should be allowed
	if len(execution.GetRunId()) == 0 || uuid.Parse(execution.GetRunId()) == nil {
		return &gen.BadRequestError{Message: "Invalid RunID."}
	}

	pageSize := int(request.GetMaximumPageSize())
	if pageSize <= 0 {
		return &gen.BadRequestError{Message: "Invalid PageSize."}
	}

	if request.GetStartEventId() == common.EmptyEventID &&
		request.GetStartEventVersion() == common.EmptyVersion &&
		request.GetEndEventId() == common.EmptyEventID &&
		request.GetEndEventVersion() == common.EmptyVersion {
		return &gen.BadRequestError{Message: "Invalid event query range."}
	}

	if (request.GetStartEventId() != common.EmptyEventID && request.GetStartEventVersion() == common.EmptyVersion) ||
		(request.GetStartEventId() == common.EmptyEventID && request.GetStartEventVersion() != common.EmptyVersion) {
		return &gen.BadRequestError{Message: "Invalid start event id and start event version combination."}
	}

	if (request.GetEndEventId() != common.EmptyEventID && request.GetEndEventVersion() == common.EmptyVersion) ||
		(request.GetEndEventId() == common.EmptyEventID && request.GetEndEventVersion() != common.EmptyVersion) {
		return &gen.BadRequestError{Message: "Invalid end event id and end event version combination."}
	}
	return nil
}

func (adh *AdminHandler) validateConfigForAdvanceVisibility() error {
	if adh.params.ESConfig == nil || adh.params.ESClient == nil {
		return errors.New("ES related config not found")
	}
	return nil
}

func (adh *AdminHandler) setRequestDefaultValueAndGetTargetVersionHistory(
	request *admin.GetWorkflowExecutionRawHistoryV2Request,
	versionHistories *persistence.VersionHistories,
) (*persistence.VersionHistory, error) {

	targetBranch, err := versionHistories.GetCurrentVersionHistory()
	if err != nil {
		return nil, err
	}
	firstItem, err := targetBranch.GetFirstItem()
	if err != nil {
		return nil, err
	}
	lastItem, err := targetBranch.GetLastItem()
	if err != nil {
		return nil, err
	}

	if request.GetStartEventId() == common.EmptyVersion || request.GetStartEventVersion() == common.EmptyVersion {
		// If start event is not set, get the events from the first event
		// As the API is exclusive-exclusive, use first event id - 1 here
		request.StartEventId = common.Int64Ptr(common.FirstEventID - 1)
		request.StartEventVersion = common.Int64Ptr(firstItem.GetVersion())
	}
	if request.GetEndEventId() == common.EmptyEventID || request.GetEndEventVersion() == common.EmptyVersion {
		// If end event is not set, get the events until the end event
		// As the API is exclusive-exclusive, use end event id + 1 here
		request.EndEventId = common.Int64Ptr(lastItem.GetEventID() + 1)
		request.EndEventVersion = common.Int64Ptr(lastItem.GetVersion())
	}

	if request.GetStartEventId() < 0 {
		return nil, &gen.BadRequestError{Message: "Invalid FirstEventID && NextEventID combination."}
	}

	// get branch based on the end event if end event is defined in the request
	if request.GetEndEventId() == lastItem.GetEventID()+1 &&
		request.GetEndEventVersion() == lastItem.GetVersion() {
		// this is a special case, target branch remains the same
	} else {
		endItem := persistence.NewVersionHistoryItem(request.GetEndEventId(), request.GetEndEventVersion())
		idx, err := versionHistories.FindFirstVersionHistoryIndexByItem(endItem)
		if err != nil {
			return nil, err
		}

		targetBranch, err = versionHistories.GetVersionHistory(idx)
		if err != nil {
			return nil, err
		}
	}

	startItem := persistence.NewVersionHistoryItem(request.GetStartEventId(), request.GetStartEventVersion())
	// If the request start event is defined. The start event may be on a different branch as current branch.
	// We need to find the LCA of the start event and the current branch.
	if request.GetStartEventId() == common.FirstEventID-1 &&
		request.GetStartEventVersion() == firstItem.GetVersion() {
		// this is a special case, start event is on the same branch as target branch
	} else {
		if !targetBranch.ContainsItem(startItem) {
			idx, err := versionHistories.FindFirstVersionHistoryIndexByItem(startItem)
			if err != nil {
				return nil, err
			}
			startBranch, err := versionHistories.GetVersionHistory(idx)
			if err != nil {
				return nil, err
			}
			startItem, err = targetBranch.FindLCAItem(startBranch)
			if err != nil {
				return nil, err
			}
			request.StartEventId = common.Int64Ptr(startItem.GetEventID())
			request.StartEventVersion = common.Int64Ptr(startItem.GetVersion())
		}
	}

	return targetBranch, nil
}

func (adh *AdminHandler) generatePaginationToken(
	request *admin.GetWorkflowExecutionRawHistoryV2Request,
	versionHistories *persistence.VersionHistories,
) *getWorkflowRawHistoryV2Token {

	execution := request.Execution
	return &getWorkflowRawHistoryV2Token{
		DomainName:        request.GetDomain(),
		WorkflowID:        execution.GetWorkflowId(),
		RunID:             execution.GetRunId(),
		StartEventID:      request.GetStartEventId(),
		StartEventVersion: request.GetStartEventVersion(),
		EndEventID:        request.GetEndEventId(),
		EndEventVersion:   request.GetEndEventVersion(),
		VersionHistories:  versionHistories.ToThrift(),
		PersistenceToken:  nil, // this is the initialized value
	}
}

func (adh *AdminHandler) validatePaginationToken(
	request *admin.GetWorkflowExecutionRawHistoryV2Request,
	token *getWorkflowRawHistoryV2Token,
) error {

	execution := request.Execution
	if request.GetDomain() != token.DomainName ||
		execution.GetWorkflowId() != token.WorkflowID ||
		execution.GetRunId() != token.RunID ||
		request.GetStartEventId() != token.StartEventID ||
		request.GetStartEventVersion() != token.StartEventVersion ||
		request.GetEndEventId() != token.EndEventID ||
		request.GetEndEventVersion() != token.EndEventVersion {
		return &gen.BadRequestError{Message: "Invalid pagination token."}
	}
	return nil
}

// startRequestProfile initiates recording of request metrics
func (adh *AdminHandler) startRequestProfile(scope int) (metrics.Scope, metrics.Stopwatch) {
	metricsScope := adh.GetMetricsClient().Scope(scope)
	sw := metricsScope.StartTimer(metrics.CadenceLatency)
	metricsScope.IncCounter(metrics.CadenceRequests)
	return metricsScope, sw
}

func (adh *AdminHandler) error(err error, scope metrics.Scope) error {
	switch err.(type) {
	case *gen.InternalServiceError:
		adh.GetLogger().Error("Internal service error", tag.Error(err))
		scope.IncCounter(metrics.CadenceFailures)
		return err
	case *gen.BadRequestError:
		scope.IncCounter(metrics.CadenceErrBadRequestCounter)
		return err
	case *gen.ServiceBusyError:
		scope.IncCounter(metrics.CadenceErrServiceBusyCounter)
		return err
	case *gen.EntityNotExistsError:
		return err
	default:
		adh.GetLogger().Error("Uncategorized error", tag.Error(err))
		scope.IncCounter(metrics.CadenceFailures)
		return &gen.InternalServiceError{Message: err.Error()}
	}
}

func convertIndexedValueTypeToESDataType(valueType gen.IndexedValueType) string {
	switch valueType {
	case gen.IndexedValueTypeString:
		return "text"
	case gen.IndexedValueTypeKeyword:
		return "keyword"
	case gen.IndexedValueTypeInt:
		return "long"
	case gen.IndexedValueTypeDouble:
		return "double"
	case gen.IndexedValueTypeBool:
		return "boolean"
	case gen.IndexedValueTypeDatetime:
		return "date"
	default:
		return ""
	}
}
