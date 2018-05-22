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
	"context"

	"strconv"

	"github.com/uber/cadence/.gen/go/admin"
	"github.com/uber/cadence/.gen/go/admin/adminserviceserver"
	gen "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/service"
)

var _ adminserviceserver.Interface = (*AdminHandler)(nil)

type (
	// AdminHandler - Thrift handler inteface for admin service
	AdminHandler struct {
		numberOfHistoryShards int
		service.Service
	}
)

// NewAdminHandler creates a thrift handler for the cadence admin service
func NewAdminHandler(
	sVice service.Service, numberOfHistoryShards int) *AdminHandler {
	handler := &AdminHandler{
		numberOfHistoryShards: numberOfHistoryShards,
		Service:               sVice,
	}
	return handler
}

// Start starts the handler
func (adh *AdminHandler) Start() error {
	adh.Service.GetDispatcher().Register(adminserviceserver.New(adh))
	adh.Service.Start()
	return nil
}

// Stop stops the handler
func (adh *AdminHandler) Stop() {
	adh.Service.Stop()
}

// InquiryWorkflowExecution returns information about the specified workflow execution.
func (adh *AdminHandler) InquiryWorkflowExecution(ctx context.Context, request *gen.DescribeWorkflowExecutionRequest) (*admin.InquiryWorkflowExecutionResponse, error) {
	if request == nil {
		return nil, adh.error(errRequestNotSet)
	}

	if err := validateExecution(request.Execution); err != nil {
		return nil, adh.error(err)
	}

	shardID := common.WorkflowIDToHistoryShard(*request.Execution.WorkflowId, adh.numberOfHistoryShards)
	shardIDstr := string(shardID)
	shardIDForOutput := strconv.Itoa(shardID)

	historyHost, err := adh.GetMembershipMonitor().Lookup(common.HistoryServiceName, shardIDstr)
	if err != nil {
		return nil, adh.error(err)
	}

	historyAddr := historyHost.GetAddress()

	return &admin.InquiryWorkflowExecutionResponse{
		ShardId:     common.StringPtr(shardIDForOutput),
		HistoryAddr: common.StringPtr(historyAddr),
	}, nil
}

func (adh *AdminHandler) error(err error) error {
	switch err.(type) {
	case *gen.InternalServiceError:
		logging.LogInternalServiceError(adh.Service.GetLogger(), err)
		return err
	case *gen.BadRequestError:
		return err
	case *gen.ServiceBusyError:
		return err
	case *gen.EntityNotExistsError:
		return err
	default:
		logging.LogUncategorizedError(adh.Service.GetLogger(), err)
		return &gen.InternalServiceError{Message: err.Error()}
	}
}
