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

package visibility

import (
	"context"
	"strings"

	"go.temporal.io/server/common/persistence/visibility/manager"
)

type (
	visibilityManagerDual struct {
		visibilityManager          manager.VisibilityManager
		secondaryVisibilityManager manager.VisibilityManager
		managerSelector            managerSelector
	}
)

var _ manager.VisibilityManager = (*visibilityManagerDual)(nil)

// NewVisibilityManagerDual create a visibility manager that operate on multiple manager
// implementations based on dynamic config.
func NewVisibilityManagerDual(
	visibilityManager manager.VisibilityManager,
	secondaryVisibilityManager manager.VisibilityManager,
	managerSelector managerSelector,
) *visibilityManagerDual {
	return &visibilityManagerDual{
		visibilityManager:          visibilityManager,
		secondaryVisibilityManager: secondaryVisibilityManager,
		managerSelector:            managerSelector,
	}
}

func (v *visibilityManagerDual) Close() {
	v.visibilityManager.Close()
	v.secondaryVisibilityManager.Close()
}

func (v *visibilityManagerDual) GetName() string {
	return strings.Join([]string{v.visibilityManager.GetName(), v.secondaryVisibilityManager.GetName()}, ",")
}

func (v *visibilityManagerDual) RecordWorkflowExecutionStarted(
	ctx context.Context,
	request *manager.RecordWorkflowExecutionStartedRequest,
) error {
	ms, err := v.managerSelector.writeManagers()
	if err != nil {
		return err
	}
	for _, m := range ms {
		err = m.RecordWorkflowExecutionStarted(ctx, request)
		if err != nil {
			return err
		}
	}
	return nil
}

func (v *visibilityManagerDual) RecordWorkflowExecutionClosed(
	ctx context.Context,
	request *manager.RecordWorkflowExecutionClosedRequest,
) error {
	ms, err := v.managerSelector.writeManagers()
	if err != nil {
		return err
	}
	for _, m := range ms {
		err = m.RecordWorkflowExecutionClosed(ctx, request)
		if err != nil {
			return err
		}
	}
	return nil
}

func (v *visibilityManagerDual) UpsertWorkflowExecution(
	ctx context.Context,
	request *manager.UpsertWorkflowExecutionRequest,
) error {
	ms, err := v.managerSelector.writeManagers()
	if err != nil {
		return err
	}
	for _, m := range ms {
		err = m.UpsertWorkflowExecution(ctx, request)
		if err != nil {
			return err
		}
	}
	return nil
}

func (v *visibilityManagerDual) DeleteWorkflowExecution(
	ctx context.Context,
	request *manager.VisibilityDeleteWorkflowExecutionRequest,
) error {
	ms, err := v.managerSelector.writeManagers()
	if err != nil {
		return err
	}
	for _, m := range ms {
		err = m.DeleteWorkflowExecution(ctx, request)
		if err != nil {
			return err
		}
	}
	return nil
}

func (v *visibilityManagerDual) ListOpenWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequest,
) (*manager.ListWorkflowExecutionsResponse, error) {
	return v.managerSelector.readManager(request.Namespace).ListOpenWorkflowExecutions(ctx, request)
}

func (v *visibilityManagerDual) ListClosedWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequest,
) (*manager.ListWorkflowExecutionsResponse, error) {
	return v.managerSelector.readManager(request.Namespace).ListClosedWorkflowExecutions(ctx, request)
}

func (v *visibilityManagerDual) ListOpenWorkflowExecutionsByType(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByTypeRequest,
) (*manager.ListWorkflowExecutionsResponse, error) {
	return v.managerSelector.readManager(request.Namespace).ListOpenWorkflowExecutionsByType(ctx, request)
}

func (v *visibilityManagerDual) ListClosedWorkflowExecutionsByType(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByTypeRequest,
) (*manager.ListWorkflowExecutionsResponse, error) {
	return v.managerSelector.readManager(request.Namespace).ListClosedWorkflowExecutionsByType(ctx, request)
}

func (v *visibilityManagerDual) ListOpenWorkflowExecutionsByWorkflowID(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByWorkflowIDRequest,
) (*manager.ListWorkflowExecutionsResponse, error) {
	return v.managerSelector.readManager(request.Namespace).ListOpenWorkflowExecutionsByWorkflowID(ctx, request)
}

func (v *visibilityManagerDual) ListClosedWorkflowExecutionsByWorkflowID(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByWorkflowIDRequest,
) (*manager.ListWorkflowExecutionsResponse, error) {
	return v.managerSelector.readManager(request.Namespace).ListClosedWorkflowExecutionsByWorkflowID(ctx, request)
}

func (v *visibilityManagerDual) ListClosedWorkflowExecutionsByStatus(
	ctx context.Context,
	request *manager.ListClosedWorkflowExecutionsByStatusRequest,
) (*manager.ListWorkflowExecutionsResponse, error) {
	return v.managerSelector.readManager(request.Namespace).ListClosedWorkflowExecutionsByStatus(ctx, request)
}

func (v *visibilityManagerDual) ListWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequestV2,
) (*manager.ListWorkflowExecutionsResponse, error) {
	return v.managerSelector.readManager(request.Namespace).ListWorkflowExecutions(ctx, request)
}

func (v *visibilityManagerDual) ScanWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequestV2,
) (*manager.ListWorkflowExecutionsResponse, error) {
	return v.managerSelector.readManager(request.Namespace).ScanWorkflowExecutions(ctx, request)
}

func (v *visibilityManagerDual) CountWorkflowExecutions(
	ctx context.Context,
	request *manager.CountWorkflowExecutionsRequest,
) (*manager.CountWorkflowExecutionsResponse, error) {
	return v.managerSelector.readManager(request.Namespace).CountWorkflowExecutions(ctx, request)
}
