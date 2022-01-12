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
	return "VisibilityManagerDual"
}

func (v *visibilityManagerDual) RecordWorkflowExecutionStarted(request *manager.RecordWorkflowExecutionStartedRequest) error {
	ms, err := v.managerSelector.writeManagers()
	if err != nil {
		return err
	}
	for _, m := range ms {
		err = m.RecordWorkflowExecutionStarted(request)
		if err != nil {
			return err
		}
	}
	return nil
}

func (v *visibilityManagerDual) RecordWorkflowExecutionClosed(request *manager.RecordWorkflowExecutionClosedRequest) error {
	ms, err := v.managerSelector.writeManagers()
	if err != nil {
		return err
	}
	for _, m := range ms {
		err = m.RecordWorkflowExecutionClosed(request)
		if err != nil {
			return err
		}
	}
	return nil
}

func (v *visibilityManagerDual) UpsertWorkflowExecution(request *manager.UpsertWorkflowExecutionRequest) error {
	ms, err := v.managerSelector.writeManagers()
	if err != nil {
		return err
	}
	for _, m := range ms {
		err = m.UpsertWorkflowExecution(request)
		if err != nil {
			return err
		}
	}
	return nil
}

func (v *visibilityManagerDual) DeleteWorkflowExecution(request *manager.VisibilityDeleteWorkflowExecutionRequest) error {
	ms, err := v.managerSelector.writeManagers()
	if err != nil {
		return err
	}
	for _, m := range ms {
		err = m.DeleteWorkflowExecution(request)
		if err != nil {
			return err
		}
	}
	return nil
}

func (v *visibilityManagerDual) ListOpenWorkflowExecutions(request *manager.ListWorkflowExecutionsRequest) (*manager.ListWorkflowExecutionsResponse, error) {
	return v.managerSelector.readManager(request.Namespace).ListOpenWorkflowExecutions(request)
}

func (v *visibilityManagerDual) ListClosedWorkflowExecutions(request *manager.ListWorkflowExecutionsRequest) (*manager.ListWorkflowExecutionsResponse, error) {
	return v.managerSelector.readManager(request.Namespace).ListClosedWorkflowExecutions(request)
}

func (v *visibilityManagerDual) ListOpenWorkflowExecutionsByType(request *manager.ListWorkflowExecutionsByTypeRequest) (*manager.ListWorkflowExecutionsResponse, error) {
	return v.managerSelector.readManager(request.Namespace).ListOpenWorkflowExecutionsByType(request)
}

func (v *visibilityManagerDual) ListClosedWorkflowExecutionsByType(request *manager.ListWorkflowExecutionsByTypeRequest) (*manager.ListWorkflowExecutionsResponse, error) {
	return v.managerSelector.readManager(request.Namespace).ListClosedWorkflowExecutionsByType(request)
}

func (v *visibilityManagerDual) ListOpenWorkflowExecutionsByWorkflowID(request *manager.ListWorkflowExecutionsByWorkflowIDRequest) (*manager.ListWorkflowExecutionsResponse, error) {
	return v.managerSelector.readManager(request.Namespace).ListOpenWorkflowExecutionsByWorkflowID(request)
}

func (v *visibilityManagerDual) ListClosedWorkflowExecutionsByWorkflowID(request *manager.ListWorkflowExecutionsByWorkflowIDRequest) (*manager.ListWorkflowExecutionsResponse, error) {
	return v.managerSelector.readManager(request.Namespace).ListClosedWorkflowExecutionsByWorkflowID(request)
}

func (v *visibilityManagerDual) ListClosedWorkflowExecutionsByStatus(request *manager.ListClosedWorkflowExecutionsByStatusRequest) (*manager.ListWorkflowExecutionsResponse, error) {
	return v.managerSelector.readManager(request.Namespace).ListClosedWorkflowExecutionsByStatus(request)
}

func (v *visibilityManagerDual) ListWorkflowExecutions(request *manager.ListWorkflowExecutionsRequestV2) (*manager.ListWorkflowExecutionsResponse, error) {
	return v.managerSelector.readManager(request.Namespace).ListWorkflowExecutions(request)
}

func (v *visibilityManagerDual) ScanWorkflowExecutions(request *manager.ListWorkflowExecutionsRequestV2) (*manager.ListWorkflowExecutionsResponse, error) {
	return v.managerSelector.readManager(request.Namespace).ScanWorkflowExecutions(request)
}

func (v *visibilityManagerDual) CountWorkflowExecutions(request *manager.CountWorkflowExecutionsRequest) (*manager.CountWorkflowExecutionsResponse, error) {
	return v.managerSelector.readManager(request.Namespace).CountWorkflowExecutions(request)
}
