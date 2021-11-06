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
	"fmt"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
)

type (
	visibilityManagerDual struct {
		stdVisibilityManager          manager.VisibilityManager
		advVisibilityManager          manager.VisibilityManager
		enableAdvancedVisibilityRead  dynamicconfig.BoolPropertyFnWithNamespaceFilter
		advancedVisibilityWritingMode dynamicconfig.StringPropertyFn
	}
)

var _ manager.VisibilityManager = (*visibilityManagerDual)(nil)

// NewVisibilityManagerDual create a visibility manager that operate on DB or Elasticsearch based on dynamic config.
func NewVisibilityManagerDual(
	stdVisibilityManager manager.VisibilityManager,
	advVisibilityManager manager.VisibilityManager,
	enableAdvancedVisibilityRead dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	advancedVisibilityWritingMode dynamicconfig.StringPropertyFn,
) *visibilityManagerDual {
	return &visibilityManagerDual{
		stdVisibilityManager:          stdVisibilityManager,
		advVisibilityManager:          advVisibilityManager,
		enableAdvancedVisibilityRead:  enableAdvancedVisibilityRead,
		advancedVisibilityWritingMode: advancedVisibilityWritingMode,
	}
}

func (v *visibilityManagerDual) Close() {
	v.stdVisibilityManager.Close()
	v.advVisibilityManager.Close()
}

func (v *visibilityManagerDual) GetName() string {
	return "VisibilityManagerDual"
}

func (v *visibilityManagerDual) RecordWorkflowExecutionStarted(request *manager.RecordWorkflowExecutionStartedRequest) error {
	ms, err := v.writeManagers()
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
	ms, err := v.writeManagers()
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
	ms, err := v.writeManagers()
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
	ms, err := v.writeManagers()
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
	return v.readManager(request.Namespace).ListOpenWorkflowExecutions(request)
}

func (v *visibilityManagerDual) ListClosedWorkflowExecutions(request *manager.ListWorkflowExecutionsRequest) (*manager.ListWorkflowExecutionsResponse, error) {
	return v.readManager(request.Namespace).ListClosedWorkflowExecutions(request)
}

func (v *visibilityManagerDual) ListOpenWorkflowExecutionsByType(request *manager.ListWorkflowExecutionsByTypeRequest) (*manager.ListWorkflowExecutionsResponse, error) {
	return v.readManager(request.Namespace).ListOpenWorkflowExecutionsByType(request)
}

func (v *visibilityManagerDual) ListClosedWorkflowExecutionsByType(request *manager.ListWorkflowExecutionsByTypeRequest) (*manager.ListWorkflowExecutionsResponse, error) {
	return v.readManager(request.Namespace).ListClosedWorkflowExecutionsByType(request)
}

func (v *visibilityManagerDual) ListOpenWorkflowExecutionsByWorkflowID(request *manager.ListWorkflowExecutionsByWorkflowIDRequest) (*manager.ListWorkflowExecutionsResponse, error) {
	return v.readManager(request.Namespace).ListOpenWorkflowExecutionsByWorkflowID(request)
}

func (v *visibilityManagerDual) ListClosedWorkflowExecutionsByWorkflowID(request *manager.ListWorkflowExecutionsByWorkflowIDRequest) (*manager.ListWorkflowExecutionsResponse, error) {
	return v.readManager(request.Namespace).ListClosedWorkflowExecutionsByWorkflowID(request)
}

func (v *visibilityManagerDual) ListClosedWorkflowExecutionsByStatus(request *manager.ListClosedWorkflowExecutionsByStatusRequest) (*manager.ListWorkflowExecutionsResponse, error) {
	return v.readManager(request.Namespace).ListClosedWorkflowExecutionsByStatus(request)
}

func (v *visibilityManagerDual) ListWorkflowExecutions(request *manager.ListWorkflowExecutionsRequestV2) (*manager.ListWorkflowExecutionsResponse, error) {
	return v.readManager(request.Namespace).ListWorkflowExecutions(request)
}

func (v *visibilityManagerDual) ScanWorkflowExecutions(request *manager.ListWorkflowExecutionsRequestV2) (*manager.ListWorkflowExecutionsResponse, error) {
	return v.readManager(request.Namespace).ScanWorkflowExecutions(request)
}

func (v *visibilityManagerDual) CountWorkflowExecutions(request *manager.CountWorkflowExecutionsRequest) (*manager.CountWorkflowExecutionsResponse, error) {
	return v.readManager(request.Namespace).CountWorkflowExecutions(request)
}

func (v *visibilityManagerDual) writeManagers() ([]manager.VisibilityManager, error) {
	switch v.advancedVisibilityWritingMode() {
	case AdvancedVisibilityWritingModeOff:
		return []manager.VisibilityManager{v.stdVisibilityManager}, nil
	case AdvancedVisibilityWritingModeOn:
		return []manager.VisibilityManager{v.advVisibilityManager}, nil
	case AdvancedVisibilityWritingModeDual:
		return []manager.VisibilityManager{v.stdVisibilityManager, v.advVisibilityManager}, nil
	default:
		return nil, serviceerror.NewInternal(fmt.Sprintf("Unknown advanced visibility writing mode: %s", v.advancedVisibilityWritingMode()))
	}
}

func (v *visibilityManagerDual) readManager(namespace namespace.Name) manager.VisibilityManager {
	if v.enableAdvancedVisibilityRead(namespace.String()) {
		return v.advVisibilityManager
	}
	return v.stdVisibilityManager
}
