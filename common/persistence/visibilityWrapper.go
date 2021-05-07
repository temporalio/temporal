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

package persistence

import (
	"fmt"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
)

type (
	visibilityManagerWrapper struct {
		visibilityManager          VisibilityManager
		esVisibilityManager        VisibilityManager
		enableReadVisibilityFromES dynamicconfig.BoolPropertyFnWithNamespaceFilter
		advancedVisWritingMode     dynamicconfig.StringPropertyFn
	}
)

var _ VisibilityManager = (*visibilityManagerWrapper)(nil)

// NewVisibilityManagerWrapper create a visibility manager that operate on DB or ElasticSearch based on dynamic config.
func NewVisibilityManagerWrapper(visibilityManager, esVisibilityManager VisibilityManager,
	enableReadVisibilityFromES dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	advancedVisWritingMode dynamicconfig.StringPropertyFn) VisibilityManager {
	return &visibilityManagerWrapper{
		visibilityManager:          visibilityManager,
		esVisibilityManager:        esVisibilityManager,
		enableReadVisibilityFromES: enableReadVisibilityFromES,
		advancedVisWritingMode:     advancedVisWritingMode,
	}
}

func (v *visibilityManagerWrapper) Close() {
	if v.visibilityManager != nil {
		v.visibilityManager.Close()
	}
	if v.esVisibilityManager != nil {
		v.esVisibilityManager.Close()
	}
}

func (v *visibilityManagerWrapper) GetName() string {
	return "visibilityManagerWrapper"
}

func (v *visibilityManagerWrapper) RecordWorkflowExecutionStarted(request *RecordWorkflowExecutionStartedRequest) error {
	switch v.advancedVisWritingMode() {
	case common.AdvancedVisibilityWritingModeOff:
		return v.visibilityManager.RecordWorkflowExecutionStarted(request)
	case common.AdvancedVisibilityWritingModeOn:
		return v.esVisibilityManager.RecordWorkflowExecutionStarted(request)
	case common.AdvancedVisibilityWritingModeDual:
		if err := v.esVisibilityManager.RecordWorkflowExecutionStarted(request); err != nil {
			return err
		}
		return v.visibilityManager.RecordWorkflowExecutionStarted(request)
	default:
		return serviceerror.NewInternal(fmt.Sprintf("Unknown advanced visibility writing mode: %s", v.advancedVisWritingMode()))
	}
}

func (v *visibilityManagerWrapper) RecordWorkflowExecutionClosed(request *RecordWorkflowExecutionClosedRequest) error {
	switch v.advancedVisWritingMode() {
	case common.AdvancedVisibilityWritingModeOff:
		return v.visibilityManager.RecordWorkflowExecutionClosed(request)
	case common.AdvancedVisibilityWritingModeOn:
		return v.esVisibilityManager.RecordWorkflowExecutionClosed(request)
	case common.AdvancedVisibilityWritingModeDual:
		if err := v.esVisibilityManager.RecordWorkflowExecutionClosed(request); err != nil {
			return err
		}
		return v.visibilityManager.RecordWorkflowExecutionClosed(request)
	default:
		return serviceerror.NewInternal(fmt.Sprintf("Unknown advanced visibility writing mode: %s", v.advancedVisWritingMode()))
	}
}

func (v *visibilityManagerWrapper) UpsertWorkflowExecution(request *UpsertWorkflowExecutionRequest) error {
	switch v.advancedVisWritingMode() {
	case common.AdvancedVisibilityWritingModeOff:
		// no op on SQL/Cassandra persistence.
		return v.visibilityManager.UpsertWorkflowExecution(request)
	case common.AdvancedVisibilityWritingModeOn:
		return v.esVisibilityManager.UpsertWorkflowExecution(request)
	case common.AdvancedVisibilityWritingModeDual:
		if err := v.esVisibilityManager.UpsertWorkflowExecution(request); err != nil {
			return err
		}
		// no op on SQL/Cassandra persistence.
		return v.visibilityManager.UpsertWorkflowExecution(request)
	default:
		return serviceerror.NewInternal(fmt.Sprintf("Unknown advanced visibility writing mode: %s", v.advancedVisWritingMode()))
	}
}

func (v *visibilityManagerWrapper) ListOpenWorkflowExecutions(request *ListWorkflowExecutionsRequest) (*ListWorkflowExecutionsResponse, error) {
	manager := v.chooseVisibilityManagerForNamespace(request.Namespace)
	return manager.ListOpenWorkflowExecutions(request)
}

func (v *visibilityManagerWrapper) ListClosedWorkflowExecutions(request *ListWorkflowExecutionsRequest) (*ListWorkflowExecutionsResponse, error) {
	manager := v.chooseVisibilityManagerForNamespace(request.Namespace)
	return manager.ListClosedWorkflowExecutions(request)
}

func (v *visibilityManagerWrapper) ListOpenWorkflowExecutionsByType(request *ListWorkflowExecutionsByTypeRequest) (*ListWorkflowExecutionsResponse, error) {
	manager := v.chooseVisibilityManagerForNamespace(request.Namespace)
	return manager.ListOpenWorkflowExecutionsByType(request)
}

func (v *visibilityManagerWrapper) ListClosedWorkflowExecutionsByType(request *ListWorkflowExecutionsByTypeRequest) (*ListWorkflowExecutionsResponse, error) {
	manager := v.chooseVisibilityManagerForNamespace(request.Namespace)
	return manager.ListClosedWorkflowExecutionsByType(request)
}

func (v *visibilityManagerWrapper) ListOpenWorkflowExecutionsByWorkflowID(request *ListWorkflowExecutionsByWorkflowIDRequest) (*ListWorkflowExecutionsResponse, error) {
	manager := v.chooseVisibilityManagerForNamespace(request.Namespace)
	return manager.ListOpenWorkflowExecutionsByWorkflowID(request)
}

func (v *visibilityManagerWrapper) ListClosedWorkflowExecutionsByWorkflowID(request *ListWorkflowExecutionsByWorkflowIDRequest) (*ListWorkflowExecutionsResponse, error) {
	manager := v.chooseVisibilityManagerForNamespace(request.Namespace)
	return manager.ListClosedWorkflowExecutionsByWorkflowID(request)
}

func (v *visibilityManagerWrapper) ListClosedWorkflowExecutionsByStatus(request *ListClosedWorkflowExecutionsByStatusRequest) (*ListWorkflowExecutionsResponse, error) {
	manager := v.chooseVisibilityManagerForNamespace(request.Namespace)
	return manager.ListClosedWorkflowExecutionsByStatus(request)
}

func (v *visibilityManagerWrapper) GetClosedWorkflowExecution(request *GetClosedWorkflowExecutionRequest) (*GetClosedWorkflowExecutionResponse, error) {
	manager := v.chooseVisibilityManagerForNamespace(request.Namespace)
	return manager.GetClosedWorkflowExecution(request)
}

func (v *visibilityManagerWrapper) DeleteWorkflowExecution(request *VisibilityDeleteWorkflowExecutionRequest) error {
	switch v.advancedVisWritingMode() {
	case common.AdvancedVisibilityWritingModeOff:
		return v.visibilityManager.DeleteWorkflowExecution(request)
	case common.AdvancedVisibilityWritingModeOn:
		return v.esVisibilityManager.DeleteWorkflowExecution(request)
	case common.AdvancedVisibilityWritingModeDual:
		if err := v.esVisibilityManager.DeleteWorkflowExecution(request); err != nil {
			return err
		}
		return v.visibilityManager.DeleteWorkflowExecution(request)
	default:
		return serviceerror.NewInternal(fmt.Sprintf("Unknown advanced visibility writing mode: %s", v.advancedVisWritingMode()))
	}
}

func (v *visibilityManagerWrapper) ListWorkflowExecutions(request *ListWorkflowExecutionsRequestV2) (*ListWorkflowExecutionsResponse, error) {
	manager := v.chooseVisibilityManagerForNamespace(request.Namespace)
	return manager.ListWorkflowExecutions(request)
}

func (v *visibilityManagerWrapper) ScanWorkflowExecutions(request *ListWorkflowExecutionsRequestV2) (*ListWorkflowExecutionsResponse, error) {
	manager := v.chooseVisibilityManagerForNamespace(request.Namespace)
	return manager.ScanWorkflowExecutions(request)
}

func (v *visibilityManagerWrapper) CountWorkflowExecutions(request *CountWorkflowExecutionsRequest) (*CountWorkflowExecutionsResponse, error) {
	manager := v.chooseVisibilityManagerForNamespace(request.Namespace)
	return manager.CountWorkflowExecutions(request)
}

func (v *visibilityManagerWrapper) chooseVisibilityManagerForNamespace(namespace string) VisibilityManager {
	var visibilityMgr VisibilityManager
	if v.enableReadVisibilityFromES(namespace) && v.esVisibilityManager != nil {
		visibilityMgr = v.esVisibilityManager
	} else {
		visibilityMgr = v.visibilityManager
	}
	return visibilityMgr
}
