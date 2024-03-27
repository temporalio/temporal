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

	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
)

type (
	VisibilityManagerDual struct {
		visibilityManager          manager.VisibilityManager
		secondaryVisibilityManager manager.VisibilityManager
		managerSelector            managerSelector
	}
)

var _ manager.VisibilityManager = (*VisibilityManagerDual)(nil)

// NewVisibilityManagerDual create a visibility manager that operate on multiple manager
// implementations based on dynamic config.
func NewVisibilityManagerDual(
	visibilityManager manager.VisibilityManager,
	secondaryVisibilityManager manager.VisibilityManager,
	managerSelector managerSelector,
) *VisibilityManagerDual {
	return &VisibilityManagerDual{
		visibilityManager:          visibilityManager,
		secondaryVisibilityManager: secondaryVisibilityManager,
		managerSelector:            managerSelector,
	}
}

func (v *VisibilityManagerDual) GetPrimaryVisibility() manager.VisibilityManager {
	return v.visibilityManager
}

func (v *VisibilityManagerDual) GetSecondaryVisibility() manager.VisibilityManager {
	return v.secondaryVisibilityManager
}

func (v *VisibilityManagerDual) Close() {
	v.visibilityManager.Close()
	v.secondaryVisibilityManager.Close()
}

func (v *VisibilityManagerDual) GetReadStoreName(nsName namespace.Name) string {
	return v.managerSelector.readManager(nsName).GetReadStoreName(nsName)
}

func (v *VisibilityManagerDual) GetStoreNames() []string {
	return append(v.visibilityManager.GetStoreNames(), v.secondaryVisibilityManager.GetStoreNames()...)
}

func (v *VisibilityManagerDual) HasStoreName(stName string) bool {
	for _, sn := range v.GetStoreNames() {
		if sn == stName {
			return true
		}
	}
	return false
}

func (v *VisibilityManagerDual) GetIndexName() string {
	return v.visibilityManager.GetIndexName()
}

func (v *VisibilityManagerDual) ValidateCustomSearchAttributes(
	searchAttributes map[string]any,
) (map[string]any, error) {
	ms, err := v.managerSelector.writeManagers()
	if err != nil {
		return nil, err
	}
	for _, m := range ms {
		searchAttributes, err = m.ValidateCustomSearchAttributes(searchAttributes)
		if err != nil {
			return nil, err
		}
	}
	return searchAttributes, nil
}

func (v *VisibilityManagerDual) RecordWorkflowExecutionStarted(
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

func (v *VisibilityManagerDual) RecordWorkflowExecutionClosed(
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

func (v *VisibilityManagerDual) UpsertWorkflowExecution(
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

func (v *VisibilityManagerDual) DeleteWorkflowExecution(
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

func (v *VisibilityManagerDual) ListOpenWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequest,
) (*manager.ListWorkflowExecutionsResponse, error) {
	return v.managerSelector.readManager(request.Namespace).ListOpenWorkflowExecutions(ctx, request)
}

func (v *VisibilityManagerDual) ListClosedWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequest,
) (*manager.ListWorkflowExecutionsResponse, error) {
	return v.managerSelector.readManager(request.Namespace).ListClosedWorkflowExecutions(ctx, request)
}

func (v *VisibilityManagerDual) ListOpenWorkflowExecutionsByType(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByTypeRequest,
) (*manager.ListWorkflowExecutionsResponse, error) {
	return v.managerSelector.readManager(request.Namespace).ListOpenWorkflowExecutionsByType(ctx, request)
}

func (v *VisibilityManagerDual) ListClosedWorkflowExecutionsByType(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByTypeRequest,
) (*manager.ListWorkflowExecutionsResponse, error) {
	return v.managerSelector.readManager(request.Namespace).ListClosedWorkflowExecutionsByType(ctx, request)
}

func (v *VisibilityManagerDual) ListOpenWorkflowExecutionsByWorkflowID(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByWorkflowIDRequest,
) (*manager.ListWorkflowExecutionsResponse, error) {
	return v.managerSelector.readManager(request.Namespace).ListOpenWorkflowExecutionsByWorkflowID(ctx, request)
}

func (v *VisibilityManagerDual) ListClosedWorkflowExecutionsByWorkflowID(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByWorkflowIDRequest,
) (*manager.ListWorkflowExecutionsResponse, error) {
	return v.managerSelector.readManager(request.Namespace).ListClosedWorkflowExecutionsByWorkflowID(ctx, request)
}

func (v *VisibilityManagerDual) ListOpenWorkflowExecutionsByVersion(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByVersionSARequest,
) (*manager.ListWorkflowExecutionsResponse, error) {
	return v.managerSelector.readManager(request.Namespace).ListOpenWorkflowExecutionsByVersion(ctx, request)
}

func (v *VisibilityManagerDual) ListClosedWorkflowExecutionsByVersion(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByVersionSARequest,
) (*manager.ListWorkflowExecutionsResponse, error) {
	return v.managerSelector.readManager(request.Namespace).ListClosedWorkflowExecutionsByVersion(ctx, request)
}

func (v *VisibilityManagerDual) ListClosedWorkflowExecutionsByStatus(
	ctx context.Context,
	request *manager.ListClosedWorkflowExecutionsByStatusRequest,
) (*manager.ListWorkflowExecutionsResponse, error) {
	return v.managerSelector.readManager(request.Namespace).ListClosedWorkflowExecutionsByStatus(ctx, request)
}

func (v *VisibilityManagerDual) ListWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequestV2,
) (*manager.ListWorkflowExecutionsResponse, error) {
	return v.managerSelector.readManager(request.Namespace).ListWorkflowExecutions(ctx, request)
}

func (v *VisibilityManagerDual) ScanWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequestV2,
) (*manager.ListWorkflowExecutionsResponse, error) {
	return v.managerSelector.readManager(request.Namespace).ScanWorkflowExecutions(ctx, request)
}

func (v *VisibilityManagerDual) CountWorkflowExecutions(
	ctx context.Context,
	request *manager.CountWorkflowExecutionsRequest,
) (*manager.CountWorkflowExecutionsResponse, error) {
	return v.managerSelector.readManager(request.Namespace).CountWorkflowExecutions(ctx, request)
}

func (v *VisibilityManagerDual) GetWorkflowExecution(
	ctx context.Context,
	request *manager.GetWorkflowExecutionRequest,
) (*manager.GetWorkflowExecutionResponse, error) {
	return v.managerSelector.readManager(request.Namespace).GetWorkflowExecution(ctx, request)
}
