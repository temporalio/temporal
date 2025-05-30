package visibility

import (
	"context"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
)

type (
	VisibilityManagerDual struct {
		visibilityManager          manager.VisibilityManager
		secondaryVisibilityManager manager.VisibilityManager
		managerSelector            managerSelector
		enableShadowReadMode       dynamicconfig.BoolPropertyFn
	}
)

var _ manager.VisibilityManager = (*VisibilityManagerDual)(nil)

// NewVisibilityManagerDual create a visibility manager that operate on multiple manager
// implementations based on dynamic config.
func NewVisibilityManagerDual(
	visibilityManager manager.VisibilityManager,
	secondaryVisibilityManager manager.VisibilityManager,
	managerSelector managerSelector,
	enableShadowReadMode dynamicconfig.BoolPropertyFn,
) *VisibilityManagerDual {
	return &VisibilityManagerDual{
		visibilityManager:          visibilityManager,
		secondaryVisibilityManager: secondaryVisibilityManager,
		managerSelector:            managerSelector,
		enableShadowReadMode:       enableShadowReadMode,
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

func (v *VisibilityManagerDual) ListWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequestV2,
) (*manager.ListWorkflowExecutionsResponse, error) {
	if v.enableShadowReadMode() {
		ms, err := v.managerSelector.readManagers(request.Namespace)
		if err != nil {
			return nil, err
		}
		//nolint:errcheck // ignore error since it's shadow request
		go ms[1].ListWorkflowExecutions(ctx, request)
		res, err := ms[0].ListWorkflowExecutions(ctx, request)
		if err != nil {
			return nil, err
		}
		return res, err
	}
	return v.managerSelector.readManager(request.Namespace).ListWorkflowExecutions(ctx, request)
}

func (v *VisibilityManagerDual) ScanWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequestV2,
) (*manager.ListWorkflowExecutionsResponse, error) {
	if v.enableShadowReadMode() {
		ms, err := v.managerSelector.readManagers(request.Namespace)
		if err != nil {
			return nil, err
		}
		//nolint:errcheck // ignore error since it's shadow request
		go ms[1].ScanWorkflowExecutions(ctx, request)
		res, err := ms[0].ScanWorkflowExecutions(ctx, request)
		if err != nil {
			return nil, err
		}
		return res, err
	}
	return v.managerSelector.readManager(request.Namespace).ScanWorkflowExecutions(ctx, request)
}

func (v *VisibilityManagerDual) CountWorkflowExecutions(
	ctx context.Context,
	request *manager.CountWorkflowExecutionsRequest,
) (*manager.CountWorkflowExecutionsResponse, error) {
	if v.enableShadowReadMode() {
		ms, err := v.managerSelector.readManagers(request.Namespace)
		if err != nil {
			return nil, err
		}
		//nolint:errcheck // ignore error since it's shadow request
		go ms[1].CountWorkflowExecutions(ctx, request)
		res, err := ms[0].CountWorkflowExecutions(ctx, request)
		if err != nil {
			return nil, err
		}
		return res, err
	}
	return v.managerSelector.readManager(request.Namespace).CountWorkflowExecutions(ctx, request)
}

func (v *VisibilityManagerDual) GetWorkflowExecution(
	ctx context.Context,
	request *manager.GetWorkflowExecutionRequest,
) (*manager.GetWorkflowExecutionResponse, error) {
	if v.enableShadowReadMode() {
		ms, err := v.managerSelector.readManagers(request.Namespace)
		if err != nil {
			return nil, err
		}
		//nolint:errcheck // ignore error since it's shadow request
		go ms[1].GetWorkflowExecution(ctx, request)
		res, err := ms[0].GetWorkflowExecution(ctx, request)
		if err != nil {
			return nil, err
		}
		return res, err
	}
	return v.managerSelector.readManager(request.Namespace).GetWorkflowExecution(ctx, request)
}

func (v *VisibilityManagerDual) AddSearchAttributes(
	ctx context.Context,
	request *manager.AddSearchAttributesRequest,
) error {
	if err := v.visibilityManager.AddSearchAttributes(ctx, request); err != nil {
		return err
	}
	return v.secondaryVisibilityManager.AddSearchAttributes(ctx, request)
}
