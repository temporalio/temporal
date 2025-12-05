package visibility

import (
	"context"
	"errors"
	"sync"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/chasm"
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

		// Separate context for the shadow read requests. It ensures the main request context won't
		// cancel the shadow request if the former completes before the shadow request.
		// Each shadow request gets a new child context from shadowReadCtx, and we cancel all of them
		// when visibilityManager closes by calling shadowReadCtxCancel.
		shadowReadCtx       context.Context
		shadowReadCtxCancel context.CancelFunc
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
	shadowReadCtx, shadowReadCtxCancel := context.WithCancel(context.Background())
	return &VisibilityManagerDual{
		visibilityManager:          visibilityManager,
		secondaryVisibilityManager: secondaryVisibilityManager,
		managerSelector:            managerSelector,
		enableShadowReadMode:       enableShadowReadMode,
		shadowReadCtx:              shadowReadCtx,
		shadowReadCtxCancel:        shadowReadCtxCancel,
	}
}

func (v *VisibilityManagerDual) GetPrimaryVisibility() manager.VisibilityManager {
	return v.visibilityManager
}

func (v *VisibilityManagerDual) GetSecondaryVisibility() manager.VisibilityManager {
	return v.secondaryVisibilityManager
}

func (v *VisibilityManagerDual) Close() {
	v.shadowReadCtxCancel()
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
	return dualWriteWrapper(
		ctx,
		v,
		request,
		manager.VisibilityManager.RecordWorkflowExecutionStarted,
	)
}

func (v *VisibilityManagerDual) RecordWorkflowExecutionClosed(
	ctx context.Context,
	request *manager.RecordWorkflowExecutionClosedRequest,
) error {
	return dualWriteWrapper(
		ctx,
		v,
		request,
		manager.VisibilityManager.RecordWorkflowExecutionClosed,
	)
}

func (v *VisibilityManagerDual) UpsertWorkflowExecution(
	ctx context.Context,
	request *manager.UpsertWorkflowExecutionRequest,
) error {
	return dualWriteWrapper(
		ctx,
		v,
		request,
		manager.VisibilityManager.UpsertWorkflowExecution,
	)
}

func (v *VisibilityManagerDual) DeleteWorkflowExecution(
	ctx context.Context,
	request *manager.VisibilityDeleteWorkflowExecutionRequest,
) error {
	return dualWriteWrapper(
		ctx,
		v,
		request,
		manager.VisibilityManager.DeleteWorkflowExecution,
	)
}

func (v *VisibilityManagerDual) ListWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequestV2,
) (*manager.ListWorkflowExecutionsResponse, error) {
	return dualReadWrapper(
		ctx,
		v,
		request,
		request.Namespace,
		manager.VisibilityManager.ListWorkflowExecutions,
	)
}

func (v *VisibilityManagerDual) ListChasmExecutions(
	ctx context.Context,
	request *manager.ListChasmExecutionsRequest,
) (*chasm.ListExecutionsResponse[*commonpb.Payload], error) {
	return dualReadWrapper(
		ctx,
		v,
		request,
		request.Namespace,
		manager.VisibilityManager.ListChasmExecutions,
	)
}

func (v *VisibilityManagerDual) CountWorkflowExecutions(
	ctx context.Context,
	request *manager.CountWorkflowExecutionsRequest,
) (*manager.CountWorkflowExecutionsResponse, error) {
	return dualReadWrapper(
		ctx,
		v,
		request,
		request.Namespace,
		manager.VisibilityManager.CountWorkflowExecutions,
	)
}

func (v *VisibilityManagerDual) CountChasmExecutions(
	ctx context.Context,
	request *manager.CountChasmExecutionsRequest,
) (*chasm.CountExecutionsResponse, error) {
	return dualReadWrapper(
		ctx,
		v,
		request,
		request.Namespace,
		manager.VisibilityManager.CountChasmExecutions,
	)
}

func (v *VisibilityManagerDual) GetWorkflowExecution(
	ctx context.Context,
	request *manager.GetWorkflowExecutionRequest,
) (*manager.GetWorkflowExecutionResponse, error) {
	return dualReadWrapper(
		ctx,
		v,
		request,
		request.Namespace,
		manager.VisibilityManager.GetWorkflowExecution,
	)
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

func dualWriteWrapper[RequestT any](
	ctx context.Context,
	v *VisibilityManagerDual,
	request *RequestT,
	fn func(manager.VisibilityManager, context.Context, *RequestT) error,
) error {
	ms, err := v.managerSelector.writeManagers()
	if err != nil {
		return err
	}
	errs := make([]error, len(ms))
	wg := sync.WaitGroup{}
	for i, m := range ms {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = fn(m, ctx, request)
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

func dualReadWrapper[RequestT any, ResponseT any](
	ctx context.Context,
	v *VisibilityManagerDual,
	request *RequestT,
	nsName namespace.Name,
	fn func(manager.VisibilityManager, context.Context, *RequestT) (*ResponseT, error),
) (*ResponseT, error) {
	if v.enableShadowReadMode() {
		ms, err := v.managerSelector.readManagers(nsName)
		if err != nil {
			return nil, err
		}

		var shadowCtx context.Context
		var shadowCtxCancel context.CancelFunc
		if deadline, ok := ctx.Deadline(); ok {
			shadowCtx, shadowCtxCancel = context.WithDeadline(v.shadowReadCtx, deadline)
		} else {
			shadowCtx, shadowCtxCancel = context.WithCancel(v.shadowReadCtx)
		}
		go func() {
			defer shadowCtxCancel()
			// ignore error since it's shadow request
			_, _ = fn(ms[1], shadowCtx, request)
		}()

		return fn(ms[0], ctx, request)
	}
	return fn(v.managerSelector.readManager(nsName), ctx, request)
}
