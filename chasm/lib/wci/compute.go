package wci

import (
	"context"
	"errors"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/auto-scaled-workers/wci/compute"
	"go.temporal.io/auto-scaled-workers/wci/workflow/iface"
	"go.temporal.io/server/chasm"
	wcipb "go.temporal.io/server/chasm/lib/wci/gen/wcipb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ComputeClient runs provider operations; history provides an in-process or delegated implementation.
type ComputeClient interface {
	// ValidateSpec checks provider configs only; algorithm configs are validated in history.
	ValidateSpec(ctx context.Context, rc compute.RequestContext, spec *iface.WorkerControllerInstanceSpec) error
	InvokeWorker(ctx context.Context, rc compute.RequestContext, cfg *iface.ComputeProviderSpec) error
	UpdateWorkerSetSize(ctx context.Context, rc compute.RequestContext, cfg *iface.ComputeProviderSpec, size int32) error
}

// ErrProviderNotSupportedOnCHASM means there is no CHASM compute path for this provider type yet.
var ErrProviderNotSupportedOnCHASM = serviceerror.NewFailedPrecondition("compute provider not supported on CHASM")

const (
	// Today's workflow uses MaximumAttempts: 2 for InvokeWorker.
	maxInvokeAttempts = 2
	computeTaskGroup  = "wci-compute"
)

var resizeBackoff = backoff.NewExponentialRetryPolicy(time.Second).WithMaximumInterval(time.Minute)

// retryable: transient provider failures back off and retry; anything the customer must fix fails.
func retryable(err error) bool {
	if errors.Is(err, ErrProviderNotSupportedOnCHASM) {
		return false
	}
	var pErr *compute.ProviderError
	if !errors.As(err, &pErr) {
		return true
	}
	switch pErr.Class {
	case compute.FailureRejected, compute.FailureNotFound, compute.FailureAccessDenied, compute.FailureInternal:
		return false
	default:
		return true
	}
}

func requestContext(ctx chasm.Context, w *WorkerControllerInstance) compute.RequestContext {
	return compute.RequestContext{
		NamespaceName:     ctx.NamespaceEntry().Name().String(),
		DeploymentName:    w.DeploymentName,
		DeploymentBuildID: w.BuildId,
	}
}

// recordValidation updates the status and signals the version workflow, as the workflow does.
func (w *WorkerControllerInstance) recordValidation(ctx chasm.MutableContext, err error) {
	status := &wcipb.ValidationStatus{LastValidationTime: timestamppb.New(ctx.Now(w)), Result: wcipb.VALIDATION_RESULT_SUCCESS}
	if err != nil {
		status.Result = wcipb.VALIDATION_RESULT_FAILED
		status.ErrMessage = err.Error()
	}
	w.ValidationStatus = status
	ctx.AddTask(w, chasm.TaskAttributes{}, &wcipb.SignalVersionWorkflowTask{LastValidationTime: common.CloneProto(status.LastValidationTime)})
}
