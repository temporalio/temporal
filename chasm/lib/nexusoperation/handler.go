package nexusoperation

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/log"
)

type handler struct {
	nexusoperationpb.UnimplementedNexusOperationServiceServer

	config *Config
	logger log.Logger
}

func newHandler(config *Config, logger log.Logger) *handler {
	return &handler{
		config: config,
		logger: logger,
	}
}

// StartNexusOperation creates a new standalone Nexus operation execution via CHASM.
func (h *handler) StartNexusOperation(
	ctx context.Context,
	req *nexusoperationpb.StartNexusOperationRequest,
) (response *nexusoperationpb.StartNexusOperationResponse, err error) {
	defer log.CapturePanic(h.logger, &err)

	frontendReq := req.GetFrontendRequest()

	result, err := chasm.StartExecution[*Operation](
		ctx,
		chasm.ExecutionKey{
			NamespaceID: req.GetNamespaceId(),
			BusinessID:  frontendReq.GetOperationId(),
		},
		newStandaloneOperation,
		req,
		chasm.WithRequestID(frontendReq.GetRequestId()),
		chasm.WithBusinessIDPolicy(
			idReusePolicyFromProto(frontendReq.GetIdReusePolicy()),
			idConflictPolicyFromProto(frontendReq.GetIdConflictPolicy()),
		),
	)
	if err != nil {
		return nil, err
	}

	return &nexusoperationpb.StartNexusOperationResponse{
		FrontendResponse: &workflowservice.StartNexusOperationExecutionResponse{
			RunId:   result.ExecutionKey.RunID,
			Started: result.Created,
		},
	}, nil
}

// TODO: Add long-poll support.
func (h *handler) DescribeNexusOperation(
	ctx context.Context,
	req *nexusoperationpb.DescribeNexusOperationRequest,
) (response *nexusoperationpb.DescribeNexusOperationResponse, err error) {
	defer log.CapturePanic(h.logger, &err)

	ref := chasm.NewComponentRef[*Operation](chasm.ExecutionKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  req.GetFrontendRequest().GetOperationId(),
		RunID:       req.GetFrontendRequest().GetRunId(),
	})

	return chasm.ReadComponent(ctx, ref, (*Operation).buildDescribeResponse, req, nil)
}

// RequestCancelNexusOperation requests cancellation of a standalone Nexus operation via CHASM.
func (h *handler) RequestCancelNexusOperation(
	ctx context.Context,
	req *nexusoperationpb.RequestCancelNexusOperationRequest,
) (response *nexusoperationpb.RequestCancelNexusOperationResponse, err error) {
	defer log.CapturePanic(h.logger, &err)

	ref := chasm.NewComponentRef[*Operation](chasm.ExecutionKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  req.GetFrontendRequest().GetOperationId(),
		RunID:       req.GetFrontendRequest().GetRunId(),
	})

	resp, _, err := chasm.UpdateComponent(
		ctx,
		ref,
		func(o *Operation, ctx chasm.MutableContext, req *nexusoperationpb.RequestCancelNexusOperationRequest) (*nexusoperationpb.RequestCancelNexusOperationResponse, error) {
			if err := o.RequestCancel(ctx, &nexusoperationpb.CancellationState{
				RequestId: req.GetFrontendRequest().GetRequestId(),
				Identity:  req.GetFrontendRequest().GetIdentity(),
				Reason:    req.GetFrontendRequest().GetReason(),
			}); err != nil {
				return nil, err
			}
			return &nexusoperationpb.RequestCancelNexusOperationResponse{}, nil
		}, req)

	return resp, err
}

// TerminateNexusOperation terminates a standalone Nexus operation via CHASM.
func (h *handler) TerminateNexusOperation(
	ctx context.Context,
	req *nexusoperationpb.TerminateNexusOperationRequest,
) (response *nexusoperationpb.TerminateNexusOperationResponse, err error) {
	defer log.CapturePanic(h.logger, &err)

	ref := chasm.NewComponentRef[*Operation](chasm.ExecutionKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  req.GetFrontendRequest().GetOperationId(),
		RunID:       req.GetFrontendRequest().GetRunId(),
	})

	resp, _, err := chasm.UpdateComponent(
		ctx,
		ref,
		func(o *Operation, ctx chasm.MutableContext, req *nexusoperationpb.TerminateNexusOperationRequest) (*nexusoperationpb.TerminateNexusOperationResponse, error) {
			if _, err := o.Terminate(ctx, chasm.TerminateComponentRequest{
				RequestID: req.GetFrontendRequest().GetRequestId(),
				Identity:  req.GetFrontendRequest().GetIdentity(),
				Reason:    req.GetFrontendRequest().GetReason(),
			}); err != nil {
				return nil, err
			}
			return &nexusoperationpb.TerminateNexusOperationResponse{}, nil
		}, req)

	return resp, err
}

// DeleteNexusOperation terminates the nexus operation if running, then schedules it for deletion.
func (h *handler) DeleteNexusOperation(
	ctx context.Context,
	req *nexusoperationpb.DeleteNexusOperationRequest,
) (response *nexusoperationpb.DeleteNexusOperationResponse, err error) {
	defer log.CapturePanic(h.logger, &err)

	frontendReq := req.GetFrontendRequest()

	key := chasm.ExecutionKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  frontendReq.GetOperationId(),
		RunID:       frontendReq.GetRunId(),
	}

	if err := chasm.DeleteExecution[*Operation](ctx, key, chasm.DeleteExecutionRequest{
		TerminateComponentRequest: chasm.TerminateComponentRequest{
			Reason: "Delete nexus operation execution",
		},
	}); err != nil {
		return nil, err
	}

	return &nexusoperationpb.DeleteNexusOperationResponse{}, nil
}
func idReusePolicyFromProto(p enumspb.NexusOperationIdReusePolicy) chasm.BusinessIDReusePolicy {
	switch p {
	case enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY:
		return chasm.BusinessIDReusePolicyAllowDuplicateFailedOnly
	case enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_REJECT_DUPLICATE:
		return chasm.BusinessIDReusePolicyRejectDuplicate
	default:
		return chasm.BusinessIDReusePolicyAllowDuplicate
	}
}

func idConflictPolicyFromProto(p enumspb.NexusOperationIdConflictPolicy) chasm.BusinessIDConflictPolicy {
	switch p {
	case enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_USE_EXISTING:
		return chasm.BusinessIDConflictPolicyUseExisting
	default:
		return chasm.BusinessIDConflictPolicyFail
	}
}
