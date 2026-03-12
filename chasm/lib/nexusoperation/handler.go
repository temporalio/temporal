package nexusoperation

import (
	"context"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/log"
	"google.golang.org/protobuf/types/known/timestamppb"
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
		func(ctx chasm.MutableContext, req *nexusoperationpb.StartNexusOperationRequest) (*Operation, error) {
			frontendReq := req.GetFrontendRequest()
			op := NewOperation(&nexusoperationpb.OperationState{
				Endpoint:               frontendReq.GetEndpoint(),
				Service:                frontendReq.GetService(),
				Operation:              frontendReq.GetOperation(),
				ScheduleToCloseTimeout: frontendReq.GetScheduleToCloseTimeout(),
				ScheduledTime:          timestamppb.New(ctx.Now(nil)),
				RequestId:              uuid.NewString(), // different from client-supplied RequestID!
			})
			op.RequestData = chasm.NewDataField(ctx, &nexusoperationpb.OperationRequestData{
				Input:        frontendReq.GetInput(),
				NexusHeader:  frontendReq.GetNexusHeader(),
				UserMetadata: frontendReq.GetUserMetadata(),
				Identity:     frontendReq.GetIdentity(),
			})
			op.Visibility = chasm.NewComponentField(ctx, chasm.NewVisibilityWithData(
				ctx,
				frontendReq.GetSearchAttributes().GetIndexedFields(),
				nil,
			))
			if err := transitionScheduled.Apply(op, ctx, EventScheduled{}); err != nil {
				return nil, err
			}
			return op, nil
		},
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
