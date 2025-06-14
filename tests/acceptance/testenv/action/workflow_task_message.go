package action

import (
	failurepb "go.temporal.io/api/failure/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/stamp"
	"go.temporal.io/server/tests/acceptance/model"
)

type WorkflowUpdateAcceptMessage struct {
	Update    *model.WorkflowUpdate `validate:"required"`
	MessageID stamp.Gen[stamp.ID]
}

func (m WorkflowUpdateAcceptMessage) Next(ctx stamp.GenContext) *protocolpb.Message {
	req := unmarshalAny[*updatepb.Request](m.Update.Request.GetBody())
	return &protocolpb.Message{
		Id:                 req.Meta.GetUpdateId() + "/accept",
		ProtocolInstanceId: req.Meta.GetUpdateId(),
		SequencingId:       nil,
		Body: marshalAny(&updatepb.Acceptance{
			AcceptedRequestMessageId:         m.Update.Request.GetId(),
			AcceptedRequestSequencingEventId: m.Update.Request.GetEventId(),
			AcceptedRequest:                  req,
		}),
	}
}

type WorkflowUpdateRejectionMessage struct {
	Update    *model.WorkflowUpdate `validate:"required"`
	Failure   WorkflowUpdateFailure `validate:"required"`
	MessageID stamp.Gen[stamp.ID]
}

func (m WorkflowUpdateRejectionMessage) Next(ctx stamp.GenContext) *protocolpb.Message {
	req := unmarshalAny[*updatepb.Request](m.Update.Request.GetBody())
	return &protocolpb.Message{
		Id:                 req.Meta.GetUpdateId() + "/reject",
		ProtocolInstanceId: req.Meta.GetUpdateId(),
		SequencingId:       nil,
		Body: marshalAny(&updatepb.Rejection{
			RejectedRequestMessageId:         m.Update.Request.GetId(),
			RejectedRequestSequencingEventId: m.Update.Request.GetEventId(),
			RejectedRequest:                  req,
			Failure:                          m.Failure.Next(ctx),
		}),
	}
}

type WorkflowUpdateCompletionMessage struct {
	Update    *model.WorkflowUpdate `validate:"required"`
	Outcome   WorkflowUpdateOutcome `validate:"required"`
	MessageID stamp.Gen[stamp.ID]
}

func (m WorkflowUpdateCompletionMessage) Next(ctx stamp.GenContext) *protocolpb.Message {
	req := unmarshalAny[*updatepb.Request](m.Update.Request.GetBody())
	return &protocolpb.Message{
		Id:                 req.Meta.GetUpdateId() + "/complete",
		ProtocolInstanceId: req.Meta.GetUpdateId(),
		SequencingId:       nil,
		Body: marshalAny(&updatepb.Response{
			Meta:    req.Meta,
			Outcome: m.Outcome.Next(ctx),
		}),
	}
}

type WorkflowUpdateOutcome struct {
	// TODO
}

func (o WorkflowUpdateOutcome) Next(ctx stamp.GenContext) *updatepb.Outcome {
	return &updatepb.Outcome{
		Value: &updatepb.Outcome_Success{
			Success: payloads.EncodeString("success"),
		},
	}
}

type WorkflowUpdateFailure struct {
	// TODO
}

func (f WorkflowUpdateFailure) Next(ctx stamp.GenContext) *failurepb.Failure {
	return &failurepb.Failure{
		Message: "rejection",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{},
		},
	}
}
