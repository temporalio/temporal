package updateutils

import (
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/protoutils"
	"go.temporal.io/server/common/testing/testvars"
)

type (
	UpdateUtils struct {
		t require.TestingT
	}

	helper interface {
		Helper()
	}
)

func New(t require.TestingT) UpdateUtils {
	return UpdateUtils{
		t: t,
	}
}

func (u UpdateUtils) UpdateAcceptCommands(tv *testvars.TestVars) []*commandpb.Command {
	if th, ok := u.t.(helper); ok {
		th.Helper()
	}

	return []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
		Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
			MessageId: tv.MessageID() + "_update-accepted",
		}},
	}}
}

func (u UpdateUtils) UpdateCompleteCommands(tv *testvars.TestVars) []*commandpb.Command {
	if th, ok := u.t.(helper); ok {
		th.Helper()
	}
	return []*commandpb.Command{
		{
			CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
			Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
				MessageId: tv.MessageID() + "_update-completed",
			}},
		},
	}
}

func (u UpdateUtils) UpdateAcceptCompleteCommands(tv *testvars.TestVars) []*commandpb.Command {
	if th, ok := u.t.(helper); ok {
		th.Helper()
	}
	return append(u.UpdateAcceptCommands(tv), u.UpdateCompleteCommands(tv)...)
}

func (u UpdateUtils) UpdateAcceptMessages(tv *testvars.TestVars, updRequestMsg *protocolpb.Message) []*protocolpb.Message {
	if th, ok := u.t.(helper); ok {
		th.Helper()
	}
	updRequest := protoutils.UnmarshalAny[*updatepb.Request](u.t, updRequestMsg.GetBody())

	return []*protocolpb.Message{
		{
			Id:                 tv.MessageID() + "_update-accepted",
			ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
			SequencingId:       nil,
			Body: protoutils.MarshalAny(u.t, &updatepb.Acceptance{
				AcceptedRequestMessageId:         updRequestMsg.GetId(),
				AcceptedRequestSequencingEventId: updRequestMsg.GetEventId(),
				AcceptedRequest:                  updRequest,
			}),
		},
	}
}

func (u UpdateUtils) UpdateCompleteMessages(tv *testvars.TestVars, updRequestMsg *protocolpb.Message) []*protocolpb.Message {
	if th, ok := u.t.(helper); ok {
		th.Helper()
	}
	updRequest := protoutils.UnmarshalAny[*updatepb.Request](u.t, updRequestMsg.GetBody())

	return []*protocolpb.Message{
		{
			Id:                 tv.MessageID() + "_update-completed",
			ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
			SequencingId:       nil,
			Body: protoutils.MarshalAny(u.t, &updatepb.Response{
				Meta: updRequest.GetMeta(),
				Outcome: &updatepb.Outcome{
					Value: &updatepb.Outcome_Success{
						Success: payloads.EncodeString("success-result-of-" + updRequest.GetMeta().GetUpdateId()),
					},
				},
			}),
		},
	}
}

func (u UpdateUtils) UpdateAcceptCompleteMessages(tv *testvars.TestVars, updRequestMsg *protocolpb.Message) []*protocolpb.Message {
	if th, ok := u.t.(helper); ok {
		th.Helper()
	}
	return append(
		u.UpdateAcceptMessages(tv, updRequestMsg),
		u.UpdateCompleteMessages(tv, updRequestMsg)...)
}

func (u UpdateUtils) UpdateRejectMessages(tv *testvars.TestVars, updRequestMsg *protocolpb.Message) []*protocolpb.Message {
	if th, ok := u.t.(helper); ok {
		th.Helper()
	}
	updRequest := protoutils.UnmarshalAny[*updatepb.Request](u.t, updRequestMsg.GetBody())

	return []*protocolpb.Message{
		{
			Id:                 tv.MessageID() + "_update-rejected",
			ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
			SequencingId:       nil,
			Body: protoutils.MarshalAny(u.t, &updatepb.Rejection{
				RejectedRequestMessageId:         updRequestMsg.GetId(),
				RejectedRequestSequencingEventId: updRequestMsg.GetEventId(),
				RejectedRequest:                  updRequest,
				Failure: &failurepb.Failure{
					Message:     "rejection-of-" + updRequest.GetMeta().GetUpdateId(),
					FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{}},
				},
			}),
		},
	}
}
