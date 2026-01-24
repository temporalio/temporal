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
	"google.golang.org/protobuf/proto"
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

	return []*commandpb.Command{commandpb.Command_builder{
		CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
		ProtocolMessageCommandAttributes: commandpb.ProtocolMessageCommandAttributes_builder{
			MessageId: tv.MessageID() + "_update-accepted",
		}.Build(),
	}.Build()}
}

func (u UpdateUtils) UpdateCompleteCommands(tv *testvars.TestVars) []*commandpb.Command {
	if th, ok := u.t.(helper); ok {
		th.Helper()
	}
	return []*commandpb.Command{
		commandpb.Command_builder{
			CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
			ProtocolMessageCommandAttributes: commandpb.ProtocolMessageCommandAttributes_builder{
				MessageId: tv.MessageID() + "_update-completed",
			}.Build(),
		}.Build(),
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
		protocolpb.Message_builder{
			Id:                 tv.MessageID() + "_update-accepted",
			ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
			Body: protoutils.MarshalAny(u.t, updatepb.Acceptance_builder{
				AcceptedRequestMessageId:         updRequestMsg.GetId(),
				AcceptedRequestSequencingEventId: updRequestMsg.GetEventId(),
				AcceptedRequest:                  updRequest,
			}.Build()),
		}.Build(),
	}
}

func (u UpdateUtils) UpdateCompleteMessages(tv *testvars.TestVars, updRequestMsg *protocolpb.Message) []*protocolpb.Message {
	if th, ok := u.t.(helper); ok {
		th.Helper()
	}
	updRequest := protoutils.UnmarshalAny[*updatepb.Request](u.t, updRequestMsg.GetBody())

	return []*protocolpb.Message{
		protocolpb.Message_builder{
			Id:                 tv.MessageID() + "_update-completed",
			ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
			Body: protoutils.MarshalAny(u.t, updatepb.Response_builder{
				Meta: updRequest.GetMeta(),
				Outcome: updatepb.Outcome_builder{
					Success: proto.ValueOrDefault(payloads.EncodeString("success-result-of-" + updRequest.GetMeta().GetUpdateId())),
				}.Build(),
			}.Build()),
		}.Build(),
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
		protocolpb.Message_builder{
			Id:                 tv.MessageID() + "_update-rejected",
			ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
			Body: protoutils.MarshalAny(u.t, updatepb.Rejection_builder{
				RejectedRequestMessageId:         updRequestMsg.GetId(),
				RejectedRequestSequencingEventId: updRequestMsg.GetEventId(),
				RejectedRequest:                  updRequest,
				Failure: failurepb.Failure_builder{
					Message:                "rejection-of-" + updRequest.GetMeta().GetUpdateId(),
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{},
				}.Build(),
			}.Build()),
		}.Build(),
	}
}
