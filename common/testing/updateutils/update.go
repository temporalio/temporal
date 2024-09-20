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

func (u UpdateUtils) UpdateAcceptCommands(tv *testvars.TestVars, messageID string) []*commandpb.Command {
	if th, ok := u.t.(helper); ok {
		th.Helper()
	}

	return []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
		Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
			MessageId: tv.MessageID("update-accepted", messageID),
		}},
	}}
}

func (u UpdateUtils) UpdateCompleteCommands(tv *testvars.TestVars, messageID string) []*commandpb.Command {
	if th, ok := u.t.(helper); ok {
		th.Helper()
	}
	return []*commandpb.Command{
		{
			CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
			Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
				MessageId: tv.MessageID("update-completed", messageID),
			}},
		},
	}
}

func (u UpdateUtils) UpdateAcceptCompleteCommands(tv *testvars.TestVars, messageID string) []*commandpb.Command {
	if th, ok := u.t.(helper); ok {
		th.Helper()
	}
	return append(u.UpdateAcceptCommands(tv, messageID), u.UpdateCompleteCommands(tv, messageID)...)
}

func (u UpdateUtils) UpdateAcceptMessages(tv *testvars.TestVars, updRequestMsg *protocolpb.Message, messageID string) []*protocolpb.Message {
	if th, ok := u.t.(helper); ok {
		th.Helper()
	}
	updRequest := protoutils.UnmarshalAny[*updatepb.Request](u.t, updRequestMsg.GetBody())

	return []*protocolpb.Message{
		{
			Id:                 tv.MessageID("update-accepted", messageID),
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

func (u UpdateUtils) UpdateCompleteMessages(tv *testvars.TestVars, updRequestMsg *protocolpb.Message, messageID string) []*protocolpb.Message {
	if th, ok := u.t.(helper); ok {
		th.Helper()
	}
	updRequest := protoutils.UnmarshalAny[*updatepb.Request](u.t, updRequestMsg.GetBody())

	return []*protocolpb.Message{
		{
			Id:                 tv.MessageID("update-completed", messageID),
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

func (u UpdateUtils) UpdateAcceptCompleteMessages(tv *testvars.TestVars, updRequestMsg *protocolpb.Message, messageID string) []*protocolpb.Message {
	if th, ok := u.t.(helper); ok {
		th.Helper()
	}
	return append(u.UpdateAcceptMessages(tv, updRequestMsg, messageID), u.UpdateCompleteMessages(tv, updRequestMsg, messageID)...)
}

func (u UpdateUtils) UpdateRejectMessages(tv *testvars.TestVars, updRequestMsg *protocolpb.Message, messageID string) []*protocolpb.Message {
	if th, ok := u.t.(helper); ok {
		th.Helper()
	}
	updRequest := protoutils.UnmarshalAny[*updatepb.Request](u.t, updRequestMsg.GetBody())

	return []*protocolpb.Message{
		{
			Id:                 tv.MessageID("update-rejected", messageID),
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
