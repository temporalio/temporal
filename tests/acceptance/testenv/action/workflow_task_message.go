// The MIT License
//
// Copyright (c) 2025 Temporal Technologies Inc.  All rights reserved.
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
// FITNESS FOR A PARTICULAR PURPOSE AND NGetONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
