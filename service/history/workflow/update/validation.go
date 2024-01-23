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

package update

import (
	updatepb "go.temporal.io/api/update/v1"
	"google.golang.org/protobuf/proto"
)

func notZero[T comparable](v T, label string, msg proto.Message) func() error {
	return func() error {
		var zero T
		if v == zero {
			return invalidArgf("invalid %T: %v is not set", msg, label)
		}
		return nil
	}
}

func eq[T comparable](
	left T,
	leftLbl string,
	right T,
	rightLbl string,
	msg proto.Message,
) func() error {
	return func() error {
		if left != right {
			return invalidArgf("invalid %T: %v != %v", msg, leftLbl, rightLbl)
		}
		return nil
	}
}

func validate(vs ...func() error) error {
	for _, v := range vs {
		if err := v(); err != nil {
			return err
		}
	}
	return nil
}

func validateRequestMsg(updateID string, msg *updatepb.Request) error {
	return validateRequestMsgPrefix(updateID, "", msg)
}

func validateRequestMsgPrefix(
	updateID string,
	prefix string,
	msg *updatepb.Request,
) error {
	return validate(
		notZero(msg, prefix+"body", msg),
		notZero(msg.GetMeta(), prefix+"meta", msg),
		notZero(msg.GetMeta().GetUpdateId(), prefix+"meta.update_id", msg),
		eq(msg.GetMeta().GetUpdateId(), prefix+"meta.update_id", updateID, updateID, msg),
		notZero(msg.GetInput(), prefix+"input", msg),
		notZero(msg.GetInput().GetName(), prefix+"input.name", msg),
	)
}

func validateAcceptanceMsg(msg *updatepb.Acceptance) error {
	return validate(
		notZero(msg, "body", msg),
		notZero(msg.GetAcceptedRequestSequencingEventId(), "accepted_request_sequencing_event_id", msg),
		notZero(msg.GetAcceptedRequestMessageId(), "accepted_request_message_id", msg),
	)
}

func validateResponseMsg(updateID string, msg *updatepb.Response) error {
	return validate(
		notZero(msg, "body", msg),
		notZero(msg.GetMeta(), "meta", msg),
		notZero(msg.GetMeta().GetUpdateId(), "meta.update_id", msg),
		eq(msg.GetMeta().GetUpdateId(), "meta.update_id", updateID, updateID, msg),
		notZero(msg.GetOutcome(), "outcome", msg),
	)
}

// ValidateWorkflowExecutionUpdateRejectionMessage validates the presence of
// expected fields on updatepb.Rejection messages.
func validateRejectionMsg(msg *updatepb.Rejection) error {
	return validate(
		notZero(msg, "body", msg),
	)
}
