package update

import (
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	"google.golang.org/protobuf/proto"
)

func notZero[T comparable](v T, label string, msg proto.Message) func() error {
	return func() error {
		var zero T
		if v == zero {
			return serviceerror.NewInvalidArgumentf("invalid %T: %v is not set", msg, label)
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
			return serviceerror.NewInvalidArgumentf("invalid %T: %v != %v", msg, leftLbl, rightLbl)
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
