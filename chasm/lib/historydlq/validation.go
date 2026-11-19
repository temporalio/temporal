package historydlq

import (
	"go.temporal.io/api/serviceerror"
	historydlqpb "go.temporal.io/server/chasm/lib/historydlq/gen/historydlqpb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func validateEntryRef(ref MessageRef) error {
	if ref.BusinessID == "" ||
		ref.RunID == "" ||
		ref.EntryKey == "" {
		return serviceerror.NewInvalidArgument("history DLQ requires a segment Business ID, Run ID, and entry key")
	}
	return validateQueue(ref.Queue)
}

func validateMessageRef(ref MessageRef) error {
	if err := validateEntryRef(ref); err != nil {
		return err
	}
	if ref.MessageID < 0 ||
		ref.EnqueuedAt.IsZero() ||
		timestamppb.New(ref.EnqueuedAt).CheckValid() != nil {
		return serviceerror.NewInvalidArgument("history DLQ requires a full segment entry reference")
	}
	return nil
}

func validateResolutionReason(resolutionReason historydlqpb.Resolution) error {
	if resolutionReason != historydlqpb.RESOLUTION_MERGED && resolutionReason != historydlqpb.RESOLUTION_PURGED {
		return serviceerror.NewInvalidArgument("invalid history DLQ resolution")
	}
	return nil
}
