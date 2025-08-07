package priorities

import (
	"cmp"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
)

const (
	fairnessKeyMaxLength = 64
)

var (
	ErrInvalidPriority       = serviceerror.NewInvalidArgument("PriorityKey can't be negative")
	ErrFairnessKeyLength     = serviceerror.NewInvalidArgument("FairnessKey length exceeds limit")
	ErrInvalidFairnessWeight = serviceerror.NewInvalidArgument("FairnessWeight can't be negative")
)

func Merge(
	base *commonpb.Priority,
	override *commonpb.Priority,
) *commonpb.Priority {
	if base == nil {
		return override
	} else if override == nil {
		return base
	}

	return &commonpb.Priority{
		PriorityKey:    cmp.Or(override.PriorityKey, base.PriorityKey),
		FairnessKey:    cmp.Or(override.FairnessKey, base.FairnessKey),
		FairnessWeight: cmp.Or(override.FairnessWeight, base.FairnessWeight),
	}
}

func Validate(p *commonpb.Priority) error {
	if p == nil {
		return nil
	} else if p.PriorityKey < 0 {
		return ErrInvalidPriority
	} else if len(p.FairnessKey) > fairnessKeyMaxLength {
		return ErrFairnessKeyLength
	} else if p.FairnessWeight < 0 {
		return ErrInvalidFairnessWeight
	}
	return nil
}
