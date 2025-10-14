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
	ErrInvalidPriority       = serviceerror.NewInvalidArgument("priority key can't be negative")
	ErrFairnessKeyLength     = serviceerror.NewInvalidArgument("fairness key length exceeds limit")
	ErrInvalidFairnessWeight = serviceerror.NewInvalidArgument("must be greater than zero")
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

func ValidateFairnessKey(key string) error {
	if len(key) > fairnessKeyMaxLength {
		return ErrFairnessKeyLength
	}
	return nil
}

func ValidateFairnessWeight(weight float32) error {
	if weight <= 0 {
		return ErrInvalidFairnessWeight
	}
	return nil
}

func Validate(p *commonpb.Priority) error {
	if p == nil {
		return nil
	} else if p.PriorityKey < 0 {
		return ErrInvalidPriority
	} else if err := ValidateFairnessKey(p.FairnessKey); err != nil {
		return err
	} else if p.FairnessWeight < 0 {
		return ErrInvalidFairnessWeight
	}
	return nil
}
