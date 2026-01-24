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

	return commonpb.Priority_builder{
		PriorityKey:    cmp.Or(override.GetPriorityKey(), base.GetPriorityKey()),
		FairnessKey:    cmp.Or(override.GetFairnessKey(), base.GetFairnessKey()),
		FairnessWeight: cmp.Or(override.GetFairnessWeight(), base.GetFairnessWeight()),
	}.Build()
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
	} else if p.GetPriorityKey() < 0 {
		return ErrInvalidPriority
	} else if err := ValidateFairnessKey(p.GetFairnessKey()); err != nil {
		return err
	} else if p.GetFairnessWeight() < 0 {
		return ErrInvalidFairnessWeight
	}
	return nil
}
