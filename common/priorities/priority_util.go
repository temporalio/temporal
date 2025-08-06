package priorities

import (
	"cmp"

	commonpb "go.temporal.io/api/common/v1"
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
