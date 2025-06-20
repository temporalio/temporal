package priorities

import (
	"cmp"

	commonpb "go.temporal.io/api/common/v1"
)

func Merge(
	base *commonpb.Priority,
	override *commonpb.Priority,
) *commonpb.Priority {
	if base == nil || override == nil {
		return cmp.Or(override, base)
	}

	return &commonpb.Priority{
		PriorityKey: cmp.Or(override.PriorityKey, base.PriorityKey),
	}
}
