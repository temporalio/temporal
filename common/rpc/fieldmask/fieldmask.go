package fieldmask

import (
	"fmt"

	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
)

// MergeOptions copies the given paths in `src` struct to `dst` struct
func MergeOptions(paths []string, src, dst *workflowpb.WorkflowExecutionOptions) (*workflowpb.WorkflowExecutionOptions, error) {
	// Apply masked fields
	for _, p := range paths {
		switch p {
		case "versioning_behavior_override":
			dst.VersioningBehaviorOverride = src.GetVersioningBehaviorOverride()
		default:
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("error parsing UpdateMask: path %s not supported", p))
		}
	}
	return dst, nil
}
