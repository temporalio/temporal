package fieldmask

import (
	"fmt"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// MergeOptions copies `src` struct to `dst` struct using the given FieldMask, and returns the mutated `dst`.
// Only the fields present in FieldMask will be copied to `dst`. `src` and `dst` must be coherent in terms of
// the field names, but it is not required for them to be of the same type. Unexported fields are copied only
// if the corresponding struct filter is empty and `dst` is assignable to `src`.
func MergeOptions(maskpb *fieldmaskpb.FieldMask, src, dst *workflowpb.WorkflowExecutionOptions) (*workflowpb.WorkflowExecutionOptions, error) {
	if maskpb == nil {
		return nil, serviceerror.NewInvalidArgument("UpdateMask must be non-nil")
	}

	// This will error if any of the given fields are not present in WorkflowExecutionOptions
	_, err := fieldmaskpb.New(src, maskpb.GetPaths()...)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("error parsing UpdateMask: %s", err.Error()))
	}

	// Apply masked fields
	for _, p := range maskpb.GetPaths() {
		switch p {
		case "versioning_behavior_override":
			dst.VersioningBehaviorOverride = src.GetVersioningBehaviorOverride()
		default:
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("error parsing UpdateMask: path %s not recognized", p))
		}
	}

	// Confirm resulting versioning behavior is valid
	if dst.GetVersioningBehaviorOverride().GetBehavior() == enumspb.VERSIONING_BEHAVIOR_PINNED && src.GetVersioningBehaviorOverride().GetWorkerDeployment() == nil {
		return nil, serviceerror.NewInvalidArgument("Deployment must be set if behavior override is PINNED")
	}
	return dst, nil
}
