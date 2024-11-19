package fieldmask

import (
	"fmt"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// MergeOptions copies `src` struct to `dst` struct using the given FieldMask, and returns the mutated `dst`.
// Only the fields present in FieldMask will be copied to `dst`. `src` and `dst` must be coherent in terms of
// the field names, but it is not required for them to be of the same type. Unexported fields are copied only
// if the corresponding struct filter is empty and `dst` is assignable to `src`.
func MergeOptions(maskpb *fieldmaskpb.FieldMask, src, dst *workflowpb.WorkflowExecutionOptions) (*workflowpb.WorkflowExecutionOptions, error) {
	// Apply masked fields
	for _, p := range maskpb.GetPaths() {
		switch p {
		case "versioning_behavior_override":
			dst.VersioningBehaviorOverride = src.GetVersioningBehaviorOverride()
		default:
			print("got here")
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("error parsing UpdateMask: path %s not supported", p))
		}
	}
	return dst, nil
}
