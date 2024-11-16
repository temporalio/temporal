package fieldmask

import (
	"fmt"
	fieldmask_utils "github.com/mennanov/fieldmask-utils"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// A function that maps field mask field names to the names used in Go structs.
// This can be replaced by a generic CamelCase function at some point.
// Using this for now to be simple.
func naming(s string) string {
	if s == "versioning_behavior_override" {
		return "VersioningBehaviorOverride"
	}
	return s
}

// MergeOptions copies `src` struct to `dst` struct using the given FieldMask, and returns the mutated `dst`.
// Only the fields present in FieldMask will be copied to `dst`. `src` and `dst` must be coherent in terms of
// the field names, but it is not required for them to be of the same type. Unexported fields are copied only
// if the corresponding struct filter is empty and `dst` is assignable to `src`.
func MergeOptions(maskpb *fieldmaskpb.FieldMask, src, dst *workflowpb.WorkflowExecutionOptions) (*workflowpb.WorkflowExecutionOptions, error) {
	mask, err := fieldmask_utils.MaskFromPaths(maskpb.GetPaths(), naming)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("error parsing UpdateMask: %s", err.Error()))
	}
	err = fieldmask_utils.StructToStruct(mask, src, dst)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("error applying UpdateMask: %s", err.Error()))
	}
	return dst, nil
}
