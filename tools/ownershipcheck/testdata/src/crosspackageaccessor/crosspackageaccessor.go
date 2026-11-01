// crosspackageaccessor: a borrowed map obtained from an accessor in ANOTHER
// package is embedded into a proto returned from an exported handler. Detection
// relies on the imported resultOwnership fact for accessorlib.CustomSearchAttributes.
// (The #10707 shape.)
package crosspackageaccessor

import "accessorlib"

type SearchAttributes struct {
	IndexedFields map[string]*accessorlib.Payload
}

func (*SearchAttributes) ProtoReflect() any { return nil }

type ExecutionInfo struct {
	SearchAttributes *SearchAttributes
}

func (*ExecutionInfo) ProtoReflect() any { return nil }

type Operation struct {
	Visibility *accessorlib.Vis
}

func (o *Operation) BuildExecutionInfo() *ExecutionInfo {
	return &ExecutionInfo{
		SearchAttributes: &SearchAttributes{
			IndexedFields: o.Visibility.CustomSearchAttributes(), // want `borrowed map embedded into`
		},
	}
}

// BuildSafe uses the cloning accessor (inferred owned across packages) and must
// stay silent.
func (o *Operation) BuildSafe() *ExecutionInfo {
	return &ExecutionInfo{
		SearchAttributes: &SearchAttributes{
			IndexedFields: o.Visibility.SafeSearchAttributes(),
		},
	}
}
