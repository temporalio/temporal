package main

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
)

func fieldByName(t *testing.T, v any, name string) reflect.StructField {
	t.Helper()
	sf, ok := reflect.TypeOf(v).Elem().FieldByName(name)
	require.True(t, ok, "field %s not found", name)
	return sf
}

// TestBuildFieldModel_Kinds checks the field-kind dispatch against real proto
// struct fields.
func TestBuildFieldModel_Kinds(t *testing.T) {
	imports := newImports()

	scalar := buildFieldModel(fieldByName(t, &commonpb.WorkflowType{}, "Name"), "*T", imports)
	require.Equal(t, "NewScalar", scalar.Constructor)
	require.Equal(t, "m.Name != \"\"", scalar.IsSetExpr)

	msg := buildFieldModel(fieldByName(t, &commonpb.Memo{}, "Fields"), "*T", imports)
	require.Equal(t, "NewMap", msg.Constructor)
}

// proto3 optional scalars are absent from Temporal's public API, so this test
// synthesizes the struct shape protoc-gen-go emits for `optional int32 count`
// to lock in the pointer-to-scalar code path.
type optionalScalarStruct struct {
	Count *int32 `protobuf:"varint,1,opt,name=count,proto3,oneof"`
}

func TestBuildFieldModel_ProtoOptionalScalar(t *testing.T) {
	sf := fieldByName(t, &optionalScalarStruct{}, "Count")
	fm := buildFieldModel(sf, "*optionalScalarStruct", newImports())

	require.Equal(t, "NewScalar", fm.Constructor)
	require.Equal(t, "count", fm.ProtoName)
	require.Equal(t, "*int32", fm.ValueExpr)
	require.Equal(t, "m.Count != nil", fm.IsSetExpr, "presence is the non-nil pointer")
	require.Equal(t, "protofield.Scalar[*optionalScalarStruct, *int32]", fm.HandleType)
}
