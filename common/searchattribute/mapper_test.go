package searchattribute

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
)

type (
	TestMapper struct {
	}
)

func (t *TestMapper) GetAlias(fieldName string, namespace string) (string, error) {
	if namespace == "test" {
		return "alias_of_" + fieldName, nil
	}
	if namespace == "error" {
		return "", serviceerror.NewInvalidArgument("mapper error")
	}
	return fieldName, nil
}

func (t *TestMapper) GetFieldName(alias string, namespace string) (string, error) {
	if namespace == "test" {
		return strings.TrimPrefix(alias, "alias_of_"), nil
	}
	if namespace == "error" {
		return "", serviceerror.NewInvalidArgument("mapper error")
	}
	return alias, nil
}

func Test_ApplyAliases(t *testing.T) {
	sa := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"field1": {Data: []byte("data1")},
			"field2": {Data: []byte("data2")},
		},
	}
	err := ApplyAliases(&TestMapper{}, sa, "error")
	assert.Error(t, err)
	var invalidArgumentErr *serviceerror.InvalidArgument
	assert.ErrorAs(t, err, &invalidArgumentErr)

	err = ApplyAliases(&TestMapper{}, sa, "namespace1")
	assert.NoError(t, err)
	assert.Len(t, sa.GetIndexedFields(), 2)
	assert.EqualValues(t, "data1", sa.GetIndexedFields()["field1"].GetData())
	assert.EqualValues(t, "data2", sa.GetIndexedFields()["field2"].GetData())

	err = ApplyAliases(&TestMapper{}, sa, "test")
	assert.NoError(t, err)
	assert.Len(t, sa.GetIndexedFields(), 2)
	assert.EqualValues(t, "data1", sa.GetIndexedFields()["alias_of_field1"].GetData())
	assert.EqualValues(t, "data2", sa.GetIndexedFields()["alias_of_field2"].GetData())
}

func Test_SubstituteAliases(t *testing.T) {
	sa := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"alias_of_field1": {Data: []byte("data1")},
			"alias_of_field2": {Data: []byte("data2")},
		},
	}
	err := SubstituteAliases(&TestMapper{}, sa, "error")
	assert.Error(t, err)
	var invalidArgumentErr *serviceerror.InvalidArgument
	assert.ErrorAs(t, err, &invalidArgumentErr)

	err = SubstituteAliases(&TestMapper{}, sa, "namespace1")
	assert.NoError(t, err)
	assert.Len(t, sa.GetIndexedFields(), 2)
	assert.EqualValues(t, "data1", sa.GetIndexedFields()["alias_of_field1"].GetData())
	assert.EqualValues(t, "data2", sa.GetIndexedFields()["alias_of_field2"].GetData())

	err = SubstituteAliases(&TestMapper{}, sa, "test")
	assert.NoError(t, err)
	assert.Len(t, sa.GetIndexedFields(), 2)
	assert.EqualValues(t, "data1", sa.GetIndexedFields()["field1"].GetData())
	assert.EqualValues(t, "data2", sa.GetIndexedFields()["field2"].GetData())
}
