package searchattribute

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	enumspb "go.temporal.io/api/enums/v1"
)

func Test_IsValid(t *testing.T) {
	assert := assert.New(t)
	typeMap := NameTypeMap{customSearchAttributes: map[string]enumspb.IndexedValueType{
		"key1": enumspb.INDEXED_VALUE_TYPE_TEXT,
		"key2": enumspb.INDEXED_VALUE_TYPE_INT,
		"key3": enumspb.INDEXED_VALUE_TYPE_BOOL,
	}}

	isDefined := typeMap.IsDefined("RunId")
	assert.True(isDefined)
	isDefined = typeMap.IsDefined("TemporalChangeVersion")
	assert.True(isDefined)
	isDefined = typeMap.IsDefined("key1")
	assert.True(isDefined)

	isDefined = NameTypeMap{}.IsDefined("key1")
	assert.False(isDefined)
	isDefined = typeMap.IsDefined("key4")
	assert.False(isDefined)
	isDefined = typeMap.IsDefined("NamespaceId")
	assert.False(isDefined)
}

func Test_GetType(t *testing.T) {
	assert := assert.New(t)
	typeMap := NameTypeMap{customSearchAttributes: map[string]enumspb.IndexedValueType{
		"key1": enumspb.INDEXED_VALUE_TYPE_TEXT,
		"key2": enumspb.INDEXED_VALUE_TYPE_INT,
		"key3": enumspb.INDEXED_VALUE_TYPE_BOOL,
	}}

	ivt, err := typeMap.GetType("key1")
	assert.NoError(err)
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_TEXT, ivt)
	ivt, err = typeMap.GetType("key2")
	assert.NoError(err)
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_INT, ivt)
	ivt, err = typeMap.GetType("key3")
	assert.NoError(err)
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_BOOL, ivt)
	ivt, err = typeMap.GetType("RunId")
	assert.NoError(err)
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_KEYWORD, ivt)
	ivt, err = typeMap.GetType("TemporalChangeVersion")
	assert.NoError(err)
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, ivt)
	ivt, err = typeMap.GetType("NamespaceId")
	assert.Error(err)
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, ivt)

	ivt, err = NameTypeMap{}.GetType("key1")
	assert.Error(err)
	assert.True(errors.Is(err, ErrInvalidName))
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, ivt)
	ivt, err = typeMap.GetType("key4")
	assert.Error(err)
	assert.True(errors.Is(err, ErrInvalidName))
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, ivt)
}
