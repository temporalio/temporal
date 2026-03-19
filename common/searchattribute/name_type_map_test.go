package searchattribute

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

func Test_IsValid(t *testing.T) {
	r := require.New(t)
	typeMap := NameTypeMap{customSearchAttributes: map[string]enumspb.IndexedValueType{
		"key1": enumspb.INDEXED_VALUE_TYPE_TEXT,
		"key2": enumspb.INDEXED_VALUE_TYPE_INT,
		"key3": enumspb.INDEXED_VALUE_TYPE_BOOL,
	}}

	isDefined := typeMap.IsDefined("RunId")
	r.True(isDefined)
	isDefined = typeMap.IsDefined("TemporalChangeVersion")
	r.True(isDefined)
	isDefined = typeMap.IsDefined("key1")
	r.True(isDefined)

	isDefined = NameTypeMap{}.IsDefined("key1")
	r.False(isDefined)
	isDefined = typeMap.IsDefined("key4")
	r.False(isDefined)
	isDefined = typeMap.IsDefined("NamespaceId")
	r.False(isDefined)
}

func Test_GetType(t *testing.T) {
	r := require.New(t)
	typeMap := NameTypeMap{customSearchAttributes: map[string]enumspb.IndexedValueType{
		"key1": enumspb.INDEXED_VALUE_TYPE_TEXT,
		"key2": enumspb.INDEXED_VALUE_TYPE_INT,
		"key3": enumspb.INDEXED_VALUE_TYPE_BOOL,
	}}

	ivt, err := typeMap.GetType("key1")
	r.NoError(err)
	r.Equal(enumspb.INDEXED_VALUE_TYPE_TEXT, ivt)
	ivt, err = typeMap.GetType("key2")
	r.NoError(err)
	r.Equal(enumspb.INDEXED_VALUE_TYPE_INT, ivt)
	ivt, err = typeMap.GetType("key3")
	r.NoError(err)
	r.Equal(enumspb.INDEXED_VALUE_TYPE_BOOL, ivt)
	ivt, err = typeMap.GetType("RunId")
	r.NoError(err)
	r.Equal(enumspb.INDEXED_VALUE_TYPE_KEYWORD, ivt)
	ivt, err = typeMap.GetType("TemporalChangeVersion")
	r.NoError(err)
	r.Equal(enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, ivt)
	ivt, err = typeMap.GetType("NamespaceId")
	r.Error(err)
	r.Equal(enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, ivt)

	ivt, err = NameTypeMap{}.GetType("key1")
	r.Error(err)
	r.ErrorIs(err, sadefs.ErrInvalidName)
	r.Equal(enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, ivt)
	ivt, err = typeMap.GetType("key4")
	r.Error(err)
	r.ErrorIs(err, sadefs.ErrInvalidName)
	r.Equal(enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, ivt)
}
