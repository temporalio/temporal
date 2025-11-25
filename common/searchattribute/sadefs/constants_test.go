package sadefs

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
)

func TestGetDbIndexSearchAttributesDefault(t *testing.T) {
	want := map[string]enumspb.IndexedValueType{
		"Bool01":        enumspb.INDEXED_VALUE_TYPE_BOOL,
		"Bool02":        enumspb.INDEXED_VALUE_TYPE_BOOL,
		"Bool03":        enumspb.INDEXED_VALUE_TYPE_BOOL,
		"Datetime01":    enumspb.INDEXED_VALUE_TYPE_DATETIME,
		"Datetime02":    enumspb.INDEXED_VALUE_TYPE_DATETIME,
		"Datetime03":    enumspb.INDEXED_VALUE_TYPE_DATETIME,
		"Double01":      enumspb.INDEXED_VALUE_TYPE_DOUBLE,
		"Double02":      enumspb.INDEXED_VALUE_TYPE_DOUBLE,
		"Double03":      enumspb.INDEXED_VALUE_TYPE_DOUBLE,
		"Int01":         enumspb.INDEXED_VALUE_TYPE_INT,
		"Int02":         enumspb.INDEXED_VALUE_TYPE_INT,
		"Int03":         enumspb.INDEXED_VALUE_TYPE_INT,
		"Keyword01":     enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"Keyword02":     enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"Keyword03":     enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"Keyword04":     enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"Keyword05":     enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"Keyword06":     enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"Keyword07":     enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"Keyword08":     enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"Keyword09":     enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"Keyword10":     enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"Text01":        enumspb.INDEXED_VALUE_TYPE_TEXT,
		"Text02":        enumspb.INDEXED_VALUE_TYPE_TEXT,
		"Text03":        enumspb.INDEXED_VALUE_TYPE_TEXT,
		"KeywordList01": enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
		"KeywordList02": enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
		"KeywordList03": enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
	}
	got := GetDBIndexSearchAttributes(nil)
	require.Equal(t, want, got.CustomSearchAttributes)
}

func TestGetDbIndexSearchAttributes(t *testing.T) {
	override := map[enumspb.IndexedValueType]int{
		// Bool: no overwrite, it should use default value
		enumspb.INDEXED_VALUE_TYPE_DATETIME:     1,
		enumspb.INDEXED_VALUE_TYPE_DOUBLE:       2,
		enumspb.INDEXED_VALUE_TYPE_INT:          3,
		enumspb.INDEXED_VALUE_TYPE_KEYWORD:      4,
		enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST: 5,
		enumspb.INDEXED_VALUE_TYPE_TEXT:         0,
	}
	want := map[string]enumspb.IndexedValueType{
		"Bool01":        enumspb.INDEXED_VALUE_TYPE_BOOL,
		"Bool02":        enumspb.INDEXED_VALUE_TYPE_BOOL,
		"Bool03":        enumspb.INDEXED_VALUE_TYPE_BOOL,
		"Datetime01":    enumspb.INDEXED_VALUE_TYPE_DATETIME,
		"Double01":      enumspb.INDEXED_VALUE_TYPE_DOUBLE,
		"Double02":      enumspb.INDEXED_VALUE_TYPE_DOUBLE,
		"Int01":         enumspb.INDEXED_VALUE_TYPE_INT,
		"Int02":         enumspb.INDEXED_VALUE_TYPE_INT,
		"Int03":         enumspb.INDEXED_VALUE_TYPE_INT,
		"Keyword01":     enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"Keyword02":     enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"Keyword03":     enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"Keyword04":     enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"KeywordList01": enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
		"KeywordList02": enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
		"KeywordList03": enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
		"KeywordList04": enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
		"KeywordList05": enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
	}
	got := GetDBIndexSearchAttributes(override)
	require.Equal(t, want, got.CustomSearchAttributes)
}

// This test is to make sure all system search attributes have a corresponding
// column name in SQL DB.
// If this test is failing, make sure that you updated the SQL DB schemas, and
// you mapped the search attribute constant to the column name.
func TestValidateSqlDbSystemNameToColNameMap(t *testing.T) {
	require.Contains(t, sqlDbSystemNameToColName, NamespaceID)
	system := System()
	for key := range system {
		require.Contains(t, sqlDbSystemNameToColName, key)
	}
}

func TestIsCustomSearchAttributeFieldName(t *testing.T) {
	require.False(t, IsPreallocatedCSAFieldName("Keyword00", enumspb.INDEXED_VALUE_TYPE_KEYWORD))
	require.False(t, IsPreallocatedCSAFieldName("Int00", enumspb.INDEXED_VALUE_TYPE_INT))
	require.False(t, IsPreallocatedCSAFieldName("Int100", enumspb.INDEXED_VALUE_TYPE_INT))
	require.False(t, IsPreallocatedCSAFieldName("Int00", enumspb.INDEXED_VALUE_TYPE_KEYWORD))
	require.False(t, IsPreallocatedCSAFieldName("Unspecified01", enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED))

	require.True(t, IsPreallocatedCSAFieldName("Int01", enumspb.INDEXED_VALUE_TYPE_INT))
	require.True(t, IsPreallocatedCSAFieldName("Int02", enumspb.INDEXED_VALUE_TYPE_INT))
	require.True(t, IsPreallocatedCSAFieldName("Int10", enumspb.INDEXED_VALUE_TYPE_INT))
	require.True(t, IsPreallocatedCSAFieldName("Keyword05", enumspb.INDEXED_VALUE_TYPE_KEYWORD))
	require.True(t, IsPreallocatedCSAFieldName("Bool45", enumspb.INDEXED_VALUE_TYPE_BOOL))
	require.True(t, IsPreallocatedCSAFieldName("Double45", enumspb.INDEXED_VALUE_TYPE_DOUBLE))
	require.True(t, IsPreallocatedCSAFieldName("Datetime45", enumspb.INDEXED_VALUE_TYPE_DATETIME))
	require.True(t, IsPreallocatedCSAFieldName("Text45", enumspb.INDEXED_VALUE_TYPE_TEXT))
	require.True(t, IsPreallocatedCSAFieldName("KeywordList45", enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST))
}
