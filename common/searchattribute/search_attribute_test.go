package searchattribute

import (
	"testing"

	"github.com/stretchr/testify/assert"
	enumspb "go.temporal.io/api/enums/v1"
)

func TestConvertIndexedKeyToProto(t *testing.T) {
	assert := assert.New(t)
	m := map[string]interface{}{
		"key1":  float64(1),
		"key2":  float64(2),
		"key3":  float64(3),
		"key4":  float64(4),
		"key5":  float64(5),
		"key6":  float64(6),
		"key1i": 1,
		"key2i": 2,
		"key3i": 3,
		"key4i": 4,
		"key5i": 5,
		"key6i": 6,
		"key1t": enumspb.INDEXED_VALUE_TYPE_STRING,
		"key2t": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"key3t": enumspb.INDEXED_VALUE_TYPE_INT,
		"key4t": enumspb.INDEXED_VALUE_TYPE_DOUBLE,
		"key5t": enumspb.INDEXED_VALUE_TYPE_BOOL,
		"key6t": enumspb.INDEXED_VALUE_TYPE_DATETIME,
		"key1s": "String",
		"key2s": "Keyword",
		"key3s": "Int",
		"key4s": "Double",
		"key5s": "Bool",
		"key6s": "Datetime",
	}
	result := ConvertDynamicConfigToIndexedValueTypes(m)
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_STRING, result["key1"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_KEYWORD, result["key2"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_INT, result["key3"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_DOUBLE, result["key4"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_BOOL, result["key5"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_DATETIME, result["key6"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_STRING, result["key1i"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_KEYWORD, result["key2i"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_INT, result["key3i"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_DOUBLE, result["key4i"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_BOOL, result["key5i"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_DATETIME, result["key6i"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_STRING, result["key1t"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_KEYWORD, result["key2t"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_INT, result["key3t"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_DOUBLE, result["key4t"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_BOOL, result["key5t"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_DATETIME, result["key6t"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_STRING, result["key1s"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_KEYWORD, result["key2s"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_INT, result["key3s"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_DOUBLE, result["key4s"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_BOOL, result["key5s"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_DATETIME, result["key6s"])
	assert.Panics(func() {
		ConvertDynamicConfigToIndexedValueTypes(map[string]interface{}{
			"invalidType": "unknown",
		})
	})
}
