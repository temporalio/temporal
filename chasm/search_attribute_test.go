package chasm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSearchAttributesMap_Get(t *testing.T) {
	// Define test search attributes
	boolAttr := NewSearchAttributeBool("completed", SearchAttributeFieldBool01)
	intAttr := NewSearchAttributeInt("count", SearchAttributeFieldInt01)
	doubleAttr := NewSearchAttributeDouble("score", SearchAttributeFieldDouble01)
	keywordAttr := NewSearchAttributeKeyword("status", SearchAttributeFieldKeyword01)
	datetimeAttr := NewSearchAttributeDateTime("timestamp", SearchAttributeFieldDateTime01)
	keywordListAttr := NewSearchAttributeKeywordList("tags", SearchAttributeFieldKeywordList01)

	now := time.Now()

	// Create map with test values
	values := map[string]VisibilityValue{
		"completed": VisibilityValueBool(true),
		"count":     VisibilityValueInt64(42),
		"score":     VisibilityValueFloat64(3.14),
		"status":    VisibilityValueString("active"),
		"timestamp": VisibilityValueTime(now),
		"tags":      VisibilityValueStringSlice([]string{"tag1", "tag2"}),
	}
	m := NewSearchAttributesMap(values)

	t.Run("GetBool", func(t *testing.T) {
		val, ok := GetValue(m, boolAttr)
		assert.True(t, ok)
		assert.True(t, val)
	})

	t.Run("GetInt64", func(t *testing.T) {
		val, ok := GetValue(m, intAttr)
		assert.True(t, ok)
		assert.Equal(t, int64(42), val)
	})

	t.Run("GetFloat64", func(t *testing.T) {
		val, ok := GetValue(m, doubleAttr)
		assert.True(t, ok)
		assert.InDelta(t, 3.14, val, 0.0001)
	})

	t.Run("GetString", func(t *testing.T) {
		val, ok := GetValue(m, keywordAttr)
		assert.True(t, ok)
		assert.Equal(t, "active", val)
	})

	t.Run("GetTime", func(t *testing.T) {
		val, ok := GetValue(m, datetimeAttr)
		assert.True(t, ok)
		assert.True(t, now.Equal(val))
	})

	t.Run("GetStringSlice", func(t *testing.T) {
		val, ok := GetValue(m, keywordListAttr)
		assert.True(t, ok)
		assert.Equal(t, []string{"tag1", "tag2"}, val)
	})

	t.Run("NotFound", func(t *testing.T) {
		missingAttr := NewSearchAttributeBool("missing", SearchAttributeFieldBool02)
		val, ok := GetValue(m, missingAttr)
		assert.False(t, ok)
		assert.False(t, val)
	})

	t.Run("NilMap", func(t *testing.T) {
		emptyMap := NewSearchAttributesMap(nil)
		val, ok := GetValue(emptyMap, boolAttr)
		assert.False(t, ok)
		assert.False(t, val)
	})
}
