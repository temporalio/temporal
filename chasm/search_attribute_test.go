package chasm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/searchattribute/sadefs"
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
		"status":    VisibilityValueKeyword("active"),
		"timestamp": VisibilityValueTime(now),
		"tags":      VisibilityValueStringSlice([]string{"tag1", "tag2"}),
	}
	m := NewSearchAttributesMap(values)

	t.Run("GetBool", func(t *testing.T) {
		val, ok := SearchAttributeValue(m, boolAttr)
		require.True(t, ok)
		require.True(t, val)
	})

	t.Run("GetInt64", func(t *testing.T) {
		val, ok := SearchAttributeValue(m, intAttr)
		require.True(t, ok)
		require.Equal(t, int64(42), val)
	})

	t.Run("GetFloat64", func(t *testing.T) {
		val, ok := SearchAttributeValue(m, doubleAttr)
		require.True(t, ok)
		require.InDelta(t, 3.14, val, 0.0001)
	})

	t.Run("GetString", func(t *testing.T) {
		val, ok := SearchAttributeValue(m, keywordAttr)
		require.True(t, ok)
		require.Equal(t, "active", val)
	})

	t.Run("GetTime", func(t *testing.T) {
		val, ok := SearchAttributeValue(m, datetimeAttr)
		require.True(t, ok)
		require.True(t, now.Equal(val))
	})

	t.Run("GetStringSlice", func(t *testing.T) {
		val, ok := SearchAttributeValue(m, keywordListAttr)
		require.True(t, ok)
		require.Equal(t, []string{"tag1", "tag2"}, val)
	})

	t.Run("NotFound", func(t *testing.T) {
		missingAttr := NewSearchAttributeBool("missing", SearchAttributeFieldBool02)
		val, ok := SearchAttributeValue(m, missingAttr)
		require.False(t, ok)
		require.False(t, val)
	})

	t.Run("NilMap", func(t *testing.T) {
		emptyMap := NewSearchAttributesMap(nil)
		val, ok := SearchAttributeValue(emptyMap, boolAttr)
		require.False(t, ok)
		require.False(t, val)
	})
}

func TestSearchAttributesMap_ToProto(t *testing.T) {
	t.Run("NilReceiver", func(t *testing.T) {
		var m *SearchAttributesMap
		require.Nil(t, m.ToProto())
	})

	t.Run("NilValues", func(t *testing.T) {
		m := &SearchAttributesMap{values: nil}
		require.Nil(t, m.ToProto())
	})

	t.Run("EmptyMap", func(t *testing.T) {
		m := NewSearchAttributesMap(map[string]VisibilityValue{})
		proto := m.ToProto()
		require.NotNil(t, proto)
		require.Empty(t, proto.IndexedFields)
	})

	t.Run("AllTypes", func(t *testing.T) {
		now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		values := map[string]VisibilityValue{
			"bool_key":         VisibilityValueBool(true),
			"int_key":          VisibilityValueInt64(42),
			"double_key":       VisibilityValueFloat64(3.14),
			"keyword_key":      VisibilityValueKeyword("active"),
			"datetime_key":     VisibilityValueTime(now),
			"keyword_list_key": VisibilityValueStringSlice([]string{"a", "b"}),
		}
		m := NewSearchAttributesMap(values)
		proto := m.ToProto()

		require.NotNil(t, proto)
		require.Len(t, proto.IndexedFields, 6)

		// Verify each field was encoded correctly by decoding and comparing.
		decoded, err := sadefs.DecodeValue(proto.IndexedFields["bool_key"], enumspb.INDEXED_VALUE_TYPE_BOOL, false)
		require.NoError(t, err)
		require.Equal(t, true, decoded)

		decoded, err = sadefs.DecodeValue(proto.IndexedFields["int_key"], enumspb.INDEXED_VALUE_TYPE_INT, false)
		require.NoError(t, err)
		require.Equal(t, int64(42), decoded)

		decoded, err = sadefs.DecodeValue(proto.IndexedFields["double_key"], enumspb.INDEXED_VALUE_TYPE_DOUBLE, false)
		require.NoError(t, err)
		require.InDelta(t, 3.14, decoded, 0.0001)

		decoded, err = sadefs.DecodeValue(proto.IndexedFields["keyword_key"], enumspb.INDEXED_VALUE_TYPE_KEYWORD, false)
		require.NoError(t, err)
		require.Equal(t, "active", decoded)

		decoded, err = sadefs.DecodeValue(proto.IndexedFields["datetime_key"], enumspb.INDEXED_VALUE_TYPE_DATETIME, false)
		require.NoError(t, err)
		require.True(t, now.Equal(decoded.(time.Time)))

		decoded, err = sadefs.DecodeValue(proto.IndexedFields["keyword_list_key"], enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, false)
		require.NoError(t, err)
		require.Equal(t, []string{"a", "b"}, decoded)
	})
}
