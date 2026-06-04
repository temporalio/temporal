package chasm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
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

func TestNewSearchAttributesMapFromProto(t *testing.T) {
	t.Run("NilSearchAttributes", func(t *testing.T) {
		m, err := newSearchAttributesMapFromProto(nil)
		require.NoError(t, err)
		require.Empty(t, m.values)
	})

	t.Run("EmptyIndexedFields", func(t *testing.T) {
		m, err := newSearchAttributesMapFromProto(&commonpb.SearchAttributes{
			IndexedFields: map[string]*commonpb.Payload{},
		})
		require.NoError(t, err)
		require.Empty(t, m.values)
	})

	t.Run("SingleBoolValue", func(t *testing.T) {
		sa := &commonpb.SearchAttributes{
			IndexedFields: map[string]*commonpb.Payload{
				"completed": sadefs.MustEncodeValue(true, enumspb.INDEXED_VALUE_TYPE_BOOL),
			},
		}
		m, err := newSearchAttributesMapFromProto(sa)
		require.NoError(t, err)

		boolAttr := NewSearchAttributeBool("completed", SearchAttributeFieldBool01)
		val, ok := SearchAttributeValue(m, boolAttr)
		require.True(t, ok)
		require.True(t, val)
	})

	t.Run("MultipleValueTypes", func(t *testing.T) {
		now := time.Now().UTC().Truncate(time.Millisecond)
		sa := &commonpb.SearchAttributes{
			IndexedFields: map[string]*commonpb.Payload{
				"completed": sadefs.MustEncodeValue(true, enumspb.INDEXED_VALUE_TYPE_BOOL),
				"count":     sadefs.MustEncodeValue(int64(42), enumspb.INDEXED_VALUE_TYPE_INT),
				"score":     sadefs.MustEncodeValue(3.14, enumspb.INDEXED_VALUE_TYPE_DOUBLE),
				"status":    sadefs.MustEncodeValue("active", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
				"timestamp": sadefs.MustEncodeValue(now, enumspb.INDEXED_VALUE_TYPE_DATETIME),
				"tags":      sadefs.MustEncodeValue([]string{"tag1", "tag2"}, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST),
			},
		}
		m, err := newSearchAttributesMapFromProto(sa)
		require.NoError(t, err)

		boolAttr := NewSearchAttributeBool("completed", SearchAttributeFieldBool01)
		boolVal, ok := SearchAttributeValue(m, boolAttr)
		require.True(t, ok)
		require.True(t, boolVal)

		intAttr := NewSearchAttributeInt("count", SearchAttributeFieldInt01)
		intVal, ok := SearchAttributeValue(m, intAttr)
		require.True(t, ok)
		require.Equal(t, int64(42), intVal)

		doubleAttr := NewSearchAttributeDouble("score", SearchAttributeFieldDouble01)
		doubleVal, ok := SearchAttributeValue(m, doubleAttr)
		require.True(t, ok)
		require.InDelta(t, 3.14, doubleVal, 0.0001)

		keywordAttr := NewSearchAttributeKeyword("status", SearchAttributeFieldKeyword01)
		keywordVal, ok := SearchAttributeValue(m, keywordAttr)
		require.True(t, ok)
		require.Equal(t, "active", keywordVal)

		datetimeAttr := NewSearchAttributeDateTime("timestamp", SearchAttributeFieldDateTime01)
		timeVal, ok := SearchAttributeValue(m, datetimeAttr)
		require.True(t, ok)
		require.True(t, now.Equal(timeVal))

		keywordListAttr := NewSearchAttributeKeywordList("tags", SearchAttributeFieldKeywordList01)
		listVal, ok := SearchAttributeValue(m, keywordListAttr)
		require.True(t, ok)
		require.Equal(t, []string{"tag1", "tag2"}, listVal)
	})

	t.Run("InvalidPayload", func(t *testing.T) {
		sa := &commonpb.SearchAttributes{
			IndexedFields: map[string]*commonpb.Payload{
				"bad": {Data: []byte("not valid")},
			},
		}
		m, err := newSearchAttributesMapFromProto(sa)
		// Current implementation returns nil error on decode failure
		require.NoError(t, err)
		require.Empty(t, m.values)
	})
}
