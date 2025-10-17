package chasm

import (
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
)

type (
	SearchAttribute struct {
		key string
		// alias refers to a fully formed schema field, which is either a Predefined or CHASM search attribute
		alias     string
		valueType enumspb.IndexedValueType
		value     any
	}

	SearchAttributeBool struct {
		SearchAttribute
	}

	SearchAttributeTime struct {
		SearchAttribute
	}

	SearchAttributeFloat64 struct {
		SearchAttribute
	}

	SearchAttributeKeyword struct {
		SearchAttribute
	}

	SearchAttributeText struct {
		SearchAttribute
	}

	SearchAttributeKeywordList struct {
		SearchAttribute
	}
)

func ResolveFieldName(valueType enumspb.IndexedValueType, index int) string {
	// Columns are named like TemporalBool01, TemporalDatetime01, TemporalDouble01, TemporalInt01.
	suffix := fmt.Sprintf("%02d", index)
	switch valueType {
	case enumspb.INDEXED_VALUE_TYPE_BOOL:
		return "TemporalBool" + suffix
	case enumspb.INDEXED_VALUE_TYPE_DATETIME:
		return "TemporalDatetime" + suffix
	case enumspb.INDEXED_VALUE_TYPE_DOUBLE:
		return "TemporalDouble" + suffix
	case enumspb.INDEXED_VALUE_TYPE_INT:
		return "TemporalInt" + suffix
	case enumspb.INDEXED_VALUE_TYPE_KEYWORD:
		return "TemporalKeyword" + suffix
	case enumspb.INDEXED_VALUE_TYPE_TEXT:
		return "TemporalText" + suffix
	case enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST:
		return "TemporalKeywordList" + suffix
	default:
		return ""
	}
}

// GetKey returns the search attribute key.
func (s *SearchAttribute) GetKey() string {
	return s.key
}

// GetAlias returns the search attribute alias (field name).
func (s *SearchAttribute) GetAlias() string {
	return s.alias
}

// GetValueType returns the indexed value type.
func (s *SearchAttribute) GetValueType() enumspb.IndexedValueType {
	return s.valueType
}

// GetValue returns the search attribute value.
func (s *SearchAttribute) GetValue() any {
	return s.value
}

func NewSearchAttributeBoolByIndex(key string, index int) *SearchAttributeBool {
	return &SearchAttributeBool{
		SearchAttribute: SearchAttribute{
			key:       key,
			alias:     ResolveFieldName(enumspb.INDEXED_VALUE_TYPE_BOOL, index),
			valueType: enumspb.INDEXED_VALUE_TYPE_BOOL,
		},
	}
}

func NewSearchAttributeBoolByAlias(key string, alias string) *SearchAttributeBool {
	return &SearchAttributeBool{
		SearchAttribute: SearchAttribute{
			key:       key,
			alias:     alias,
			valueType: enumspb.INDEXED_VALUE_TYPE_BOOL,
		},
	}
}

func (s *SearchAttributeBool) SetValue(value bool) {
	s.value = value
}

func NewSearchAttributeTimeByIndex(key string, index int) *SearchAttributeTime {
	return &SearchAttributeTime{
		SearchAttribute: SearchAttribute{
			key:       key,
			alias:     ResolveFieldName(enumspb.INDEXED_VALUE_TYPE_DATETIME, index),
			valueType: enumspb.INDEXED_VALUE_TYPE_DATETIME,
		},
	}
}

func NewSearchAttributeTimeByAlias(key string, alias string) *SearchAttributeTime {
	return &SearchAttributeTime{
		SearchAttribute: SearchAttribute{
			key:       key,
			alias:     alias,
			valueType: enumspb.INDEXED_VALUE_TYPE_DATETIME,
		},
	}
}

func (s *SearchAttributeTime) SetValue(value time.Time) {
	s.value = value
}

func NewSearchAttributeFloat64ByIndex(key string, index int) *SearchAttributeFloat64 {
	return &SearchAttributeFloat64{
		SearchAttribute: SearchAttribute{
			key:       key,
			alias:     ResolveFieldName(enumspb.INDEXED_VALUE_TYPE_DOUBLE, index),
			valueType: enumspb.INDEXED_VALUE_TYPE_DOUBLE,
		},
	}
}

func NewSearchAttributeFloat64ByAlias(key string, alias string) *SearchAttributeFloat64 {
	return &SearchAttributeFloat64{
		SearchAttribute: SearchAttribute{
			key:       key,
			alias:     alias,
			valueType: enumspb.INDEXED_VALUE_TYPE_DOUBLE,
		},
	}
}

func (s *SearchAttributeFloat64) SetValue(value float64) {
	s.value = value
}

func NewSearchAttributeKeywordByIndex(key string, index int) *SearchAttributeKeyword {
	return &SearchAttributeKeyword{
		SearchAttribute: SearchAttribute{
			key:       key,
			alias:     ResolveFieldName(enumspb.INDEXED_VALUE_TYPE_KEYWORD, index),
			valueType: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	}
}

func NewSearchAttributeKeywordByAlias(key string, alias string) *SearchAttributeKeyword {
	return &SearchAttributeKeyword{
		SearchAttribute: SearchAttribute{
			key:       key,
			alias:     alias,
			valueType: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	}
}

func (s *SearchAttributeKeyword) SetValue(value string) {
	s.value = value
}

func NewSearchAttributeTextByIndex(key string, index int) *SearchAttributeText {
	return &SearchAttributeText{
		SearchAttribute: SearchAttribute{
			key:       key,
			alias:     ResolveFieldName(enumspb.INDEXED_VALUE_TYPE_TEXT, index),
			valueType: enumspb.INDEXED_VALUE_TYPE_TEXT,
		},
	}
}

func NewSearchAttributeTextByAlias(key string, alias string) *SearchAttributeText {
	return &SearchAttributeText{
		SearchAttribute: SearchAttribute{
			key:       key,
			alias:     alias,
			valueType: enumspb.INDEXED_VALUE_TYPE_TEXT,
		},
	}
}
func (s *SearchAttributeText) SetValue(value string) {
	s.value = value
}

func NewSearchAttributeKeywordListByIndex(key string, index int) *SearchAttributeKeywordList {
	return &SearchAttributeKeywordList{
		SearchAttribute: SearchAttribute{
			key:       key,
			alias:     ResolveFieldName(enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, index),
			valueType: enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
		},
	}
}

func NewSearchAttributeKeywordListByAlias(key string, alias string) *SearchAttributeKeywordList {
	return &SearchAttributeKeywordList{
		SearchAttribute: SearchAttribute{
			key:       key,
			alias:     alias,
			valueType: enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
		},
	}
}

func (s *SearchAttributeKeywordList) SetValue(value []string) {
	s.value = value
}
