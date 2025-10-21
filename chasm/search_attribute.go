package chasm

import (
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
)

type (
	SearchAttribute struct {
		// alias refers to the user defined name of the search attribute
		alias string
		// field refers to a fully formed schema field, which is either a Predefined or CHASM search attribute
		field     string
		valueType enumspb.IndexedValueType
	}

	SearchAttributeKeyValue struct {
		alias string
		field string
		value VisibilityValue
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

// GetAlias returns the search attribute alias.
func (s *SearchAttribute) GetAlias() string {
	return s.alias
}

// GetField returns the search attribute field name.
func (s *SearchAttribute) GetField() string {
	return s.field
}

// GetValueType returns the indexed value type.
func (s *SearchAttribute) GetValueType() enumspb.IndexedValueType {
	return s.valueType
}

func NewSearchAttributeBoolByIndex(alias string, index int) *SearchAttributeBool {
	return &SearchAttributeBool{
		SearchAttribute: SearchAttribute{
			alias:     alias,
			field:     ResolveFieldName(enumspb.INDEXED_VALUE_TYPE_BOOL, index),
			valueType: enumspb.INDEXED_VALUE_TYPE_BOOL,
		},
	}
}

func NewSearchAttributeBoolByField(alias string, field string) *SearchAttributeBool {
	return &SearchAttributeBool{
		SearchAttribute: SearchAttribute{
			alias:     alias,
			field:     field,
			valueType: enumspb.INDEXED_VALUE_TYPE_BOOL,
		},
	}
}

func (s SearchAttributeBool) SetValue(value bool) SearchAttributeKeyValue {
	return SearchAttributeKeyValue{
		alias: s.alias,
		field: s.field,
		value: VisibilityValueBool(value),
	}
}

func NewSearchAttributeTimeByIndex(alias string, index int) *SearchAttributeTime {
	return &SearchAttributeTime{
		SearchAttribute: SearchAttribute{
			alias:     alias,
			field:     ResolveFieldName(enumspb.INDEXED_VALUE_TYPE_DATETIME, index),
			valueType: enumspb.INDEXED_VALUE_TYPE_DATETIME,
		},
	}
}

func NewSearchAttributeTimeByField(alias string, field string) *SearchAttributeTime {
	return &SearchAttributeTime{
		SearchAttribute: SearchAttribute{
			alias:     alias,
			field:     field,
			valueType: enumspb.INDEXED_VALUE_TYPE_DATETIME,
		},
	}
}

func (s SearchAttributeTime) SetValue(value time.Time) SearchAttributeKeyValue {
	return SearchAttributeKeyValue{
		alias: s.alias,
		field: s.field,
		value: VisibilityValueTime(value),
	}
}

func NewSearchAttributeFloat64ByIndex(alias string, index int) *SearchAttributeFloat64 {
	return &SearchAttributeFloat64{
		SearchAttribute: SearchAttribute{
			alias:     alias,
			field:     ResolveFieldName(enumspb.INDEXED_VALUE_TYPE_DOUBLE, index),
			valueType: enumspb.INDEXED_VALUE_TYPE_DOUBLE,
		},
	}
}

func NewSearchAttributeFloat64ByField(alias string, field string) *SearchAttributeFloat64 {
	return &SearchAttributeFloat64{
		SearchAttribute: SearchAttribute{
			alias:     alias,
			field:     field,
			valueType: enumspb.INDEXED_VALUE_TYPE_DOUBLE,
		},
	}
}

func (s SearchAttributeFloat64) SetValue(value float64) SearchAttributeKeyValue {
	return SearchAttributeKeyValue{
		alias: s.alias,
		field: s.field,
		value: VisibilityValueFloat64(value),
	}
}

func NewSearchAttributeKeywordByIndex(alias string, index int) *SearchAttributeKeyword {
	return &SearchAttributeKeyword{
		SearchAttribute: SearchAttribute{
			alias:     alias,
			field:     ResolveFieldName(enumspb.INDEXED_VALUE_TYPE_KEYWORD, index),
			valueType: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	}
}

func NewSearchAttributeKeywordByField(alias string, field string) *SearchAttributeKeyword {
	return &SearchAttributeKeyword{
		SearchAttribute: SearchAttribute{
			alias:     alias,
			field:     field,
			valueType: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	}
}

func (s SearchAttributeKeyword) SetValue(value string) SearchAttributeKeyValue {
	return SearchAttributeKeyValue{
		alias: s.alias,
		field: s.field,
		value: VisibilityValueString(value),
	}
}

func NewSearchAttributeTextByIndex(alias string, index int) *SearchAttributeText {
	return &SearchAttributeText{
		SearchAttribute: SearchAttribute{
			alias:     alias,
			field:     ResolveFieldName(enumspb.INDEXED_VALUE_TYPE_TEXT, index),
			valueType: enumspb.INDEXED_VALUE_TYPE_TEXT,
		},
	}
}

func NewSearchAttributeTextByField(alias string, field string) *SearchAttributeText {
	return &SearchAttributeText{
		SearchAttribute: SearchAttribute{
			alias:     alias,
			field:     field,
			valueType: enumspb.INDEXED_VALUE_TYPE_TEXT,
		},
	}
}

func (s SearchAttributeText) SetValue(value string) SearchAttributeKeyValue {
	return SearchAttributeKeyValue{
		alias: s.alias,
		field: s.field,
		value: VisibilityValueString(value),
	}
}

func NewSearchAttributeKeywordListByIndex(alias string, index int) *SearchAttributeKeywordList {
	return &SearchAttributeKeywordList{
		SearchAttribute: SearchAttribute{
			alias:     alias,
			field:     ResolveFieldName(enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, index),
			valueType: enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
		},
	}
}

func NewSearchAttributeKeywordListByField(alias string, field string) *SearchAttributeKeywordList {
	return &SearchAttributeKeywordList{
		SearchAttribute: SearchAttribute{
			alias:     alias,
			field:     field,
			valueType: enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
		},
	}
}

func (s SearchAttributeKeywordList) SetValue(value []string) SearchAttributeKeyValue {
	return SearchAttributeKeyValue{
		alias: s.alias,
		field: s.field,
		value: VisibilityValueStringSlice(value),
	}
}
