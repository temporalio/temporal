package chasm

import (
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
)

// search attribute preallocated field constants
var (
	SearchAttributeFieldBool01        = newSearchAttributeFieldBool(1)
	SearchAttributeFieldBool02        = newSearchAttributeFieldBool(2)
	SearchAttributeFieldDatetime01    = newSearchAttributeFieldDatetime(1)
	SearchAttributeFieldDatetime02    = newSearchAttributeFieldDatetime(2)
	SearchAttributeFieldDouble01      = newSearchAttributeFieldDouble(1)
	SearchAttributeFieldDouble02      = newSearchAttributeFieldDouble(2)
	SearchAttributeFieldInt01         = newSearchAttributeFieldInt(1)
	SearchAttributeFieldInt02         = newSearchAttributeFieldInt(2)
	SearchAttributeFieldKeyword01     = newSearchAttributeFieldKeyword(1)
	SearchAttributeFieldKeyword02     = newSearchAttributeFieldKeyword(2)
	SearchAttributeFieldKeyword03     = newSearchAttributeFieldKeyword(3)
	SearchAttributeFieldKeyword04     = newSearchAttributeFieldKeyword(4)
	SearchAttributeFieldText01        = newSearchAttributeFieldText(1)
	SearchAttributeFieldText02        = newSearchAttributeFieldText(2)
	SearchAttributeFieldKeywordList01 = newSearchAttributeFieldKeywordList(1)
	SearchAttributeFieldKeywordList02 = newSearchAttributeFieldKeywordList(2)
)

type (
	SearchAttributeField struct {
		idx       int
		valueType enumspb.IndexedValueType
	}

	SearchAttributeUpdate func(map[string]VisibilityValue)

	SearchAttribute struct {
		alias     string
		valueType enumspb.IndexedValueType
		value     VisibilityValue
		field     SearchAttributeField
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

	SearchAttributeInt64 struct {
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

func (s SearchAttributeField) GetFieldName() string {
	// Columns are named like TemporalBool01, TemporalDatetime01, TemporalDouble01, TemporalInt01.
	suffix := fmt.Sprintf("%02d", s.idx)
	switch s.valueType {
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

func newSearchAttributeFieldBool(id int) SearchAttributeField {
	return SearchAttributeField{
		idx:       id,
		valueType: enumspb.INDEXED_VALUE_TYPE_BOOL,
	}
}

func newSearchAttributeFieldDatetime(id int) SearchAttributeField {
	return SearchAttributeField{
		idx:       id,
		valueType: enumspb.INDEXED_VALUE_TYPE_DATETIME,
	}
}

func newSearchAttributeFieldDouble(id int) SearchAttributeField {
	return SearchAttributeField{
		idx:       id,
		valueType: enumspb.INDEXED_VALUE_TYPE_DOUBLE,
	}
}

func newSearchAttributeFieldInt(id int) SearchAttributeField {
	return SearchAttributeField{
		idx:       id,
		valueType: enumspb.INDEXED_VALUE_TYPE_INT,
	}
}

func newSearchAttributeFieldKeyword(id int) SearchAttributeField {
	return SearchAttributeField{
		idx:       id,
		valueType: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	}
}

func newSearchAttributeFieldText(id int) SearchAttributeField {
	return SearchAttributeField{
		idx:       id,
		valueType: enumspb.INDEXED_VALUE_TYPE_TEXT,
	}
}

func newSearchAttributeFieldKeywordList(id int) SearchAttributeField {
	return SearchAttributeField{
		idx:       id,
		valueType: enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
	}
}

// Getter methods for SearchAttribute
func (s *SearchAttribute) GetAlias() string {
	return s.alias
}

func (s *SearchAttribute) GetValueType() enumspb.IndexedValueType {
	return s.valueType
}

func (s *SearchAttribute) GetValue() VisibilityValue {
	return s.value
}

func (s *SearchAttribute) GetField() SearchAttributeField {
	return s.field
}

func (s *SearchAttribute) GetFieldName() string {
	return s.field.GetFieldName()
}

func NewSearchAttributeBool(alias string, field SearchAttributeField) *SearchAttributeBool {
	return &SearchAttributeBool{
		SearchAttribute: SearchAttribute{
			alias:     alias,
			valueType: enumspb.INDEXED_VALUE_TYPE_BOOL,
			field:     field,
		},
	}
}

func (s *SearchAttributeBool) SetValue(value bool) {
	s.value = VisibilityValueBool(value)
}

func NewSearchAttributeTime(alias string, field SearchAttributeField) *SearchAttributeTime {
	return &SearchAttributeTime{
		SearchAttribute: SearchAttribute{
			alias:     alias,
			valueType: enumspb.INDEXED_VALUE_TYPE_DATETIME,
			field:     field,
		},
	}
}

func (s *SearchAttributeTime) SetValue(value time.Time) {
	s.value = VisibilityValueTime(value)
}

func NewSearchAttributeFloat64(alias string, field SearchAttributeField) *SearchAttributeFloat64 {
	return &SearchAttributeFloat64{
		SearchAttribute: SearchAttribute{
			alias:     alias,
			valueType: enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			field:     field,
		},
	}
}

func (s *SearchAttributeFloat64) SetValue(value float64) {
	s.value = VisibilityValueFloat64(value)
}

func NewSearchAttributeKeyword(alias string, field SearchAttributeField) *SearchAttributeKeyword {
	return &SearchAttributeKeyword{
		SearchAttribute: SearchAttribute{
			alias:     alias,
			valueType: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			field:     field,
		},
	}
}

func (s *SearchAttributeKeyword) SetValue(value string) {
	s.value = VisibilityValueString(value)
}

func NewSearchAttributeText(alias string, field SearchAttributeField) *SearchAttributeText {
	return &SearchAttributeText{
		SearchAttribute: SearchAttribute{
			alias:     alias,
			valueType: enumspb.INDEXED_VALUE_TYPE_TEXT,
			field:     field,
		},
	}
}

func (s *SearchAttributeText) SetValue(value string) {
	s.value = VisibilityValueString(value)
}

func NewSearchAttributeKeywordList(alias string, field SearchAttributeField) *SearchAttributeKeywordList {
	return &SearchAttributeKeywordList{
		SearchAttribute: SearchAttribute{
			alias:     alias,
			valueType: enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
			field:     field,
		},
	}
}

func (s *SearchAttributeKeywordList) SetValue(value []string) {
	s.value = VisibilityValueStringSlice(value)
}
