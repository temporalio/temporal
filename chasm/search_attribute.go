package chasm

import (
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
)

// Exported CHASM search attribute field constants
var (
	SearchAttributeFieldBool01 = newSearchAttributeFieldBool(1)
	SearchAttributeFieldBool02 = newSearchAttributeFieldBool(2)

	SearchAttributeFieldDateTime01 = newSearchAttributeFieldDateTime(1)
	SearchAttributeFieldDateTime02 = newSearchAttributeFieldDateTime(2)

	SearchAttributeFieldInt01 = newSearchAttributeFieldInt(1)
	SearchAttributeFieldInt02 = newSearchAttributeFieldInt(2)

	SearchAttributeFieldDouble01 = newSearchAttributeFieldDouble(1)
	SearchAttributeFieldDouble02 = newSearchAttributeFieldDouble(2)

	SearchAttributeFieldKeyword01 = newSearchAttributeFieldKeyword(1)
	SearchAttributeFieldKeyword02 = newSearchAttributeFieldKeyword(2)
	SearchAttributeFieldKeyword03 = newSearchAttributeFieldKeyword(3)
	SearchAttributeFieldKeyword04 = newSearchAttributeFieldKeyword(4)

	SearchAttributeFieldKeywordList01 = newSearchAttributeFieldKeywordList(1)
	SearchAttributeFieldKeywordList02 = newSearchAttributeFieldKeywordList(2)
)

type (
	SearchAttributeDefinition interface {
		GetAlias() string
		GetField() string
		GetValueType() enumspb.IndexedValueType
		mustEmbedSearchAttribute()
	}

	SearchAttribute struct {
		// alias refers to the user defined name of the search attribute
		alias string
		// field refers to a fully formed schema field, which is either a Predefined or CHASM search attribute
		field     string
		valueType enumspb.IndexedValueType
	}

	SearchAttributeValue struct {
		field string
		value VisibilityValue
	}

	MemoValue struct {
		key   string
		value VisibilityValue
	}

	SearchAttributeFieldBool struct {
		field string
	}

	SearchAttributeFieldDateTime struct {
		field string
	}

	SearchAttributeFieldInt struct {
		field string
	}

	SearchAttributeFieldDouble struct {
		field string
	}

	SearchAttributeFieldKeyword struct {
		field string
	}

	SearchAttributeFieldKeywordList struct {
		field string
	}

	SearchAttributeBool struct {
		SearchAttribute
	}

	SearchAttributeDateTime struct {
		SearchAttribute
	}

	SearchAttributeInt struct {
		SearchAttribute
	}

	SearchAttributeDouble struct {
		SearchAttribute
	}

	SearchAttributeKeyword struct {
		SearchAttribute
	}

	SearchAttributeKeywordList struct {
		SearchAttribute
	}
)

func (s SearchAttributeValue) Field() string {
	return s.field
}

func (s SearchAttributeValue) Value() VisibilityValue {
	return s.value
}

func newSearchAttributeFieldBool(index int) SearchAttributeFieldBool {
	return SearchAttributeFieldBool{
		field: ResolveFieldName(enumspb.INDEXED_VALUE_TYPE_BOOL, index),
	}
}

func newSearchAttributeFieldDateTime(index int) SearchAttributeFieldDateTime {
	return SearchAttributeFieldDateTime{
		field: ResolveFieldName(enumspb.INDEXED_VALUE_TYPE_DATETIME, index),
	}
}

func newSearchAttributeFieldInt(index int) SearchAttributeFieldInt {
	return SearchAttributeFieldInt{
		field: ResolveFieldName(enumspb.INDEXED_VALUE_TYPE_INT, index),
	}
}

func newSearchAttributeFieldDouble(index int) SearchAttributeFieldDouble {
	return SearchAttributeFieldDouble{
		field: ResolveFieldName(enumspb.INDEXED_VALUE_TYPE_DOUBLE, index),
	}
}

func newSearchAttributeFieldKeyword(index int) SearchAttributeFieldKeyword {
	return SearchAttributeFieldKeyword{
		field: ResolveFieldName(enumspb.INDEXED_VALUE_TYPE_KEYWORD, index),
	}
}

func newSearchAttributeFieldKeywordList(index int) SearchAttributeFieldKeywordList {
	return SearchAttributeFieldKeywordList{
		field: ResolveFieldName(enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, index),
	}
}

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

func (s *SearchAttribute) mustEmbedSearchAttribute() {}

func NewSearchAttributeBool(alias string, field SearchAttributeFieldBool) *SearchAttributeBool {
	return &SearchAttributeBool{
		SearchAttribute: SearchAttribute{
			alias:     alias,
			field:     field.field,
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

func (s SearchAttributeBool) NewValue(value bool) SearchAttributeValue {
	return SearchAttributeValue{
		field: s.field,
		value: VisibilityValueBool(value),
	}
}

func NewSearchAttributeDateTime(alias string, field SearchAttributeFieldDateTime) *SearchAttributeDateTime {
	return &SearchAttributeDateTime{
		SearchAttribute: SearchAttribute{
			alias:     alias,
			field:     field.field,
			valueType: enumspb.INDEXED_VALUE_TYPE_DATETIME,
		},
	}
}

func NewSearchAttributeInt(alias string, field SearchAttributeFieldInt) *SearchAttributeInt {
	return &SearchAttributeInt{
		SearchAttribute: SearchAttribute{
			alias:     alias,
			field:     field.field,
			valueType: enumspb.INDEXED_VALUE_TYPE_INT,
		},
	}
}

func NewSearchAttributeIntByField(alias string, field string) *SearchAttributeInt {
	return &SearchAttributeInt{
		SearchAttribute: SearchAttribute{
			alias:     alias,
			field:     field,
			valueType: enumspb.INDEXED_VALUE_TYPE_INT,
		},
	}
}

func (s SearchAttributeInt) NewValue(value int64) SearchAttributeValue {
	return SearchAttributeValue{
		field: s.field,
		value: VisibilityValueInt64(value),
	}
}

func NewSearchAttributeDateTimeByField(alias string, field string) *SearchAttributeDateTime {
	return &SearchAttributeDateTime{
		SearchAttribute: SearchAttribute{
			alias:     alias,
			field:     field,
			valueType: enumspb.INDEXED_VALUE_TYPE_DATETIME,
		},
	}
}

func (s SearchAttributeDateTime) NewValue(value time.Time) SearchAttributeValue {
	return SearchAttributeValue{
		field: s.field,
		value: VisibilityValueTime(value),
	}
}

func NewSearchAttributeDouble(alias string, field SearchAttributeFieldDouble) *SearchAttributeDouble {
	return &SearchAttributeDouble{
		SearchAttribute: SearchAttribute{
			alias:     alias,
			field:     field.field,
			valueType: enumspb.INDEXED_VALUE_TYPE_DOUBLE,
		},
	}
}

func NewSearchAttributeDoubleByField(alias string, field string) *SearchAttributeDouble {
	return &SearchAttributeDouble{
		SearchAttribute: SearchAttribute{
			alias:     alias,
			field:     field,
			valueType: enumspb.INDEXED_VALUE_TYPE_DOUBLE,
		},
	}
}

func (s SearchAttributeDouble) NewValue(value float64) SearchAttributeValue {
	return SearchAttributeValue{
		field: s.field,
		value: VisibilityValueFloat64(value),
	}
}

func NewSearchAttributeKeyword(alias string, field SearchAttributeFieldKeyword) *SearchAttributeKeyword {
	return &SearchAttributeKeyword{
		SearchAttribute: SearchAttribute{
			alias:     alias,
			field:     field.field,
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

func (s SearchAttributeKeyword) NewValue(value string) SearchAttributeValue {
	return SearchAttributeValue{
		field: s.field,
		value: VisibilityValueString(value),
	}
}

func NewSearchAttributeKeywordList(alias string, field SearchAttributeFieldKeywordList) *SearchAttributeKeywordList {
	return &SearchAttributeKeywordList{
		SearchAttribute: SearchAttribute{
			alias:     alias,
			field:     field.field,
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

func (s SearchAttributeKeywordList) NewValue(value []string) SearchAttributeValue {
	return SearchAttributeValue{
		field: s.field,
		value: VisibilityValueStringSlice(value),
	}
}
