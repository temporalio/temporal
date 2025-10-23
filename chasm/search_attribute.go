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
	SearchAttribute interface {
		getAlias() string
		getField() string
		getValueType() enumspb.IndexedValueType
		mustEmbedSearchAttributeDefinition()
	}

	searchAttributeDefinition struct {
		// alias refers to the user defined name of the search attribute
		alias string
		// field refers to a fully formed schema field, which is either a Predefined or CHASM search attribute
		field     string
		valueType enumspb.IndexedValueType
	}

	SearchAttributeKeyValue struct {
		Field string
		Value VisibilityValue
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
		searchAttributeDefinition
	}

	SearchAttributeDateTime struct {
		searchAttributeDefinition
	}

	SearchAttributeInt struct {
		searchAttributeDefinition
	}

	SearchAttributeDouble struct {
		searchAttributeDefinition
	}

	SearchAttributeKeyword struct {
		searchAttributeDefinition
	}

	SearchAttributeKeywordList struct {
		searchAttributeDefinition
	}
)

func newSearchAttributeFieldBool(index int) SearchAttributeFieldBool {
	return SearchAttributeFieldBool{
		field: resolveFieldName(enumspb.INDEXED_VALUE_TYPE_BOOL, index),
	}
}

func newSearchAttributeFieldDateTime(index int) SearchAttributeFieldDateTime {
	return SearchAttributeFieldDateTime{
		field: resolveFieldName(enumspb.INDEXED_VALUE_TYPE_DATETIME, index),
	}
}

func newSearchAttributeFieldInt(index int) SearchAttributeFieldInt {
	return SearchAttributeFieldInt{
		field: resolveFieldName(enumspb.INDEXED_VALUE_TYPE_INT, index),
	}
}

func newSearchAttributeFieldDouble(index int) SearchAttributeFieldDouble {
	return SearchAttributeFieldDouble{
		field: resolveFieldName(enumspb.INDEXED_VALUE_TYPE_DOUBLE, index),
	}
}

func newSearchAttributeFieldKeyword(index int) SearchAttributeFieldKeyword {
	return SearchAttributeFieldKeyword{
		field: resolveFieldName(enumspb.INDEXED_VALUE_TYPE_KEYWORD, index),
	}
}

func newSearchAttributeFieldKeywordList(index int) SearchAttributeFieldKeywordList {
	return SearchAttributeFieldKeywordList{
		field: resolveFieldName(enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, index),
	}
}

func resolveFieldName(valueType enumspb.IndexedValueType, index int) string {
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
func (s *searchAttributeDefinition) getAlias() string {
	return s.alias
}

// GetField returns the search attribute field name.
func (s *searchAttributeDefinition) getField() string {
	return s.field
}

// GetValueType returns the indexed value type.
func (s *searchAttributeDefinition) getValueType() enumspb.IndexedValueType {
	return s.valueType
}

func (s *searchAttributeDefinition) mustEmbedSearchAttributeDefinition() {}

func NewSearchAttributeBool(alias string, boolField SearchAttributeFieldBool) *SearchAttributeBool {
	return &SearchAttributeBool{
		searchAttributeDefinition: searchAttributeDefinition{
			alias:     alias,
			field:     boolField.field,
			valueType: enumspb.INDEXED_VALUE_TYPE_BOOL,
		},
	}
}

func NewSearchAttributeBoolByField(field string) *SearchAttributeBool {
	return &SearchAttributeBool{
		searchAttributeDefinition: searchAttributeDefinition{
			alias:     field,
			field:     field,
			valueType: enumspb.INDEXED_VALUE_TYPE_BOOL,
		},
	}
}

func (s SearchAttributeBool) Value(value bool) SearchAttributeKeyValue {
	return SearchAttributeKeyValue{
		Field: s.field,
		Value: VisibilityValueBool(value),
	}
}

func NewSearchAttributeDateTime(alias string, datetimeField SearchAttributeFieldDateTime) *SearchAttributeDateTime {
	return &SearchAttributeDateTime{
		searchAttributeDefinition: searchAttributeDefinition{
			alias:     alias,
			field:     datetimeField.field,
			valueType: enumspb.INDEXED_VALUE_TYPE_DATETIME,
		},
	}
}

func NewSearchAttributeDateTimeByField(field string) *SearchAttributeDateTime {
	return &SearchAttributeDateTime{
		searchAttributeDefinition: searchAttributeDefinition{
			alias:     field,
			field:     field,
			valueType: enumspb.INDEXED_VALUE_TYPE_DATETIME,
		},
	}
}

func (s SearchAttributeDateTime) Value(value time.Time) SearchAttributeKeyValue {
	return SearchAttributeKeyValue{
		Field: s.field,
		Value: VisibilityValueTime(value),
	}
}

func NewSearchAttributeInt(alias string, intField SearchAttributeFieldInt) *SearchAttributeInt {
	return &SearchAttributeInt{
		searchAttributeDefinition: searchAttributeDefinition{
			alias:     alias,
			field:     intField.field,
			valueType: enumspb.INDEXED_VALUE_TYPE_INT,
		},
	}
}

func NewSearchAttributeIntByField(field string) *SearchAttributeInt {
	return &SearchAttributeInt{
		searchAttributeDefinition: searchAttributeDefinition{
			alias:     field,
			field:     field,
			valueType: enumspb.INDEXED_VALUE_TYPE_INT,
		},
	}
}

func (s SearchAttributeInt) Value(value int64) SearchAttributeKeyValue {
	return SearchAttributeKeyValue{
		Field: s.field,
		Value: VisibilityValueInt64(value),
	}
}

func NewSearchAttributeDouble(alias string, doubleField SearchAttributeFieldDouble) *SearchAttributeDouble {
	return &SearchAttributeDouble{
		searchAttributeDefinition: searchAttributeDefinition{
			alias:     alias,
			field:     doubleField.field,
			valueType: enumspb.INDEXED_VALUE_TYPE_DOUBLE,
		},
	}
}

func NewSearchAttributeDoubleByField(field string) *SearchAttributeDouble {
	return &SearchAttributeDouble{
		searchAttributeDefinition: searchAttributeDefinition{
			alias:     field,
			field:     field,
			valueType: enumspb.INDEXED_VALUE_TYPE_DOUBLE,
		},
	}
}

func (s SearchAttributeDouble) Value(value float64) SearchAttributeKeyValue {
	return SearchAttributeKeyValue{
		Field: s.field,
		Value: VisibilityValueFloat64(value),
	}
}

func NewSearchAttributeKeyword(alias string, keywordField SearchAttributeFieldKeyword) *SearchAttributeKeyword {
	return &SearchAttributeKeyword{
		searchAttributeDefinition: searchAttributeDefinition{
			alias:     alias,
			field:     keywordField.field,
			valueType: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	}
}

func NewSearchAttributeKeywordByField(field string) *SearchAttributeKeyword {
	return &SearchAttributeKeyword{
		searchAttributeDefinition: searchAttributeDefinition{
			alias:     field,
			field:     field,
			valueType: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	}
}

func (s SearchAttributeKeyword) Value(value string) SearchAttributeKeyValue {
	return SearchAttributeKeyValue{
		Field: s.field,
		Value: VisibilityValueString(value),
	}
}

func NewSearchAttributeKeywordList(alias string, keywordListField SearchAttributeFieldKeywordList) *SearchAttributeKeywordList {
	return &SearchAttributeKeywordList{
		searchAttributeDefinition: searchAttributeDefinition{
			alias:     alias,
			field:     keywordListField.field,
			valueType: enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
		},
	}
}

func NewSearchAttributeKeywordListByField(field string) *SearchAttributeKeywordList {
	return &SearchAttributeKeywordList{
		searchAttributeDefinition: searchAttributeDefinition{
			alias:     field,
			field:     field,
			valueType: enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
		},
	}
}
func (s SearchAttributeKeywordList) Value(value []string) SearchAttributeKeyValue {
	return SearchAttributeKeyValue{
		Field: s.field,
		Value: VisibilityValueStringSlice(value),
	}
}
