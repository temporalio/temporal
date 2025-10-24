package chasm

import (
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/searchattribute"
)

// CHASM Search Attribute User Guide:
//
// Below contains exported CHASM search attribute field constants. These predefined fields correspond to the exact column name in Visibility storage.
// For each root component, search attributes can be mapped from a user defined alias to these fields.
//
// To define a CHASM search attribute, create this as a package/global scoped variable. Below is an example:
// var testComponentCompletedSearchAttribute = NewSearchAttributeBool("Completed", SearchAttributeFieldBool01)
// var testComponentFailedSearchAttribute = NewSearchAttributeBool("Failed", SearchAttributeFieldBool02)
// var testComponentStartTimeSearchAttribute = NewSearchAttributeTime("StartTime", SearchAttributeFieldDateTime01)
//
// Each CHASM search attribute field is associated with a specific indexed value type. The Value() method of a search attribute
// specifies the supported value to set at compile time. eg. DateTime values must be set with a time.Time typed value.
//
// Each root component can ONLY use a predefined search attribute field ONCE. Developers should NOT reassign aliases to different fields.
// Reassiging fields to different aliases is a breaking change during visibility queries.
//
// To register these search attributes with the CHASM Registry, use the WithSearchAttributes() option when creating the component in the library.
// eg.
// NewRegistrableComponent[T]("testcomponent", WithSearchAttributes(testComponentCompletedSearchAttribute, testComponentStartTimeSearchAttribute))
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

var (
	_ SearchAttribute = (*searchAttributeDefinition)(nil)
	_ SearchAttribute = (*SearchAttributeBool)(nil)
	_ SearchAttribute = (*SearchAttributeDateTime)(nil)
	_ SearchAttribute = (*SearchAttributeInt)(nil)
	_ SearchAttribute = (*SearchAttributeDouble)(nil)
	_ SearchAttribute = (*SearchAttributeKeyword)(nil)
	_ SearchAttribute = (*SearchAttributeKeywordList)(nil)
)

type (
	SearchAttribute interface {
		definition() searchAttributeDefinition
	}

	searchAttributeDefinition struct {
		// alias refers to the user defined name of the search attribute
		Alias string
		// field refers to a fully formed schema field, which is either a Predefined or CHASM search attribute
		Field     string
		ValueType enumspb.IndexedValueType
	}

	SearchAttributeKeyValue struct {
		Alias string
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
	return fmt.Sprintf("%s%s%02d", searchattribute.ReservedPrefix, valueType.String(), index)
}

func (s searchAttributeDefinition) definition() searchAttributeDefinition {
	return s
}

func NewSearchAttributeBool(alias string, boolField SearchAttributeFieldBool) SearchAttributeBool {
	return SearchAttributeBool{
		searchAttributeDefinition: searchAttributeDefinition{
			Alias:     alias,
			Field:     boolField.field,
			ValueType: enumspb.INDEXED_VALUE_TYPE_BOOL,
		},
	}
}

func newSearchAttributeBoolByField(field string) SearchAttributeBool {
	return SearchAttributeBool{
		searchAttributeDefinition: searchAttributeDefinition{
			Alias:     field,
			Field:     field,
			ValueType: enumspb.INDEXED_VALUE_TYPE_BOOL,
		},
	}
}

func (s SearchAttributeBool) Value(value bool) SearchAttributeKeyValue {
	return SearchAttributeKeyValue{
		Alias: s.Alias,
		Field: s.Field,
		Value: VisibilityValueBool(value),
	}
}

func NewSearchAttributeDateTime(alias string, datetimeField SearchAttributeFieldDateTime) SearchAttributeDateTime {
	return SearchAttributeDateTime{
		searchAttributeDefinition: searchAttributeDefinition{
			Alias:     alias,
			Field:     datetimeField.field,
			ValueType: enumspb.INDEXED_VALUE_TYPE_DATETIME,
		},
	}
}

func newSearchAttributeDateTimeByField(field string) SearchAttributeDateTime {
	return SearchAttributeDateTime{
		searchAttributeDefinition: searchAttributeDefinition{
			Alias:     field,
			Field:     field,
			ValueType: enumspb.INDEXED_VALUE_TYPE_DATETIME,
		},
	}
}

func (s SearchAttributeDateTime) Value(value time.Time) SearchAttributeKeyValue {
	return SearchAttributeKeyValue{
		Alias: s.Alias,
		Field: s.Field,
		Value: VisibilityValueTime(value),
	}
}

func NewSearchAttributeInt(alias string, intField SearchAttributeFieldInt) SearchAttributeInt {
	return SearchAttributeInt{
		searchAttributeDefinition: searchAttributeDefinition{
			Alias:     alias,
			Field:     intField.field,
			ValueType: enumspb.INDEXED_VALUE_TYPE_INT,
		},
	}
}

func newSearchAttributeIntByField(field string) SearchAttributeInt {
	return SearchAttributeInt{
		searchAttributeDefinition: searchAttributeDefinition{
			Alias:     field,
			Field:     field,
			ValueType: enumspb.INDEXED_VALUE_TYPE_INT,
		},
	}
}

func (s SearchAttributeInt) Value(value int64) SearchAttributeKeyValue {
	return SearchAttributeKeyValue{
		Alias: s.Alias,
		Field: s.Field,
		Value: VisibilityValueInt64(value),
	}
}

func NewSearchAttributeDouble(alias string, doubleField SearchAttributeFieldDouble) SearchAttributeDouble {
	return SearchAttributeDouble{
		searchAttributeDefinition: searchAttributeDefinition{
			Alias:     alias,
			Field:     doubleField.field,
			ValueType: enumspb.INDEXED_VALUE_TYPE_DOUBLE,
		},
	}
}

func newSearchAttributeDoubleByField(field string) SearchAttributeDouble {
	return SearchAttributeDouble{
		searchAttributeDefinition: searchAttributeDefinition{
			Alias:     field,
			Field:     field,
			ValueType: enumspb.INDEXED_VALUE_TYPE_DOUBLE,
		},
	}
}

func (s SearchAttributeDouble) Value(value float64) SearchAttributeKeyValue {
	return SearchAttributeKeyValue{
		Alias: s.Alias,
		Field: s.Field,
		Value: VisibilityValueFloat64(value),
	}
}

func NewSearchAttributeKeyword(alias string, keywordField SearchAttributeFieldKeyword) SearchAttributeKeyword {
	return SearchAttributeKeyword{
		searchAttributeDefinition: searchAttributeDefinition{
			Alias:     alias,
			Field:     keywordField.field,
			ValueType: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	}
}

func newSearchAttributeKeywordByField(field string) SearchAttributeKeyword {
	return SearchAttributeKeyword{
		searchAttributeDefinition: searchAttributeDefinition{
			Alias:     field,
			Field:     field,
			ValueType: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	}
}

func (s SearchAttributeKeyword) Value(value string) SearchAttributeKeyValue {
	return SearchAttributeKeyValue{
		Alias: s.Alias,
		Field: s.Field,
		Value: VisibilityValueString(value),
	}
}

func NewSearchAttributeKeywordList(alias string, keywordListField SearchAttributeFieldKeywordList) SearchAttributeKeywordList {
	return SearchAttributeKeywordList{
		searchAttributeDefinition: searchAttributeDefinition{
			Alias:     alias,
			Field:     keywordListField.field,
			ValueType: enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
		},
	}
}

func newSearchAttributeKeywordListByField(field string) SearchAttributeKeywordList {
	return SearchAttributeKeywordList{
		searchAttributeDefinition: searchAttributeDefinition{
			Alias:     field,
			Field:     field,
			ValueType: enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
		},
	}
}
func (s SearchAttributeKeywordList) Value(value []string) SearchAttributeKeyValue {
	return SearchAttributeKeyValue{
		Alias: s.Alias,
		Field: s.Field,
		Value: VisibilityValueStringSlice(value),
	}
}
