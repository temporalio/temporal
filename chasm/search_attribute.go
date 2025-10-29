package chasm

import (
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/searchattribute"
)

// CHASM Search Attribute User Guide:
//
// This contains CHASM search attribute field constants. These predefined fields correspond to the exact column name in Visibility storage.
// For each root component, search attributes can be mapped from a user defined alias to these fields.
//
// To define a CHASM search attribute, create this as a package/global scoped variable. Below is an example:
// var testComponentCompletedSearchAttribute = NewSearchAttributeBool("Completed", SearchAttributeFieldBool01)
// var testComponentFailedSearchAttribute = NewSearchAttributeBool("Failed", SearchAttributeFieldBool02)
// var testComponentStartTimeSearchAttribute = NewSearchAttributeTime("StartTime", SearchAttributeFieldDateTime01)
//
// Each CHASM search attribute field is associated with a specific indexed value type. The Value() method of a search attribute
// specifies the supported value type to set at compile time. eg. DateTime values must be set with a time.Time typed value.
//
// Each root component can ONLY use a predefined search attribute field ONCE. Developers should NOT reassign aliases to different fields.
// Reassiging aliases to different fields will result in incorrect visibility query results.
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

	SearchAttributeTemporalScheduledByID = newSearchAttributeKeywordByField(searchattribute.TemporalScheduledById)
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
	// SearchAttribute is a shared interface for all search attribute types. Each type must embed searchAttributeDefinition.
	SearchAttribute interface {
		definition() searchAttributeDefinition
	}

	searchAttributeDefinition struct {
		alias     string
		field     string
		valueType enumspb.IndexedValueType
	}

	// SearchAttributeKeyValue is a key value pair of a search attribute.
	// Represents the current value of a search attribute in a CHASM Component during a transaction.
	SearchAttributeKeyValue struct {
		// Alias refers to the user defined name of the search attribute
		Alias string
		// Field refers to a fully formed schema field, which is a Predefined CHASM search attribute
		Field string
		// Value refers to the current value of the search attribute. Must support encoding to a Payload.
		Value VisibilityValue
	}
)

// SearchAttributeFieldBool is a search attribute field for a boolean value.
type SearchAttributeFieldBool struct {
	field string
}

func newSearchAttributeFieldBool(index int) SearchAttributeFieldBool {
	return SearchAttributeFieldBool{
		field: resolveFieldName(enumspb.INDEXED_VALUE_TYPE_BOOL, index),
	}
}

// SearchAttributeFieldDateTime is a search attribute field for a datetime value.
type SearchAttributeFieldDateTime struct {
	field string
}

func newSearchAttributeFieldDateTime(index int) SearchAttributeFieldDateTime {
	return SearchAttributeFieldDateTime{
		field: resolveFieldName(enumspb.INDEXED_VALUE_TYPE_DATETIME, index),
	}
}

// SearchAttributeFieldInt is a search attribute field for an integer value.
type SearchAttributeFieldInt struct {
	field string
}

func newSearchAttributeFieldInt(index int) SearchAttributeFieldInt {
	return SearchAttributeFieldInt{
		field: resolveFieldName(enumspb.INDEXED_VALUE_TYPE_INT, index),
	}
}

// SearchAttributeFieldDouble is a search attribute field for a double value.
type SearchAttributeFieldDouble struct {
	field string
}

func newSearchAttributeFieldDouble(index int) SearchAttributeFieldDouble {
	return SearchAttributeFieldDouble{
		field: resolveFieldName(enumspb.INDEXED_VALUE_TYPE_DOUBLE, index),
	}
}

// SearchAttributeFieldKeyword is a search attribute field for a keyword value.
type SearchAttributeFieldKeyword struct {
	field string
}

func newSearchAttributeFieldKeyword(index int) SearchAttributeFieldKeyword {
	return SearchAttributeFieldKeyword{
		field: resolveFieldName(enumspb.INDEXED_VALUE_TYPE_KEYWORD, index),
	}
}

// SearchAttributeFieldKeywordList is a search attribute field for a keyword list value.
type SearchAttributeFieldKeywordList struct {
	field string
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

// SearchAttributeBool is a search attribute for a boolean value.
type SearchAttributeBool struct {
	searchAttributeDefinition
}

func NewSearchAttributeBool(alias string, boolField SearchAttributeFieldBool) SearchAttributeBool {
	return SearchAttributeBool{
		searchAttributeDefinition: searchAttributeDefinition{
			alias:     alias,
			field:     boolField.field,
			valueType: enumspb.INDEXED_VALUE_TYPE_BOOL,
		},
	}
}

func newSearchAttributeBoolByField(field string) SearchAttributeBool {
	return SearchAttributeBool{
		searchAttributeDefinition: searchAttributeDefinition{
			alias:     field,
			field:     field,
			valueType: enumspb.INDEXED_VALUE_TYPE_BOOL,
		},
	}
}

func (s SearchAttributeBool) Value(value bool) SearchAttributeKeyValue {
	return SearchAttributeKeyValue{
		Alias: s.alias,
		Field: s.field,
		Value: VisibilityValueBool(value),
	}
}

// SearchAttributeDateTime is a search attribute for a datetime value.
type SearchAttributeDateTime struct {
	searchAttributeDefinition
}

func NewSearchAttributeDateTime(alias string, datetimeField SearchAttributeFieldDateTime) SearchAttributeDateTime {
	return SearchAttributeDateTime{
		searchAttributeDefinition: searchAttributeDefinition{
			alias:     alias,
			field:     datetimeField.field,
			valueType: enumspb.INDEXED_VALUE_TYPE_DATETIME,
		},
	}
}

func newSearchAttributeDateTimeByField(field string) SearchAttributeDateTime {
	return SearchAttributeDateTime{
		searchAttributeDefinition: searchAttributeDefinition{
			alias:     field,
			field:     field,
			valueType: enumspb.INDEXED_VALUE_TYPE_DATETIME,
		},
	}
}

func (s SearchAttributeDateTime) Value(value time.Time) SearchAttributeKeyValue {
	return SearchAttributeKeyValue{
		Alias: s.alias,
		Field: s.field,
		Value: VisibilityValueTime(value),
	}
}

// SearchAttributeInt is a search attribute for an integer value.
type SearchAttributeInt struct {
	searchAttributeDefinition
}

func NewSearchAttributeInt(alias string, intField SearchAttributeFieldInt) SearchAttributeInt {
	return SearchAttributeInt{
		searchAttributeDefinition: searchAttributeDefinition{
			alias:     alias,
			field:     intField.field,
			valueType: enumspb.INDEXED_VALUE_TYPE_INT,
		},
	}
}

func newSearchAttributeIntByField(field string) SearchAttributeInt {
	return SearchAttributeInt{
		searchAttributeDefinition: searchAttributeDefinition{
			alias:     field,
			field:     field,
			valueType: enumspb.INDEXED_VALUE_TYPE_INT,
		},
	}
}

func (s SearchAttributeInt) Value(value int64) SearchAttributeKeyValue {
	return SearchAttributeKeyValue{
		Alias: s.alias,
		Field: s.field,
		Value: VisibilityValueInt64(value),
	}
}

// SearchAttributeDouble is a search attribute for a double value.
type SearchAttributeDouble struct {
	searchAttributeDefinition
}

func NewSearchAttributeDouble(alias string, doubleField SearchAttributeFieldDouble) SearchAttributeDouble {
	return SearchAttributeDouble{
		searchAttributeDefinition: searchAttributeDefinition{
			alias:     alias,
			field:     doubleField.field,
			valueType: enumspb.INDEXED_VALUE_TYPE_DOUBLE,
		},
	}
}

func newSearchAttributeDoubleByField(field string) SearchAttributeDouble {
	return SearchAttributeDouble{
		searchAttributeDefinition: searchAttributeDefinition{
			alias:     field,
			field:     field,
			valueType: enumspb.INDEXED_VALUE_TYPE_DOUBLE,
		},
	}
}

func (s SearchAttributeDouble) Value(value float64) SearchAttributeKeyValue {
	return SearchAttributeKeyValue{
		Alias: s.alias,
		Field: s.field,
		Value: VisibilityValueFloat64(value),
	}
}

// SearchAttributeKeyword is a search attribute for a keyword value.
type SearchAttributeKeyword struct {
	searchAttributeDefinition
}

func NewSearchAttributeKeyword(alias string, keywordField SearchAttributeFieldKeyword) SearchAttributeKeyword {
	return SearchAttributeKeyword{
		searchAttributeDefinition: searchAttributeDefinition{
			alias:     alias,
			field:     keywordField.field,
			valueType: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	}
}

func newSearchAttributeKeywordByField(field string) SearchAttributeKeyword {
	return SearchAttributeKeyword{
		searchAttributeDefinition: searchAttributeDefinition{
			alias:     field,
			field:     field,
			valueType: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	}
}

func (s SearchAttributeKeyword) Value(value string) SearchAttributeKeyValue {
	return SearchAttributeKeyValue{
		Alias: s.alias,
		Field: s.field,
		Value: VisibilityValueString(value),
	}
}

// SearchAttributeKeywordList is a search attribute for a keyword list value.
type SearchAttributeKeywordList struct {
	searchAttributeDefinition
}

func NewSearchAttributeKeywordList(alias string, keywordListField SearchAttributeFieldKeywordList) SearchAttributeKeywordList {
	return SearchAttributeKeywordList{
		searchAttributeDefinition: searchAttributeDefinition{
			alias:     alias,
			field:     keywordListField.field,
			valueType: enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
		},
	}
}

func newSearchAttributeKeywordListByField(field string) SearchAttributeKeywordList {
	return SearchAttributeKeywordList{
		searchAttributeDefinition: searchAttributeDefinition{
			alias:     field,
			field:     field,
			valueType: enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
		},
	}
}
func (s SearchAttributeKeywordList) Value(value []string) SearchAttributeKeyValue {
	return SearchAttributeKeyValue{
		Alias: s.alias,
		Field: s.field,
		Value: VisibilityValueStringSlice(value),
	}
}
