package chasm

import (
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

// CHASM Search Attribute User Guide:
//
// This contains CHASM search attribute field constants. These predefined fields correspond to the exact column name in Visibility storage.
// For each root component, search attributes can be mapped from a user defined alias to these fields.
// Each component must register its search attributes with the CHASM Registry.
//
// To define a CHASM search attribute, create this as a package/global scoped variable. Below is an example:
// var testComponentCompletedSearchAttribute = NewSearchAttributeBool("Completed", SearchAttributeFieldBool01)
// var testComponentFailedSearchAttribute = NewSearchAttributeBool("Failed", SearchAttributeFieldBool02)
// var testComponentStartTimeSearchAttribute = NewSearchAttributeTime("StartTime", SearchAttributeFieldDateTime01)
// var testComponentCategorySearchAttribute = NewSearchAttributeLowCardinalityKeyword("Category", SearchAttributeFieldLowCardinalityKeyword01)
//
// Each CHASM search attribute field is associated with a specific indexed value type. The Value() method of a search attribute
// specifies the supported value type to set at compile time. eg. DateTime values must be set with a time.Time typed value.
//
// Low Cardinality Keyword Fields: used for categorical data that support GROUP BY aggregations. Values must be limited to a small number of dimensions.
//
// Each root component can only use a predefined search attribute field once. Developers should not reassign aliases to different fields.
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

	// SearchAttributeFieldLowCardinalityKeyword is a search attribute field for a low cardinality keyword value.
	// Used for categorical data that support GROUP BY aggregations, eg. CHASM Execution Statuses.
	SearchAttributeFieldLowCardinalityKeyword01 = newSearchAttributeFieldLowCardinalityKeyword(1)

	SearchAttributeFieldKeywordList01 = newSearchAttributeFieldKeywordList(1)
	SearchAttributeFieldKeywordList02 = newSearchAttributeFieldKeywordList(2)

	SearchAttributeTemporalChangeVersion                = newSearchAttributeKeywordListByField(sadefs.TemporalChangeVersion)
	SearchAttributeBinaryChecksums                      = newSearchAttributeKeywordListByField(sadefs.BinaryChecksums)
	SearchAttributeBuildIds                             = newSearchAttributeKeywordListByField(sadefs.BuildIds)
	SearchAttributeBatcherNamespace                     = newSearchAttributeKeywordByField(sadefs.BatcherNamespace)
	SearchAttributeBatcherUser                          = newSearchAttributeKeywordByField(sadefs.BatcherUser)
	SearchAttributeTemporalScheduledStartTime           = newSearchAttributeDateTimeByField(sadefs.TemporalScheduledStartTime)
	SearchAttributeTemporalScheduledByID                = newSearchAttributeKeywordByField(sadefs.TemporalScheduledById)
	SearchAttributeTemporalSchedulePaused               = newSearchAttributeBoolByField(sadefs.TemporalSchedulePaused)
	SearchAttributeTemporalNamespaceDivision            = newSearchAttributeKeywordByField(sadefs.TemporalNamespaceDivision)
	SearchAttributeTemporalPauseInfo                    = newSearchAttributeKeywordListByField(sadefs.TemporalPauseInfo)
	SearchAttributeTemporalReportedProblems             = newSearchAttributeKeywordListByField(sadefs.TemporalReportedProblems)
	SearchAttributeTemporalWorkerDeploymentVersion      = newSearchAttributeKeywordByField(sadefs.TemporalWorkerDeploymentVersion)
	SearchAttributeTemporalWorkflowVersioningBehavior   = newSearchAttributeKeywordByField(sadefs.TemporalWorkflowVersioningBehavior)
	SearchAttributeTemporalWorkerDeployment             = newSearchAttributeKeywordByField(sadefs.TemporalWorkerDeployment)
	SearchAttributeTemporalUsedWorkerDeploymentVersions = newSearchAttributeKeywordListByField(sadefs.TemporalUsedWorkerDeploymentVersions)
)

var (
	_ SearchAttribute = (*SearchAttributeBool)(nil)
	_ SearchAttribute = (*SearchAttributeDateTime)(nil)
	_ SearchAttribute = (*SearchAttributeInt)(nil)
	_ SearchAttribute = (*SearchAttributeDouble)(nil)
	_ SearchAttribute = (*SearchAttributeKeyword)(nil)
	_ SearchAttribute = (*SearchAttributeKeywordList)(nil)

	_ typedSearchAttribute[bool]      = (*SearchAttributeBool)(nil)
	_ typedSearchAttribute[time.Time] = (*SearchAttributeDateTime)(nil)
	_ typedSearchAttribute[int64]     = (*SearchAttributeInt)(nil)
	_ typedSearchAttribute[float64]   = (*SearchAttributeDouble)(nil)
	_ typedSearchAttribute[string]    = (*SearchAttributeKeyword)(nil)
	_ typedSearchAttribute[[]string]  = (*SearchAttributeKeywordList)(nil)
)

type (
	// SearchAttribute is a shared interface for all search attribute types. Each type must embed searchAttributeDefinition.
	SearchAttribute interface {
		definition() searchAttributeDefinition
	}

	typedSearchAttribute[T any] interface {
		SearchAttribute
		typeMarker(T)
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

func newSearchAttributeFieldLowCardinalityKeyword(index int) SearchAttributeFieldKeyword {
	return SearchAttributeFieldKeyword{
		field: fmt.Sprintf("%s%s%02d", sadefs.ReservedPrefix, "LowCardinalityKeyword", index),
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
	return fmt.Sprintf("%s%s%02d", sadefs.ReservedPrefix, valueType.String(), index)
}

func (s searchAttributeDefinition) definition() searchAttributeDefinition {
	return s
}

// SearchAttributeBool is a search attribute for a boolean value.
type SearchAttributeBool struct {
	searchAttributeDefinition
}

// NewSearchAttributeBool creates a new boolean search attribute given a predefined chasm field
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

// Value sets the boolean value of the search attribute.
func (s SearchAttributeBool) Value(value bool) SearchAttributeKeyValue {
	return SearchAttributeKeyValue{
		Alias: s.alias,
		Field: s.field,
		Value: VisibilityValueBool(value),
	}
}

func (s SearchAttributeBool) typeMarker(_ bool) {}

// SearchAttributeDateTime is a search attribute for a datetime value.
type SearchAttributeDateTime struct {
	searchAttributeDefinition
}

// NewSearchAttributeDateTime creates a new date time search attribute given a predefined chasm field
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

// Value sets the date time value of the search attribute.
func (s SearchAttributeDateTime) Value(value time.Time) SearchAttributeKeyValue {
	return SearchAttributeKeyValue{
		Alias: s.alias,
		Field: s.field,
		Value: VisibilityValueTime(value),
	}
}

func (s SearchAttributeDateTime) typeMarker(_ time.Time) {}

// SearchAttributeInt is a search attribute for an integer value.
type SearchAttributeInt struct {
	searchAttributeDefinition
}

// NewSearchAttributeInt creates a new integer search attribute given a predefined chasm field
func NewSearchAttributeInt(alias string, intField SearchAttributeFieldInt) SearchAttributeInt {
	return SearchAttributeInt{
		searchAttributeDefinition: searchAttributeDefinition{
			alias:     alias,
			field:     intField.field,
			valueType: enumspb.INDEXED_VALUE_TYPE_INT,
		},
	}
}

// Value sets the integer value of the search attribute.
func (s SearchAttributeInt) Value(value int64) SearchAttributeKeyValue {
	return SearchAttributeKeyValue{
		Alias: s.alias,
		Field: s.field,
		Value: VisibilityValueInt64(value),
	}
}

func (s SearchAttributeInt) typeMarker(_ int64) {}

// SearchAttributeDouble is a search attribute for a double value.
type SearchAttributeDouble struct {
	searchAttributeDefinition
}

// NewSearchAttributeDouble creates a new double search attribute given a predefined chasm field
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

// Value sets the double value of the search attribute.
func (s SearchAttributeDouble) Value(value float64) SearchAttributeKeyValue {
	return SearchAttributeKeyValue{
		Alias: s.alias,
		Field: s.field,
		Value: VisibilityValueFloat64(value),
	}
}

func (s SearchAttributeDouble) typeMarker(_ float64) {}

// SearchAttributeKeyword is a search attribute for a keyword value.
type SearchAttributeKeyword struct {
	searchAttributeDefinition
}

// NewSearchAttributeKeyword creates a new keyword search attribute given a predefined chasm field
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

// Value sets the string value of the search attribute.
func (s SearchAttributeKeyword) Value(value string) SearchAttributeKeyValue {
	return SearchAttributeKeyValue{
		Alias: s.alias,
		Field: s.field,
		Value: VisibilityValueString(value),
	}
}

func (s SearchAttributeKeyword) typeMarker(_ string) {}

// SearchAttributeKeywordList is a search attribute for a keyword list value.
type SearchAttributeKeywordList struct {
	searchAttributeDefinition
}

// NewSearchAttributeKeywordList creates a new keyword list search attribute given a predefined chasm field
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

// Value sets the string list value of the search attribute.
func (s SearchAttributeKeywordList) Value(value []string) SearchAttributeKeyValue {
	return SearchAttributeKeyValue{
		Alias: s.alias,
		Field: s.field,
		Value: VisibilityValueStringSlice(value),
	}
}

func (s SearchAttributeKeywordList) typeMarker(_ []string) {}

// SearchAttributesMap wraps search attribute values with type-safe access.
type SearchAttributesMap struct {
	values map[string]VisibilityValue
}

// NewSearchAttributesMap creates a new SearchAttributeMap from raw values.
func NewSearchAttributesMap(values map[string]VisibilityValue) SearchAttributesMap {
	return SearchAttributesMap{values: values}
}

// GetValue returns the value for a given SearchAttribute with compile-time type safety.
// The return type T is inferred from the SearchAttribute's type parameter.
// For example, SearchAttributeBool will return a bool value.
// If the value is not found or the type does not match, the zero value for the type T is returned and the second return value is false.
func GetValue[T any](m SearchAttributesMap, sa typedSearchAttribute[T]) (val T, ok bool) {
	var zero T
	if len(m.values) == 0 {
		return zero, false
	}

	alias := sa.definition().alias
	visibilityValue, exists := m.values[alias]
	if !exists {
		return zero, false
	}

	finalVal, ok := visibilityValue.Value().(T)
	if !ok {
		return zero, false
	}
	return finalVal, true
}
