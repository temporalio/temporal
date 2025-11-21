package chasm

import (
	"errors"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/searchattribute/sadefs"
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

	SearchAttributeTemporalChangeVersion              = newSearchAttributeKeywordListByField(sadefs.TemporalChangeVersion)
	SearchAttributeBinaryChecksums                    = newSearchAttributeKeywordListByField(sadefs.BinaryChecksums)
	SearchAttributeBuildIds                           = newSearchAttributeKeywordListByField(sadefs.BuildIds)
	SearchAttributeBatcherNamespace                   = newSearchAttributeKeywordByField(sadefs.BatcherNamespace)
	SearchAttributeBatcherUser                        = newSearchAttributeKeywordByField(sadefs.BatcherUser)
	SearchAttributeTemporalScheduledStartTime         = newSearchAttributeDateTimeByField(sadefs.TemporalScheduledStartTime)
	SearchAttributeTemporalScheduledByID              = newSearchAttributeKeywordByField(sadefs.TemporalScheduledById)
	SearchAttributeTemporalSchedulePaused             = newSearchAttributeBoolByField(sadefs.TemporalSchedulePaused)
	SearchAttributeTemporalNamespaceDivision          = newSearchAttributeKeywordByField(sadefs.TemporalNamespaceDivision)
	SearchAttributeTemporalPauseInfo                  = newSearchAttributeKeywordListByField(sadefs.TemporalPauseInfo)
	SearchAttributeTemporalReportedProblems           = newSearchAttributeKeywordListByField(sadefs.TemporalReportedProblems)
	SearchAttributeTemporalWorkerDeploymentVersion    = newSearchAttributeKeywordByField(sadefs.TemporalWorkerDeploymentVersion)
	SearchAttributeTemporalWorkflowVersioningBehavior = newSearchAttributeKeywordByField(sadefs.TemporalWorkflowVersioningBehavior)
	SearchAttributeTemporalWorkerDeployment           = newSearchAttributeKeywordByField(sadefs.TemporalWorkerDeployment)
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

func newSearchAttributeIntByField(field string) SearchAttributeInt {
	return SearchAttributeInt{
		searchAttributeDefinition: searchAttributeDefinition{
			alias:     field,
			field:     field,
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

// SearchAttributeMap wraps search attribute values with type-safe access.
type SearchAttributesMap struct {
	values map[string]VisibilityValue
}

// NewSearchAttributeMap creates a new SearchAttributeMap from raw values.
func NewSearchAttributesMap(values map[string]VisibilityValue) SearchAttributesMap {
	return SearchAttributesMap{values: values}
}

// GetBool returns the boolean value for a given SearchAttributeBool. If not found or map is nil, second parameter is false.
func (m SearchAttributesMap) GetBool(sa SearchAttributeBool) (bool, bool) {
	if m.values == nil {
		return false, false
	}

	alias := sa.definition().alias
	boolValue, ok := m.values[alias].(VisibilityValueBool)
	if !ok {
		return false, false
	}

	return bool(boolValue), true
}

// GetInt returns the int value for a given SearchAttributeInt. If not found or map is nil, second parameter is false.
func (m SearchAttributesMap) GetInt(sa SearchAttributeInt) (int, bool) {
	if m.values == nil {
		return 0, false
	}

	alias := sa.definition().alias
	intValue, ok := m.values[alias].(VisibilityValueInt)
	if !ok {
		return 0, false
	}

	return int(intValue), true
}

// GetDouble returns the double value for a given SearchAttributeDouble. If not found or map is nil, second parameter is false.
func (m SearchAttributesMap) GetDouble(sa SearchAttributeDouble) (float64, bool) {
	if m.values == nil {
		return 0, false
	}

	alias := sa.definition().alias
	doubleValue, ok := m.values[alias].(VisibilityValueFloat64)
	if !ok {
		return 0, false
	}

	return float64(doubleValue), true
}

// GetKeyword returns the string value for a given SearchAttributeKeyword. If not found or map is nil, second parameter is false.
func (m SearchAttributesMap) GetKeyword(sa SearchAttributeKeyword) (string, bool) {
	if m.values == nil {
		return "", false
	}

	alias := sa.definition().alias
	stringValue, ok := m.values[alias].(VisibilityValueString)
	if !ok {
		return "", false
	}

	return string(stringValue), true
}

// GetDateTime returns the time value for a given SearchAttributeDateTime. If not found or map is nil, second parameter is false.
func (m SearchAttributesMap) GetDateTime(sa SearchAttributeDateTime) (time.Time, bool) {
	if m.values == nil {
		return time.Time{}, false
	}

	alias := sa.definition().alias
	timeValue, ok := m.values[alias].(VisibilityValueTime)
	if !ok {
		return time.Time{}, false
	}

	return time.Time(timeValue), true
}

// GetKeywordList returns the string list value for a given SearchAttributeKeywordList. If not found or map is nil, second parameter is false.
func (m SearchAttributesMap) GetKeywordList(sa SearchAttributeKeywordList) ([]string, bool) {
	if m.values == nil {
		return nil, false
	}

	alias := sa.definition().alias
	keywordListValue, ok := m.values[alias].(VisibilityValueStringSlice)
	if !ok {
		return nil, false
	}

	return []string(keywordListValue), true
}

// convertToVisibilityValue converts a value to VisibilityValue based on its runtime type.
func convertToVisibilityValue(value interface{}) VisibilityValue {
	switch val := value.(type) {
	case int:
		return VisibilityValueInt64(int64(val))
	case int32:
		return VisibilityValueInt64(int64(val))
	case int64:
		return VisibilityValueInt64(val)
	case float32:
		return VisibilityValueFloat64(float64(val))
	case float64:
		return VisibilityValueFloat64(val)
	case bool:
		return VisibilityValueBool(val)
	case time.Time:
		return VisibilityValueTime(val)
	case string:
		// Try to parse as datetime first
		if parsedTime, err := time.Parse(time.RFC3339, val); err == nil {
			return VisibilityValueTime(parsedTime)
		}
		return VisibilityValueString(val)
	case []byte:
		return VisibilityValueByteSlice(val)
	case []string:
		return VisibilityValueStringSlice(val)
	default:
		// Return as string if type is unknown
		return VisibilityValueString(fmt.Sprintf("%v", val))
	}
}

// AliasChasmSearchAttributes converts search attribute values to VisibilityValue and aliases field names.
// It takes a map of field names to interface{} values, converts them to VisibilityValue based on their runtime type,
// and then aliases the field names using the mapper.
func AliasChasmSearchAttributes(
	chasmSearchAttributes map[string]interface{},
	mapper *VisibilitySearchAttributesMapper,
) (map[string]VisibilityValue, error) {
	if len(chasmSearchAttributes) == 0 {
		return nil, nil
	}

	chasmSAs := make(map[string]VisibilityValue, len(chasmSearchAttributes))
	for fieldName, value := range chasmSearchAttributes {
		visibilityValue := convertToVisibilityValue(value)
		aliasName, err := mapper.Alias(fieldName)
		if err != nil {
			// Silently ignore serviceerror.InvalidArgument because it indicates unmapped field, search attribute is not registered.
			// IMPORTANT: Chasm search attributes must be registered with the CHASM Registry using the WithSearchAttributes() option.
			var invalidArgumentErr *serviceerror.InvalidArgument
			if errors.As(err, &invalidArgumentErr) {
				continue
			}
			return nil, err
		}
		chasmSAs[aliasName] = visibilityValue
	}

	return chasmSAs, nil
}
