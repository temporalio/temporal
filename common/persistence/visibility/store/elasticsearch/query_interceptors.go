package elasticsearch

import (
	"fmt"
	"strconv"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

type (
	nameInterceptor struct {
		namespace                      namespace.Name
		searchAttributesTypeMap        searchattribute.NameTypeMap
		searchAttributesMapperProvider searchattribute.MapperProvider
		seenNamespaceDivision          bool
		chasmMapper                    *chasm.VisibilitySearchAttributesMapper
	}

	valuesInterceptor struct {
		namespace   namespace.Name
		saTypeMap   searchattribute.NameTypeMap
		chasmMapper *chasm.VisibilitySearchAttributesMapper
	}
)

func NewNameInterceptor(
	namespaceName namespace.Name,
	saTypeMap searchattribute.NameTypeMap,
	searchAttributesMapperProvider searchattribute.MapperProvider,
	chasmMapper *chasm.VisibilitySearchAttributesMapper,
) *nameInterceptor {
	return &nameInterceptor{
		namespace:                      namespaceName,
		searchAttributesTypeMap:        saTypeMap,
		searchAttributesMapperProvider: searchAttributesMapperProvider,
		seenNamespaceDivision:          false,
		chasmMapper:                    chasmMapper,
	}
}

func NewValuesInterceptor(
	namespaceName namespace.Name,
	csaTypeMap searchattribute.NameTypeMap,
	chasmMapper *chasm.VisibilitySearchAttributesMapper,
) *valuesInterceptor {
	saTypeMap := store.CombineTypeMaps(csaTypeMap, chasmMapper)
	return &valuesInterceptor{
		namespace:   namespaceName,
		saTypeMap:   saTypeMap,
		chasmMapper: chasmMapper,
	}
}

// TODO: this is invoked for non-ES validation code flow. Needs refactoring
func (ni *nameInterceptor) Name(name string, usage query.FieldNameUsage) (string, error) {
	mapper, err := ni.searchAttributesMapperProvider.GetMapper(ni.namespace)
	if err != nil {
		return "", err
	}
	fieldName, fieldType, err := query.ResolveSearchAttributeAlias(name, ni.namespace, mapper,
		ni.searchAttributesTypeMap, ni.chasmMapper)
	if err != nil {
		return "", err
	}

	switch usage {
	case query.FieldNameFilter:
		if fieldName == sadefs.TemporalNamespaceDivision {
			ni.seenNamespaceDivision = true
		}
	case query.FieldNameSorter:
		if fieldType == enumspb.INDEXED_VALUE_TYPE_TEXT {
			return "", query.NewConverterError(
				"unable to sort by field of %s type, use field of type %s",
				enumspb.INDEXED_VALUE_TYPE_TEXT,
				enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			)
		}
	case query.FieldNameGroupBy:
		if !query.IsGroupByFieldAllowed(fieldName) {
			return "", query.NewConverterError(
				"%s: 'GROUP BY' clause is only supported for ExecutionStatus",
				query.NotSupportedErrMessage,
			)
		}
	}

	return fieldName, nil
}

func (vi *valuesInterceptor) Values(name string, fieldName string, values ...interface{}) ([]interface{}, error) {
	var fieldType enumspb.IndexedValueType
	var err error

	fieldType, err = vi.saTypeMap.GetType(fieldName)
	if err != nil {
		return nil, query.NewConverterError("invalid search attribute: %s", name)
	}

	var result []interface{}
	for _, value := range values {
		value, err = parseSystemSearchAttributeValues(name, value)
		if err != nil {
			return nil, err
		}

		if name == sadefs.ScheduleID && fieldName == sadefs.WorkflowID {
			value = primitives.ScheduleWorkflowIDPrefix + fmt.Sprintf("%v", value)
		}

		value, err = validateValueType(name, value, fieldType)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, nil
}

func parseSystemSearchAttributeValues(name string, value any) (any, error) {
	switch name {
	case sadefs.StartTime, sadefs.CloseTime, sadefs.ExecutionTime:
		if nanos, isNumber := value.(int64); isNumber {
			value = time.Unix(0, nanos).UTC().Format(time.RFC3339Nano)
		}
	case sadefs.ExecutionStatus:
		if status, isNumber := value.(int64); isNumber {
			if _, ok := enumspb.WorkflowExecutionStatus_name[int32(status)]; !ok {
				return nil, query.NewConverterError("invalid value for search attribute %s: %v", name, value)
			}
			value = enumspb.WorkflowExecutionStatus(status).String()
		}
	case sadefs.ExecutionDuration:
		if durationStr, isString := value.(string); isString {
			duration, err := query.ParseExecutionDurationStr(durationStr)
			if err != nil {
				return nil, query.NewConverterError(
					"invalid value for search attribute %s: %v (%v)", name, value, err)
			}
			value = duration.Nanoseconds()
		}
	default:
	}
	return value, nil
}

func validateValueType(name string, value any, fieldType enumspb.IndexedValueType) (any, error) {
	switch fieldType {
	case enumspb.INDEXED_VALUE_TYPE_INT, enumspb.INDEXED_VALUE_TYPE_DOUBLE:
		switch v := value.(type) {
		case int64, float64:
		// nothing to do
		case string:
			// ES can do implicit casting if the value is numeric
			if _, err := strconv.ParseFloat(v, 64); err != nil {
				return nil, query.NewConverterError(
					"invalid value for search attribute %s of type %s: %#v", name, fieldType.String(), value)
			}
		default:
			return nil, query.NewConverterError(
				"invalid value for search attribute %s of type %s: %#v", name, fieldType.String(), value)
		}
	case enumspb.INDEXED_VALUE_TYPE_BOOL:
		switch value.(type) {
		case bool:
		// nothing to do
		default:
			return nil, query.NewConverterError(
				"invalid value for search attribute %s of type %s: %#v", name, fieldType.String(), value)
		}
	case enumspb.INDEXED_VALUE_TYPE_DATETIME:
		switch v := value.(type) {
		case int64:
			value = time.Unix(0, v).UTC().Format(time.RFC3339Nano)
		case string:
			if _, err := time.Parse(time.RFC3339Nano, v); err != nil {
				return nil, query.NewConverterError(
					"invalid value for search attribute %s of type %s: %#v", name, fieldType.String(), value)
			}
		default:
			return nil, query.NewConverterError(
				"invalid value for search attribute %s of type %s: %#v", name, fieldType.String(), value)
		}
	}
	return value, nil
}
