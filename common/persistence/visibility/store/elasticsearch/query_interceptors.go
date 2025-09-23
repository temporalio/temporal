package elasticsearch

import (
	"fmt"
	"strconv"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/searchattribute"
)

type (
	nameInterceptor struct {
		namespace                      namespace.Name
		searchAttributesTypeMap        searchattribute.NameTypeMap
		searchAttributesMapperProvider searchattribute.MapperProvider
		seenNamespaceDivision          bool
	}

	valuesInterceptor struct {
		namespace               namespace.Name
		searchAttributesTypeMap searchattribute.NameTypeMap
	}
)

func NewNameInterceptor(
	namespaceName namespace.Name,
	saTypeMap searchattribute.NameTypeMap,
	searchAttributesMapperProvider searchattribute.MapperProvider,
) *nameInterceptor {
	return &nameInterceptor{
		namespace:                      namespaceName,
		searchAttributesTypeMap:        saTypeMap,
		searchAttributesMapperProvider: searchAttributesMapperProvider,
	}
}

func NewValuesInterceptor(
	namespaceName namespace.Name,
	saTypeMap searchattribute.NameTypeMap,
) *valuesInterceptor {
	return &valuesInterceptor{
		namespace:               namespaceName,
		searchAttributesTypeMap: saTypeMap,
	}
}

func resolveSearchAttributeAlias(
	name string, ns namespace.Name,
	mapperProvider searchattribute.MapperProvider,
	saTypeMap searchattribute.NameTypeMap,
) (string, enumspb.IndexedValueType, error) {
	// 1. Use the mapper for customer search attributes
	mapper, err := mapperProvider.GetMapper(ns)
	if err == nil && mapper != nil {
		fieldName, err := mapper.GetFieldName(name, ns.String())
		if err == nil {
			fieldType, err := saTypeMap.GetType(fieldName)
			if err == nil {
				return fieldName, fieldType, nil
			}
		}
	}

	// 2. Check if the name is a system/predefined search attribute
	if saType, err := saTypeMap.GetType(name); err == nil {
		return name, saType, nil
	}

	// 3. Check if the name is a system/predefined search attribute with Temporal prefix
	temporalName := fmt.Sprintf("Temporal%s", name)
	if saType, err := saTypeMap.GetType(temporalName); err == nil {
		return temporalName, saType, nil
	}

	// 4. Handle special cases
	if name == searchattribute.ScheduleID {
		saType, err := saTypeMap.GetType(searchattribute.WorkflowID)
		if err == nil {
			return searchattribute.WorkflowID, saType, nil
		}
	}

	// 5. Not found, return error
	return "", enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, query.NewConverterError("invalid search attribute: %s", name)
}

// TODO: this is invoked for non-ES validation code flow. Needs refactoring
func (ni *nameInterceptor) Name(name string, usage query.FieldNameUsage) (string, error) {
	fieldName, fieldType, err := resolveSearchAttributeAlias(name, ni.namespace, ni.searchAttributesMapperProvider, ni.searchAttributesTypeMap)
	if err != nil {
		return "", err
	}

	switch usage {
	case query.FieldNameFilter:
		if fieldName == searchattribute.TemporalNamespaceDivision {
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
		if fieldName != searchattribute.ExecutionStatus {
			return "", query.NewConverterError(
				"'group by' clause is only supported for %s search attribute",
				searchattribute.ExecutionStatus,
			)
		}
	}

	return fieldName, nil
}

func (vi *valuesInterceptor) Values(name string, fieldName string, values ...interface{}) ([]interface{}, error) {
	fieldType, err := vi.searchAttributesTypeMap.GetType(fieldName)
	if err != nil {
		return nil, query.NewConverterError("invalid search attribute: %s", name)
	}

	var result []interface{}
	for _, value := range values {
		value, err = parseSystemSearchAttributeValues(name, value)
		if err != nil {
			return nil, err
		}

		if name == searchattribute.ScheduleID && fieldName == searchattribute.WorkflowID {
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
	case searchattribute.StartTime, searchattribute.CloseTime, searchattribute.ExecutionTime:
		if nanos, isNumber := value.(int64); isNumber {
			value = time.Unix(0, nanos).UTC().Format(time.RFC3339Nano)
		}
	case searchattribute.ExecutionStatus:
		if status, isNumber := value.(int64); isNumber {
			if _, ok := enumspb.WorkflowExecutionStatus_name[int32(status)]; !ok {
				return nil, query.NewConverterError("invalid value for search attribute %s: %v", name, value)
			}
			value = enumspb.WorkflowExecutionStatus(status).String()
		}
	case searchattribute.ExecutionDuration:
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
