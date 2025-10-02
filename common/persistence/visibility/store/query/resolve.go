package query

import (
	"errors"
	"fmt"
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/searchattribute"
)

func ResolveSearchAttributeAlias(
	name string,
	ns namespace.Name,
	mapper searchattribute.Mapper,
	saTypeMap searchattribute.NameTypeMap,
) (string, enumspb.IndexedValueType, error) {
	if strings.HasPrefix(name, searchattribute.ReservedPrefix) {
		if name == "TemporalBuildIds" {
			saType, err := saTypeMap.GetType(searchattribute.BuildIds)
			if err != nil {
				return "", enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, NewConverterError("invalid search attribute: %s", name)
			}
			return searchattribute.BuildIds, saType, nil
		}
		saType, err := saTypeMap.GetType(name)
		if err != nil {
			return "", enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, NewConverterError("invalid search attribute: %s", name)
		}
		return name, saType, nil
	}

	if searchattribute.IsMappable(name) {
		if result, found := tryVisibilityMapper(name, ns, mapper, saTypeMap); found {
			return result.fieldName, result.fieldType, result.err
		}
	}

	if result, found := tryDirectAndPrefixedLookup(name, saTypeMap); found {
		return result.fieldName, result.fieldType, result.err
	}

	if name == searchattribute.ScheduleID {
		saType, err := saTypeMap.GetType(searchattribute.WorkflowID)
		if err != nil {
			return "", enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, NewConverterError("invalid search attribute: %s", searchattribute.ScheduleID)
		}
		return searchattribute.WorkflowID, saType, nil
	}

	return "", enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, NewConverterError("invalid search attribute: %s", name)
}

type resolveResult struct {
	fieldName string
	fieldType enumspb.IndexedValueType
	err       error
}

func tryVisibilityMapper(name string, ns namespace.Name, mapper searchattribute.Mapper, saTypeMap searchattribute.NameTypeMap) (resolveResult, bool) {
	if mapper == nil {
		return resolveResult{}, false
	}

	fieldName, err := mapper.GetFieldName(name, ns.String())
	if err != nil {
		var internalErr serviceerror.Internal
		if errors.Is(err, &internalErr) {
			return resolveResult{err: err}, false
		}
		
		var invalidArgument *serviceerror.InvalidArgument
		if errors.As(err, &invalidArgument)
			if name == searchattribute.ScheduleID {
				fieldName = searchattribute.WorkflowID
			} else {
				return resolveResult{err: err}, false
			}
		}
	}

	fieldType, err := saTypeMap.GetType(fieldName)
	if err != nil {
		return resolveResult{err: err}, false
	}
	return resolveResult{fieldName: fieldName, fieldType: fieldType}, true
}

func tryDirectAndPrefixedLookup(name string, saTypeMap searchattribute.NameTypeMap) (resolveResult, bool) {
	if saType, err := saTypeMap.GetType(name); err == nil {
		return resolveResult{fieldName: name, fieldType: saType}, true
	}

	prefixedName := searchattribute.ReservedPrefix + name
	if saType, err := saTypeMap.GetType(prefixedName); err == nil {
		return resolveResult{fieldName: prefixedName, fieldType: saType}, true
	}

	return resolveResult{}, false
}
