package query

import (
	"errors"
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/searchattribute"
)

// ResolveSearchAttributeAlias resolves the search attribute alias for the given name. The process is:
//  1. If the name has the "Temporal", skip mapping to a custom search attribute.
//  2. If the search attribute exists in the visibility mapper, pass it through.
//  3. If it exists as a system / predefined attribute, map it.
//     3.1 Some pre-defined attributes are already defined with the Temporal prefix, so both options need to be checked.
//  4. In the future, also need to lookup in the CHASM archetype search attribute mapping
func ResolveSearchAttributeAlias(
	name string,
	ns namespace.Name,
	mapper searchattribute.Mapper,
	saTypeMap searchattribute.NameTypeMap,
) (string, enumspb.IndexedValueType, error) {
	if strings.HasPrefix(name, searchattribute.ReservedPrefix) {
		fieldName := name
		if name[len(searchattribute.ReservedPrefix):] == searchattribute.BuildIds {
			fieldName = searchattribute.BuildIds
		}
		saType, err := saTypeMap.GetType(fieldName)
		if err != nil {
			return "", enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, NewConverterError("invalid search attribute: %s", name)
		}
		return fieldName, saType, nil
	}

	if searchattribute.IsMappable(name) {
		// First check if the visibility mapper can handle this field (e.g., custom search attributes)
		if result, found := tryVisibilityMapper(name, ns, mapper, saTypeMap); found {
			return result.fieldName, result.fieldType, result.err
		}

		// Handle ScheduleID → WorkflowID transformation, but only if ScheduleID is not defined as a custom search attribute
		// This fallback only applies when the visibility mapper doesn't handle the field
		if name == searchattribute.ScheduleID {
			// First check if ScheduleID exists as a custom search attribute
			if _, err := saTypeMap.GetType(searchattribute.ScheduleID); err != nil {
				// ScheduleID is not defined, transform to WorkflowID if WorkflowID exists
				saType, err := saTypeMap.GetType(searchattribute.WorkflowID)
				if err == nil {
					return searchattribute.WorkflowID, saType, nil
				}
				// If WorkflowID is not in type map, continue with normal flow
			}
		}
	}

	if result, found := tryDirectAndPrefixedLookup(name, saTypeMap); found {
		return result.fieldName, result.fieldType, result.err
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
		var invalidArgument *serviceerror.InvalidArgument
		if errors.As(err, &invalidArgument) {
			// Check if this looks like a mapper that should handle this field
			// vs a mapper that doesn't know about this field
			errMsg := invalidArgument.Error()
			if strings.Contains(errMsg, "invalid alias") || // From GetFieldName() when alias doesn't exist in mapper
				strings.Contains(errMsg, "invalid field") || // From GetAlias() when field name doesn't exist in mapper
				strings.Contains(errMsg, "no mapping defined") { // From test mappers and some implementations when field is unknown
				// Mapper doesn't handle this field, allow fallback to direct/prefixed lookup
				return resolveResult{}, false
			}
			// Other InvalidArgument errors suggest mapper tried to handle the field but failed
			// due to validation issues, malformed data, etc. These should be propagated.
			return resolveResult{err: err}, true
		}

		// For other error types (not Internal or InvalidArgument), allow fallback.
		return resolveResult{}, false
	}

	// Mapper successfully resolved the field name, now check if it exists in the type map
	fieldType, err := saTypeMap.GetType(fieldName)
	if err != nil {
		// If the mapped field doesn't exist in type map, allow fallback to direct/prefixed lookup.
		// This can happen when the mapper returns a field name that's not registered in the namespace.
		return resolveResult{}, false
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
