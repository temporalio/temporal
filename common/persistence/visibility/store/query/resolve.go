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
		// Handle ScheduleID → WorkflowID transformation first, regardless of mapper
		if name == searchattribute.ScheduleID {
			saType, err := saTypeMap.GetType(searchattribute.WorkflowID)
			if err == nil {
				return searchattribute.WorkflowID, saType, nil
			}
			// If WorkflowID is not in type map, continue with normal flow
		}

		if result, found := tryVisibilityMapper(name, ns, mapper, saTypeMap); found {
			return result.fieldName, result.fieldType, result.err
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
		var internalErr serviceerror.Internal
		if errors.Is(err, &internalErr) {
			// Internal errors indicate system-level issues that should always be propagated
			// and not fall back to direct lookup. These could come from the MapperProvider.GetMapper()
			// call (e.g., namespace registry failures, persistence layer issues) rather than
			// the mapper methods themselves.
			return resolveResult{err: err}, true
		}

		var invalidArgument *serviceerror.InvalidArgument
		if errors.As(err, &invalidArgument) {
			// Check if this looks like a mapper that should handle this field
			// vs a mapper that doesn't know about this field
			errMsg := invalidArgument.Error()
			if strings.Contains(errMsg, "invalid alias") || strings.Contains(errMsg, "invalid field") {
				// Mapper doesn't handle this field, allow fallback
				return resolveResult{}, false
			}
			// Other errors suggest mapper tried to handle it but failed
			return resolveResult{err: err}, true
		}
	}

	fieldType, err := saTypeMap.GetType(fieldName)
	if err != nil {
		// If the mapped field doesn't exist in type map, allow fallback
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
