package query

import (
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
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
	if searchattribute.IsMappable(name) {
		// First check if the visibility mapper can handle this field (e.g., custom search attributes)
		fieldName, fieldType := tryVisibilityMapper(name, ns, mapper, saTypeMap)
		if fieldName != "" {
			return fieldName, fieldType, nil
		}

		// Handle ScheduleID → WorkflowID transformation, but only if ScheduleID is not defined as a custom search attribute
		// This fallback only applies when the visibility mapper doesn't handle the field
		if name == searchattribute.ScheduleID {
			// ScheduleID is not defined, transform to WorkflowID
			saType, _ := saTypeMap.GetType(searchattribute.WorkflowID)
			return searchattribute.WorkflowID, saType, nil
		}
	}

	fieldName, fieldType, found := tryDirectAndPrefixedLookup(name, saTypeMap)
	if found {
		return fieldName, fieldType, nil
	}

	return "", enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, NewConverterError("invalid search attribute: %s", name)
}

// tryVisibilityMapper might find a successful match in which case we return it
// otherwise we continue with the fallback logic.
func tryVisibilityMapper(
	name string,
	ns namespace.Name,
	mapper searchattribute.Mapper,
	saTypeMap searchattribute.NameTypeMap,
) (string, enumspb.IndexedValueType) {
	if mapper == nil {
		return "", enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED
	}

	fieldName, err := mapper.GetFieldName(name, ns.String())
	if err != nil {
		// If there is an error, we need to continue with the fallback logic
		// because this search attribute is not defined in the mapper, but might
		// exist with a different name in the namespace.
		return "", enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED
	}

	// Mapper successfully resolved the field name, now check if it exists in the type map
	fieldType, err := saTypeMap.GetType(fieldName)
	if err != nil {
		// If the mapped field doesn't exist in type map, allow fallback to direct/prefixed lookup.
		return "", enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED
	}

	return fieldName, fieldType
}

func tryDirectAndPrefixedLookup(name string, saTypeMap searchattribute.NameTypeMap) (string, enumspb.IndexedValueType, bool) {
	if saType, err := saTypeMap.GetType(name); err == nil {
		return name, saType, true
	}

	prefixedName := searchattribute.ReservedPrefix + name
	if saType, err := saTypeMap.GetType(prefixedName); err == nil {
		return prefixedName, saType, true
	}

	strippedName := strings.TrimPrefix(name, searchattribute.ReservedPrefix)
	if saType, err := saTypeMap.GetType(strippedName); err == nil {
		return strippedName, saType, true
	}

	return "", enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, false
}
