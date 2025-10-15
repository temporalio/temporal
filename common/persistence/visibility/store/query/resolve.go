package query

import (
	"fmt"
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
	fmt.Printf("RESOLVEDBG: Starting resolution for field name='%s', namespace='%s'\n", name, ns.String())

	// if fieldName, ok := strings.CutPrefix(name, searchattribute.ReservedPrefix); ok {
	// 	fmt.Printf("RESOLVEDBG: Found Temporal prefix, stripped field name='%s'\n", fieldName)
	// 	saType, err := saTypeMap.GetType(fieldName)
	// 	if err == nil {
	// 		fmt.Printf("RESOLVEDBG: Successfully resolved with Temporal prefix: field='%s', type=%v\n", fieldName, saType)
	// 		return fieldName, saType, nil
	// 	}
	// 	fmt.Printf("RESOLVEDBG: Failed to get type for Temporal prefixed field='%s', error=%v\n", fieldName, err)
	// }

	if searchattribute.IsMappable(name) {
		fmt.Printf("RESOLVEDBG: Field '%s' is mappable, trying visibility mapper\n", name)
		// First check if the visibility mapper can handle this field (e.g., custom search attributes)
		fieldName, fieldType := tryVisibilityMapper(name, ns, mapper, saTypeMap)
		if fieldName != "" {
			fmt.Printf("RESOLVEDBG: Visibility mapper resolved: field='%s', type=%v\n", fieldName, fieldType)
			return fieldName, fieldType, nil
		}
		fmt.Printf("RESOLVEDBG: Visibility mapper failed to resolve field '%s'\n", name)

		// Handle ScheduleID → WorkflowID transformation, but only if ScheduleID is not defined as a custom search attribute
		// This fallback only applies when the visibility mapper doesn't handle the field
		if name == searchattribute.ScheduleID {
			fmt.Printf("RESOLVEDBG: Applying ScheduleID->WorkflowID transformation for field '%s'\n", name)
			// ScheduleID is not defined, transform to WorkflowID
			saType, _ := saTypeMap.GetType(searchattribute.WorkflowID)
			fmt.Printf("RESOLVEDBG: ScheduleID transformed to WorkflowID: field='%s', type=%v\n", searchattribute.WorkflowID, saType)
			return searchattribute.WorkflowID, saType, nil
		}
	} else {
		fmt.Printf("RESOLVEDBG: Field '%s' is not mappable\n", name)
	}

	fmt.Printf("RESOLVEDBG: Trying direct and prefixed lookup for field '%s'\n", name)
	fieldName, fieldType, found := tryDirectAndPrefixedLookup(name, saTypeMap)
	if found {
		fmt.Printf("RESOLVEDBG: Direct/prefixed lookup succeeded: field='%s', type=%v\n", fieldName, fieldType)
		return fieldName, fieldType, nil
	}
	fmt.Printf("RESOLVEDBG: All resolution attempts failed for field '%s'\n", name)

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
	fmt.Printf("RESOLVEDBG: tryVisibilityMapper called with name='%s', namespace='%s'\n", name, ns.String())

	if mapper == nil {
		fmt.Printf("RESOLVEDBG: tryVisibilityMapper - mapper is nil, returning empty\n")
		return "", enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED
	}

	fieldName, err := mapper.GetFieldName(name, ns.String())
	if err != nil {
		fmt.Printf("RESOLVEDBG: tryVisibilityMapper - GetFieldName failed for name='%s', error=%v\n", name, err)
		// If there is an error, we need to continue with the fallback logic
		// because this search attribute is not defined in the mapper, but might
		// exist with a different name in the namespace.
		return "", enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED
	}
	fmt.Printf("RESOLVEDBG: tryVisibilityMapper - GetFieldName succeeded: name='%s' -> fieldName='%s'\n", name, fieldName)

	// Mapper successfully resolved the field name, now check if it exists in the type map
	fieldType, err := saTypeMap.GetType(fieldName)
	if err != nil {
		fmt.Printf("RESOLVEDBG: tryVisibilityMapper - GetType failed for fieldName='%s', error=%v\n", fieldName, err)
		// If the mapped field doesn't exist in type map, allow fallback to direct/prefixed lookup.
		return "", enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED
	}
	fmt.Printf("RESOLVEDBG: tryVisibilityMapper - GetType succeeded: fieldName='%s', type=%v\n", fieldName, fieldType)
	return fieldName, fieldType
}

func tryDirectAndPrefixedLookup(name string, saTypeMap searchattribute.NameTypeMap) (string, enumspb.IndexedValueType, bool) {
	fmt.Printf("RESOLVEDBG: tryDirectAndPrefixedLookup called with name='%s'\n", name)

	if saType, err := saTypeMap.GetType(name); err == nil {
		fmt.Printf("RESOLVEDBG: tryDirectAndPrefixedLookup - direct lookup succeeded: name='%s', type=%v\n", name, saType)
		return name, saType, true
	}
	fmt.Printf("RESOLVEDBG: tryDirectAndPrefixedLookup - direct lookup failed for name='%s'\n", name)

	prefixedName := searchattribute.ReservedPrefix + name
	fmt.Printf("RESOLVEDBG: tryDirectAndPrefixedLookup - trying prefixed name='%s'\n", prefixedName)
	if saType, err := saTypeMap.GetType(prefixedName); err == nil {
		fmt.Printf("RESOLVEDBG: tryDirectAndPrefixedLookup - prefixed lookup succeeded: name='%s', type=%v\n", prefixedName, saType)
		return prefixedName, saType, true
	}
	fmt.Printf("RESOLVEDBG: tryDirectAndPrefixedLookup - prefixed lookup failed for name='%s'\n", prefixedName)

	strippedName := strings.TrimPrefix(name, searchattribute.ReservedPrefix)
	fmt.Printf("RESOLVEDBG: tryDirectAndPrefixedLookup - trying stripped name='%s'\n", strippedName)
	if saType, err := saTypeMap.GetType(strippedName); err == nil {
		fmt.Printf("RESOLVEDBG: tryDirectAndPrefixedLookup - stripped lookup succeeded: name='%s', type=%v\n", strippedName, saType)
		return strippedName, saType, true
	}
	fmt.Printf("RESOLVEDBG: tryDirectAndPrefixedLookup - stripped lookup failed for name='%s'\n", strippedName)

	fmt.Printf("RESOLVEDBG: tryDirectAndPrefixedLookup - both direct and prefixed lookups failed for name='%s'\n", name)
	return "", enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, false
}
