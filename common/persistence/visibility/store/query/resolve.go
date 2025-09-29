package query

import (
	"fmt"
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/searchattribute"
)

func ResolveSearchAttributeAlias(
	name string,
	ns namespace.Name,
	mapper searchattribute.Mapper,
	saTypeMap searchattribute.NameTypeMap,
) (string, enumspb.IndexedValueType, error) {
	// 1. Skip mapping to custom search attribute if the name has "Temporal" prefix
	if strings.HasPrefix(name, "Temporal") {
		// Check if it's a system/predefined search attribute with Temporal prefix
		if saType, err := saTypeMap.GetType(name); err == nil {
			return name, saType, nil
		}
		return "", enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, NewConverterError("invalid search attribute: %s", name)
	}

	// 2. If the search attribute exists in the visibility mapper, pass it through
	if searchattribute.IsMappable(name) {
		if mapper != nil {
			fieldName, err := mapper.GetFieldName(name, ns.String())
			if err == nil {
				fieldType, err := saTypeMap.GetType(fieldName)
				if err == nil {
					return fieldName, fieldType, nil
				}
			} else {
				// Check if this is a mapper error that should be returned vs ignored
				// Based on the TestMapper comments, "mapper error" should be returned,
				// but "unmapped alias" and "invalid alias" should be ignored
				if err.Error() == "mapper error" {
					return "", enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, err
				}
				// For other mapper errors (like "unmapped alias", "invalid alias"), ignore and continue
			}
		}
	}

	// 3. If it exists as a system or pre-defined attribute, map it
	if saType, err := saTypeMap.GetType(name); err == nil {
		return name, saType, nil
	}
	prefixedName := fmt.Sprintf("Temporal%s", name)
	if saType, err := saTypeMap.GetType(prefixedName); err == nil {
		return prefixedName, saType, nil
	}

	// 4. Handle special cases (maintain ScheduleID -> WorkflowID mapping)
	if name == searchattribute.ScheduleID {
		saType, err := saTypeMap.GetType(searchattribute.WorkflowID)
		if err == nil {
			return searchattribute.WorkflowID, saType, nil
		}
	}

	// 5. In the future we will need to lookup in the CHASM archetype search attribute mapping.
	// For now, return error if not found
	return "", enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, NewConverterError("invalid search attribute: %s", name)
}
