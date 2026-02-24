package chasm

import enumspb "go.temporal.io/api/enums/v1"

// NewTestVisibilitySearchAttributesMapper creates a new VisibilitySearchAttributesMapper.
// For testing only.
func NewTestVisibilitySearchAttributesMapper(
	fieldToAlias map[string]string,
	saTypeMap map[string]enumspb.IndexedValueType,
) *VisibilitySearchAttributesMapper {
	aliasToField := make(map[string]string, len(fieldToAlias))
	for field, alias := range fieldToAlias {
		aliasToField[alias] = field
	}
	return &VisibilitySearchAttributesMapper{
		aliasToField: aliasToField,
		fieldToAlias: fieldToAlias,
		saTypeMap:    saTypeMap,
	}
}
