package store

import (
	"maps"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/searchattribute"
)

func CombineTypeMaps(customTypeMap searchattribute.NameTypeMap, chasmTypeMap *chasm.VisibilitySearchAttributesMapper) searchattribute.NameTypeMap {
	combinedTypeMap := make(map[string]enumspb.IndexedValueType)
	maps.Copy(combinedTypeMap, customTypeMap.Custom())
	maps.Copy(combinedTypeMap, chasmTypeMap.SATypeMap())
	return searchattribute.NewNameTypeMap(combinedTypeMap)
}
