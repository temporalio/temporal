package store

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/searchattribute"
)

func CombineTypeMaps(
	customTypeMap searchattribute.NameTypeMap,
	chasmTypeMap *chasm.VisibilitySearchAttributesMapper,
) searchattribute.NameTypeMap {
	return searchattribute.MergeNameTypeMaps(
		customTypeMap,
		searchattribute.NewNameTypeMap(chasmTypeMap.SATypeMap()),
	)
}
