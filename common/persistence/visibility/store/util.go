package store

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/searchattribute"
)

func CombineTypeMaps(
	customTypeMap searchattribute.NameTypeMap,
	chasmMapper *chasm.VisibilitySearchAttributesMapper,
) searchattribute.NameTypeMap {
	if chasmTypeMap := chasmMapper.SATypeMap(); len(chasmTypeMap) > 0 {
		return searchattribute.MergeNameTypeMaps(
			customTypeMap,
			searchattribute.NewNameTypeMap(chasmTypeMap),
		)
	}
	return customTypeMap
}
