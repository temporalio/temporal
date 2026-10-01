package query

import (
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

func ResolveSearchAttributeAlias(
	alias string,
	namespaceName namespace.Name,
	saMapper searchattribute.Mapper,
	saTypeMap searchattribute.NameTypeMap,
	chasmMapper *chasm.VisibilitySearchAttributesMapper,
	archetypeID chasm.ArchetypeID,
) (fieldName string, fieldType enumspb.IndexedValueType, retErr error) {
	// resolveCSA only returns true if `alias` is a custom search attribute.
	resolveCSA := func(alias string) bool {
		fn, err := saMapper.GetFieldName(alias, namespaceName.String())
		if err != nil {
			return false
		}
		ft, err := saTypeMap.GetType(fn)
		if err != nil {
			return false
		}
		fieldName, fieldType = fn, ft
		return true
	}

	// resolveChasmSA only returns true if `alias` is a CHASM search attribute.
	resolveChasmSA := func(alias string) bool {
		if chasmMapper == nil {
			return false
		}
		fn, err := chasmMapper.Field(alias)
		if err != nil {
			return false
		}
		ft, err := chasmMapper.ValueType(fn)
		if err != nil {
			return false
		}
		fieldName, fieldType = fn, ft
		return true
	}

	// resolveSystemSA only returns true if `fn` is a system/reserved search attribute.
	resolveSystemSA := func(fn string) bool {
		if sadefs.IsMappable(fn) {
			// If it's mappable, then it's a field used for custom search attributes.
			return false
		}
		ft, err := saTypeMap.GetType(fn)
		if err != nil {
			return false
		}
		fieldName, fieldType = fn, ft
		return true
	}

	// First, check if it's a custom search attribute.
	if sadefs.IsMappable(alias) && resolveCSA(alias) {
		return
	}
	// Second, check if it's a CHASM search attribute.
	if resolveChasmSA(alias) {
		return
	}
	// Third, check if it's a system/reserved search attribute.
	if resolveSystemSA(alias) {
		return
	}
	// Fourth, check for special aliases or adding/removing the `Temporal` prefix.
	fn := ""
	if strings.TrimPrefix(alias, sadefs.ReservedPrefix) == sadefs.ScheduleID {
		fn = sadefs.WorkflowID
	} else if archetypeID == chasm.SchedulerArchetypeID && alias == "TemporalSystemExecutionStatus" {
		// To support querying Workflow based schedulers and CHASM based schedulers, we need to translate
		// TemporalSystemExecutionStatus as an alias to the system search attribute ExecutionStatus.
		fn = sadefs.ExecutionStatus
	} else if strings.HasPrefix(alias, sadefs.ReservedPrefix) {
		fn = alias[len(sadefs.ReservedPrefix):]
	} else {
		fn = sadefs.ReservedPrefix + alias
	}
	if resolveSystemSA(fn) {
		return
	}

	return "", enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED,
		NewConverterError("%s: %s", InvalidSearchAttribute, alias)
}
