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

	var err error
	fieldName = alias
	// First, check if it's a custom search attribute.
	if sadefs.IsMappable(alias) && resolveCSA(alias) {
		return
	}
	// Second, check if it's a CHASM search attribute.
	if resolveChasmSA(alias) {
		return
	}
	// Third, check if it's a system/reserved search attribute.
	fieldType, err = saTypeMap.GetType(fieldName)
	if err == nil {
		return
	}
	// Fourth, check for special aliases or adding/removing the `Temporal` prefix.
	if strings.TrimPrefix(alias, sadefs.ReservedPrefix) == sadefs.ScheduleID {
		fieldName = sadefs.WorkflowID
	} else if archetypeID == chasm.SchedulerArchetypeID && alias == "TemporalSystemExecutionStatus" {
		// To support querying Workflow based schedulers and CHASM based schedulers, we need to translate
		// TemporalSystemExecutionStatus as an alias to the system search attribute ExecutionStatus.
		fieldName = sadefs.ExecutionStatus
	} else if strings.HasPrefix(fieldName, sadefs.ReservedPrefix) {
		fieldName = fieldName[len(sadefs.ReservedPrefix):]
	} else {
		fieldName = sadefs.ReservedPrefix + fieldName
	}
	fieldType, err = saTypeMap.GetType(fieldName)
	if err == nil {
		return
	}

	return "", enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED,
		NewConverterError("%s: %s", InvalidSearchAttribute, alias)
}
