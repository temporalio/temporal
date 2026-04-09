package searchattribute

import (
	"fmt"
	"maps"

	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

const (
	systemCategory category = 1 << iota
	predefinedCategory
	customCategory
)

var (
	system     = sadefs.System()
	predefined = sadefs.Predefined()
)

type (
	NameTypeMap struct {
		// systemSearchAttributes are by default defined internally (sadefs.System()).
		// You can overwrite it by calling WithSystemSearchAttributes.
		systemSearchAttributes map[string]enumspb.IndexedValueType

		// predefinedSearchAttributes are by default defined internally (sadefs.Predefined()).
		// You can overwrite it by calling WithPredefinedSearchAttributes.
		predefinedSearchAttributes map[string]enumspb.IndexedValueType

		// customSearchAttributes are defined by cluster admin per cluster level and
		// passed and stored in SearchAttributes object.
		customSearchAttributes map[string]enumspb.IndexedValueType
	}

	category int32
)

func buildIndexNameTypeMap(
	indexSearchAttributes map[string]*persistencespb.IndexSearchAttributes,
) map[string]NameTypeMap {
	indexNameTypeMap := make(map[string]NameTypeMap, len(indexSearchAttributes))
	for indexName, customSearchAttributes := range indexSearchAttributes {
		indexNameTypeMap[indexName] = NewNameTypeMap(customSearchAttributes.GetCustomSearchAttributes())
	}
	return indexNameTypeMap
}

// NewNameTypeMap creates a new NameTypeMap with the given custom search attributes.
func NewNameTypeMap(customSearchAttributes map[string]enumspb.IndexedValueType) NameTypeMap {
	return NameTypeMap{
		systemSearchAttributes:     system,
		predefinedSearchAttributes: predefined,
		customSearchAttributes:     customSearchAttributes,
	}
}

// WithSystemSearchAttributes returns a new NameTypeMap overriding the system search
// attributes with the given input.
// The default value is the sadefs.System() map which contains the internal system search
// attributes.
// If you need to overwrite it while preserving the internal system search attributes, you can
// call as follows:
//
//	base := NewNameTypeMap(nil)
//	systemSearchAttributes := sadefs.System()
//	systemSearchAttributes["your_system_key"] = <search_attribute_type>
//	result = base.WithSystemSearchAttributes(systemSearchAttributes)
func (m NameTypeMap) WithSystemSearchAttributes(
	systemSearchAttributes map[string]enumspb.IndexedValueType,
) NameTypeMap {
	m.systemSearchAttributes = systemSearchAttributes
	return m
}

// WithPredefinedSearchAttributes returns a new NameTypeMap overriding the predefined search
// attributes with the given input.
// The default value is the sadefs.Predefined() map which contains the internal predefined search
// attributes.
// If you need to overwrite it while preserving the internal predefined search attributes, you can
// call as follows:
//
//	base := NewNameTypeMap(nil)
//	predefinedSearchAttributes := sadefs.Predefined()
//	predefinedSearchAttributes["your_predefined_key"] = <search_attribute_type>
//	result = base.WithPredefinedSearchAttributes(predefinedSearchAttributes)
func (m NameTypeMap) WithPredefinedSearchAttributes(
	predefinedSearchAttributes map[string]enumspb.IndexedValueType,
) NameTypeMap {
	m.predefinedSearchAttributes = predefinedSearchAttributes
	return m
}

func (m NameTypeMap) system() map[string]enumspb.IndexedValueType {
	if len(m.systemSearchAttributes) == 0 {
		return system
	}
	return m.systemSearchAttributes
}

func (m NameTypeMap) predefined() map[string]enumspb.IndexedValueType {
	if len(m.predefinedSearchAttributes) == 0 {
		return predefined
	}
	return m.predefinedSearchAttributes
}

func (m NameTypeMap) System() map[string]enumspb.IndexedValueType {
	systemSearchAttributes := m.system()
	predefinedSearchAttributes := m.predefined()
	allSystem := make(
		map[string]enumspb.IndexedValueType,
		len(systemSearchAttributes)+len(predefinedSearchAttributes),
	)
	maps.Copy(allSystem, systemSearchAttributes)
	maps.Copy(allSystem, predefinedSearchAttributes)
	return allSystem
}

func (m NameTypeMap) Custom() map[string]enumspb.IndexedValueType {
	return m.customSearchAttributes
}

func (m NameTypeMap) All() map[string]enumspb.IndexedValueType {
	systemSearchAttributes := m.system()
	predefinedSearchAttributes := m.predefined()
	allSearchAttributes := make(
		map[string]enumspb.IndexedValueType,
		len(systemSearchAttributes)+len(predefinedSearchAttributes)+len(m.customSearchAttributes),
	)
	maps.Copy(allSearchAttributes, systemSearchAttributes)
	maps.Copy(allSearchAttributes, predefinedSearchAttributes)
	maps.Copy(allSearchAttributes, m.customSearchAttributes)
	return allSearchAttributes
}

// GetType returns type of search attribute from type map.
func (m NameTypeMap) GetType(name string) (enumspb.IndexedValueType, error) {
	return m.getType(name, systemCategory|predefinedCategory|customCategory)
}

// GetType returns type of search attribute from type map.
func (m NameTypeMap) getType(name string, cat category) (enumspb.IndexedValueType, error) {
	if cat|customCategory == cat && len(m.customSearchAttributes) != 0 {
		if t, isCustom := m.customSearchAttributes[name]; isCustom {
			return t, nil
		}
	}
	if cat|predefinedCategory == cat {
		predefinedSearchAttributes := m.predefined()
		if t, isPredefined := predefinedSearchAttributes[name]; isPredefined {
			return t, nil
		}
	}
	if cat|systemCategory == cat {
		systemSearchAttributes := m.system()
		if t, isSystem := systemSearchAttributes[name]; isSystem {
			return t, nil
		}
	}
	return enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, fmt.Errorf("%w: %s", sadefs.ErrInvalidName, name)
}

func (m NameTypeMap) IsDefined(name string) bool {
	if _, err := m.GetType(name); err == nil {
		return true
	}
	return false
}

// MergeNameTypeMaps merges two NameTypeMap. The first NameTypeMap is used as base, and the second
// NameTypeMap is added to the first map, ie., in case of conflicts, elements from the second map
// overwrites elements from the first map.
func MergeNameTypeMaps(a NameTypeMap, b NameTypeMap) NameTypeMap {
	res := NameTypeMap{
		systemSearchAttributes:     make(map[string]enumspb.IndexedValueType),
		predefinedSearchAttributes: make(map[string]enumspb.IndexedValueType),
		customSearchAttributes:     make(map[string]enumspb.IndexedValueType),
	}
	maps.Copy(res.systemSearchAttributes, a.systemSearchAttributes)
	maps.Copy(res.systemSearchAttributes, b.systemSearchAttributes)
	maps.Copy(res.predefinedSearchAttributes, a.predefinedSearchAttributes)
	maps.Copy(res.predefinedSearchAttributes, b.predefinedSearchAttributes)
	maps.Copy(res.customSearchAttributes, a.customSearchAttributes)
	maps.Copy(res.customSearchAttributes, b.customSearchAttributes)
	return res
}
