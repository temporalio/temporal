package searchattribute

import (
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

type (
	NameTypeMap struct {
		// customSearchAttributes are defined by cluster admin per cluster level and passed and stored in SearchAttributes object.
		customSearchAttributes map[string]enumspb.IndexedValueType
		// archetypeSearchAttributes are defined during component registration, pre-allocated field name to type map is stored here.
		archetypeSearchAttributes map[string]enumspb.IndexedValueType
	}

	category int32
)

const (
	systemCategory category = 1 << iota
	predefinedCategory
	customCategory
	archetypeCategory
)

func buildIndexNameTypeMap(indexSearchAttributes map[string]*persistencespb.IndexSearchAttributes) map[string]NameTypeMap {
	indexNameTypeMap := make(map[string]NameTypeMap, len(indexSearchAttributes))
	for indexName, attrs := range indexSearchAttributes {
		indexNameTypeMap[indexName] = NameTypeMap{
			customSearchAttributes:    attrs.GetCustomSearchAttributes(),
			archetypeSearchAttributes: attrs.GetArchetypeSearchAttributes(),
		}
	}
	return indexNameTypeMap
}

func (m NameTypeMap) System() map[string]enumspb.IndexedValueType {
	allSystem := make(map[string]enumspb.IndexedValueType, len(system)+len(predefined))
	for saName, saType := range system {
		allSystem[saName] = saType
	}
	for saName, saType := range predefined {
		allSystem[saName] = saType
	}
	return allSystem
}

func (m NameTypeMap) Custom() map[string]enumspb.IndexedValueType {
	return m.customSearchAttributes
}

func (m NameTypeMap) All() map[string]enumspb.IndexedValueType {
	allSearchAttributes := make(map[string]enumspb.IndexedValueType, len(system)+len(predefined)+len(m.customSearchAttributes)+len(m.archetypeSearchAttributes))
	for saName, saType := range system {
		allSearchAttributes[saName] = saType
	}
	for saName, saType := range predefined {
		allSearchAttributes[saName] = saType
	}
	for saName, saType := range m.customSearchAttributes {
		allSearchAttributes[saName] = saType
	}
	for saName, saType := range m.archetypeSearchAttributes {
		allSearchAttributes[saName] = saType
	}
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
	if cat|archetypeCategory == cat && len(m.archetypeSearchAttributes) != 0 {
		if t, isArchetype := m.archetypeSearchAttributes[name]; isArchetype {
			return t, nil
		}
	}
	if cat|predefinedCategory == cat {
		if t, isPredefined := predefined[name]; isPredefined {
			return t, nil
		}
	}
	if cat|systemCategory == cat {
		if t, isSystem := system[name]; isSystem {
			return t, nil
		}
	}
	return enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, fmt.Errorf("%w: %s", ErrInvalidName, name)
}

func (m NameTypeMap) IsDefined(name string) bool {
	if _, err := m.GetType(name); err == nil {
		return true
	}
	return false
}
