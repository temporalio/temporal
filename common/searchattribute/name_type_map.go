package searchattribute

import (
	"fmt"
	"maps"

	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

type (
	NameTypeMap struct {
		// customSearchAttributes are defined by cluster admin per cluster level and passed and stored in SearchAttributes object.
		customSearchAttributes map[string]enumspb.IndexedValueType
	}

	category int32
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

func buildIndexNameTypeMap(indexSearchAttributes map[string]*persistencespb.IndexSearchAttributes) map[string]NameTypeMap {
	indexNameTypeMap := make(map[string]NameTypeMap, len(indexSearchAttributes))
	for indexName, customSearchAttributes := range indexSearchAttributes {
		indexNameTypeMap[indexName] = NameTypeMap{
			customSearchAttributes: customSearchAttributes.GetCustomSearchAttributes(),
		}
	}
	return indexNameTypeMap
}

// NewNameTypeMap creates a new NameTypeMap with the given custom search attributes.
func NewNameTypeMap(customSearchAttributes map[string]enumspb.IndexedValueType) NameTypeMap {
	return NameTypeMap{
		customSearchAttributes: customSearchAttributes,
	}
}

func (m NameTypeMap) System() map[string]enumspb.IndexedValueType {
	allSystem := make(map[string]enumspb.IndexedValueType, len(system)+len(predefined))
	maps.Copy(allSystem, system)
	maps.Copy(allSystem, predefined)
	return allSystem
}

func (m NameTypeMap) Custom() map[string]enumspb.IndexedValueType {
	return m.customSearchAttributes
}

func (m NameTypeMap) All() map[string]enumspb.IndexedValueType {
	allSearchAttributes := make(map[string]enumspb.IndexedValueType, len(system)+len(m.customSearchAttributes)+len(predefined))
	maps.Copy(allSearchAttributes, system)
	maps.Copy(allSearchAttributes, predefined)
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
		predefined := sadefs.Predefined()
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
