// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
	}

	category int32
)

const (
	systemCategory category = 1 << iota
	predefinedCategory
	customCategory
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
	allSearchAttributes := make(map[string]enumspb.IndexedValueType, len(system)+len(m.customSearchAttributes)+len(predefined))
	for saName, saType := range system {
		allSearchAttributes[saName] = saType
	}
	for saName, saType := range predefined {
		allSearchAttributes[saName] = saType
	}
	for saName, saType := range m.customSearchAttributes {
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
