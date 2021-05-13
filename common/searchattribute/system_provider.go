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
	enumspb "go.temporal.io/api/enums/v1"
)

type (
	SystemProvider struct{}
)

var (
	// system are default internal system search attributes which are shown to the users.
	// Note: NamespaceID is not included.
	system = map[string]enumspb.IndexedValueType{
		WorkflowID:      enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		RunID:           enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		WorkflowType:    enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		StartTime:       enumspb.INDEXED_VALUE_TYPE_INT,
		ExecutionTime:   enumspb.INDEXED_VALUE_TYPE_INT,
		CloseTime:       enumspb.INDEXED_VALUE_TYPE_INT,
		ExecutionStatus: enumspb.INDEXED_VALUE_TYPE_INT,
		TaskQueue:       enumspb.INDEXED_VALUE_TYPE_KEYWORD,

		TemporalChangeVersion: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		BinaryChecksums:       enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		CustomNamespace:       enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		Operator:              enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	}
)

func NewSystemProvider() *SystemProvider {
	return &SystemProvider{}
}

func (s *SystemProvider) GetSearchAttributes(_ string, _ bool) (map[string]enumspb.IndexedValueType, error) {
	return system, nil
}

func GetSystem() map[string]enumspb.IndexedValueType {
	return system
}

// FilterCustom returns new search attributes map with only custom search attributes in it.
func FilterCustom(searchAttributes map[string]enumspb.IndexedValueType) map[string]enumspb.IndexedValueType {
	customSearchAttributes := map[string]enumspb.IndexedValueType{}
	for saName, saType := range searchAttributes {
		if _, isSystem := system[saName]; !isSystem {
			customSearchAttributes[saName] = saType
		}
	}
	return customSearchAttributes
}

// AddSystemTo adds system search attributes to the passed map.
func AddSystemTo(searchAttributes map[string]enumspb.IndexedValueType) {
	for saName, saType := range system {
		searchAttributes[saName] = saType
	}
}
