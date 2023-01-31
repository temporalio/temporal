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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination mapper_mock.go

package searchattribute

import (
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/namespace"
)

type (
	// Mapper interface allows overriding custom search attribute names with aliases per namespace.
	// Create an instance of a Mapper interface and pass it to the temporal.NewServer using temporal.WithSearchAttributesMapper.
	// Returned error must be from the serviceerror package.
	Mapper interface {
		GetAlias(fieldName string, namespace string) (string, error)
		GetFieldName(alias string, namespace string) (string, error)
	}

	// identityMapper is used when the visibility store is Elasticsearch.
	identityMapper struct{}

	// This mapper is to be backwards compatible with versions before v1.20.
	// Users using SQL DB might have custom search attributes registered.
	// Those search attributes won't be searchable, as they weren't before
	// v1.20 (using SQL DB means standard visibility). Thus, this mapper will
	// allow those search attributes to be used without going through aliasing.
	backCompMapper_v1_20 struct {
		mapper                 Mapper
		emptyStringNameTypeMap *NameTypeMap
	}
)

var _ Mapper = (*namespace.CustomSearchAttributesMapper)(nil)
var _ Mapper = (*identityMapper)(nil)
var _ Mapper = (*backCompMapper_v1_20)(nil)

func NewIdentityMapper() Mapper {
	return &identityMapper{}
}

func (m *identityMapper) GetAlias(fieldName string, namespaceName string) (string, error) {
	return fieldName, nil
}

func (m *identityMapper) GetFieldName(alias string, namespaceName string) (string, error) {
	return alias, nil
}

func (m *backCompMapper_v1_20) GetAlias(fieldName string, namespaceName string) (string, error) {
	alias, err1 := m.mapper.GetAlias(fieldName, namespaceName)
	if err1 != nil {
		if m.emptyStringNameTypeMap != nil {
			_, err2 := m.emptyStringNameTypeMap.getType(fieldName, customCategory)
			if err2 == nil {
				return fieldName, nil
			}
		}
		return "", err1
	}
	return alias, nil
}

func (m *backCompMapper_v1_20) GetFieldName(alias string, namespaceName string) (string, error) {
	fieldName, err1 := m.mapper.GetFieldName(alias, namespaceName)
	if err1 != nil {
		if m.emptyStringNameTypeMap != nil {
			_, err2 := m.emptyStringNameTypeMap.getType(alias, customCategory)
			if err2 == nil {
				return alias, nil
			}
		}
		return "", err1
	}
	return fieldName, nil
}

// AliasFields returns SearchAttributes struct where each search attribute name is replaced with alias.
// If no replacement where made, it returns nil which means that original SearchAttributes struct should be used.
func AliasFields(mapper Mapper, searchAttributes *commonpb.SearchAttributes, namespace string) (*commonpb.SearchAttributes, error) {
	if len(searchAttributes.GetIndexedFields()) == 0 || mapper == nil {
		return nil, nil
	}

	newIndexedFields := make(map[string]*commonpb.Payload, len(searchAttributes.GetIndexedFields()))
	mapped := false
	for saName, saPayload := range searchAttributes.GetIndexedFields() {
		if !IsMappable(saName) {
			newIndexedFields[saName] = saPayload
			continue
		}

		aliasName, err := mapper.GetAlias(saName, namespace)
		if err != nil {
			if _, isInvalidArgument := err.(*serviceerror.InvalidArgument); isInvalidArgument {
				// Silently ignore serviceerror.InvalidArgument because it indicates unmapped field (alias was deleted, for example).
				// IMPORTANT: AliasFields should never return serviceerror.InvalidArgument because it is used by Poll API and the error
				// goes through up to SDK, which shutdowns worker when it receives serviceerror.InvalidArgument as poll response.
				continue
			}
			return nil, err
		}
		if aliasName != saName {
			mapped = true
		}
		newIndexedFields[aliasName] = saPayload
	}

	// If no field name was mapped, return nil to save on clone operation on caller side.
	if !mapped {
		return nil, nil
	}
	return &commonpb.SearchAttributes{IndexedFields: newIndexedFields}, nil
}

// UnaliasFields returns SearchAttributes struct where each search attribute alias is replaced with field name.
// If no replacement where made, it returns nil which means that original SearchAttributes struct should be used.
func UnaliasFields(mapper Mapper, searchAttributes *commonpb.SearchAttributes, namespace string) (*commonpb.SearchAttributes, error) {
	if len(searchAttributes.GetIndexedFields()) == 0 || mapper == nil {
		return nil, nil
	}

	newIndexedFields := make(map[string]*commonpb.Payload, len(searchAttributes.GetIndexedFields()))
	mapped := false
	for saName, saPayload := range searchAttributes.GetIndexedFields() {
		if !IsMappable(saName) {
			newIndexedFields[saName] = saPayload
			continue
		}

		fieldName, err := mapper.GetFieldName(saName, namespace)
		if err != nil {
			return nil, err
		}
		if fieldName != saName {
			mapped = true
		}
		newIndexedFields[fieldName] = saPayload
	}

	// If no alias was mapped, return nil to save on clone operation on caller side.
	if !mapped {
		return nil, nil
	}

	return &commonpb.SearchAttributes{IndexedFields: newIndexedFields}, nil
}
