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

	noopMapper struct{}

	MapperProvider interface {
		GetMapper(nsName namespace.Name) (Mapper, error)
	}

	mapperProviderImpl struct {
		customMapper Mapper
	}
)

var _ Mapper = (*noopMapper)(nil)
var _ Mapper = (*namespace.CustomSearchAttributesMapper)(nil)
var _ MapperProvider = (*mapperProviderImpl)(nil)

func (m *noopMapper) GetAlias(fieldName string, _ string) (string, error) {
	return fieldName, nil
}

func (m *noopMapper) GetFieldName(alias string, _ string) (string, error) {
	return alias, nil
}

func NewMapperProvider(
	customMapper Mapper,
) MapperProvider {
	return &mapperProviderImpl{
		customMapper: customMapper,
	}
}

func (m *mapperProviderImpl) GetMapper(_ namespace.Name) (Mapper, error) {
	return m.customMapper, nil
}

// AliasFields returns SearchAttributes struct where each search attribute name is replaced with alias.
// If no replacement where made, it returns nil which means that original SearchAttributes struct should be used.
func AliasFields(
	mapperProvider MapperProvider,
	searchAttributes *commonpb.SearchAttributes,
	namespaceName string,
) (*commonpb.SearchAttributes, error) {
	mapper, err := mapperProvider.GetMapper(namespace.Name(namespaceName))
	if err != nil {
		return nil, err
	}

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

		aliasName, err := mapper.GetAlias(saName, namespaceName)
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
func UnaliasFields(
	mapperProvider MapperProvider,
	searchAttributes *commonpb.SearchAttributes,
	namespaceName string,
) (*commonpb.SearchAttributes, error) {
	mapper, err := mapperProvider.GetMapper(namespace.Name(namespaceName))
	if err != nil {
		return nil, err
	}

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

		fieldName, err := mapper.GetFieldName(saName, namespaceName)
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
