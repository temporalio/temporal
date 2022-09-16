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
	"errors"
	"fmt"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payload"
)

type (
	// Validator is used to validate search attributes
	Validator struct {
		searchAttributesProvider          Provider
		searchAttributesMapper            Mapper
		searchAttributesNumberOfKeysLimit dynamicconfig.IntPropertyFnWithNamespaceFilter
		searchAttributesSizeOfValueLimit  dynamicconfig.IntPropertyFnWithNamespaceFilter
		searchAttributesTotalSizeLimit    dynamicconfig.IntPropertyFnWithNamespaceFilter
	}
)

var (
	ErrExceedSizeLimit = errors.New("exceeds size limit")
)

// NewValidator create Validator
func NewValidator(
	searchAttributesProvider Provider,
	searchAttributesMapper Mapper,
	searchAttributesNumberOfKeysLimit dynamicconfig.IntPropertyFnWithNamespaceFilter,
	searchAttributesSizeOfValueLimit dynamicconfig.IntPropertyFnWithNamespaceFilter,
	searchAttributesTotalSizeLimit dynamicconfig.IntPropertyFnWithNamespaceFilter,
) *Validator {
	return &Validator{
		searchAttributesProvider:          searchAttributesProvider,
		searchAttributesMapper:            searchAttributesMapper,
		searchAttributesNumberOfKeysLimit: searchAttributesNumberOfKeysLimit,
		searchAttributesSizeOfValueLimit:  searchAttributesSizeOfValueLimit,
		searchAttributesTotalSizeLimit:    searchAttributesTotalSizeLimit,
	}
}

func (v *Validator) getAlias(saFieldName string, namespace string) (string, error) {
	var err error
	saAlias := saFieldName
	if IsMappable(saFieldName) && v.searchAttributesMapper != nil {
		saAlias, err = v.searchAttributesMapper.GetAlias(saFieldName, namespace)
	}
	return saAlias, err
}

// Generates a validation error replacing `{saAlias}` with the alias of `saFieldName`.
func (v *Validator) validationError(msg string, saFieldName string, namespace string) error {
	saAlias, err := v.getAlias(saFieldName, namespace)
	if err != nil {
		return err
	}
	return serviceerror.NewInvalidArgument(
		strings.ReplaceAll(msg, "{saAlias}", saAlias),
	)
}

// Validate search attributes are valid for writing.
// The search attributes must be unaliased before calling validation.
func (v *Validator) Validate(searchAttributes *commonpb.SearchAttributes, namespace string, indexName string) error {
	if searchAttributes == nil {
		return nil
	}

	lengthOfFields := len(searchAttributes.GetIndexedFields())
	if lengthOfFields > v.searchAttributesNumberOfKeysLimit(namespace) {
		return serviceerror.NewInvalidArgument(
			fmt.Sprintf(
				"number of search attributes %d exceeds limit %d",
				lengthOfFields,
				v.searchAttributesNumberOfKeysLimit(namespace),
			),
		)
	}

	saTypeMap, err := v.searchAttributesProvider.GetSearchAttributes(indexName, false)
	if err != nil {
		return serviceerror.NewInvalidArgument(
			fmt.Sprintf("unable to get search attributes from cluster metadata: %v", err),
		)
	}

	for saFieldName, saPayload := range searchAttributes.GetIndexedFields() {
		// user search attribute cannot be a system search attribute
		if _, err = saTypeMap.getType(saFieldName, systemCategory); err == nil {
			return serviceerror.NewInvalidArgument(
				fmt.Sprintf("%s attribute can't be set in SearchAttributes", saFieldName),
			)
		}

		saType, err := saTypeMap.getType(saFieldName, customCategory|predefinedCategory)
		if err != nil {
			if errors.Is(err, ErrInvalidName) {
				return v.validationError(
					"search attribute {saAlias} is not defined",
					saFieldName,
					namespace,
				)
			}
			return v.validationError(
				fmt.Sprintf("unable to get {saAlias} search attribute type: %v", err),
				saFieldName,
				namespace,
			)
		}

		if _, err = DecodeValue(saPayload, saType); err != nil {
			var invalidValue interface{}
			if err = payload.Decode(saPayload, &invalidValue); err != nil {
				invalidValue = fmt.Sprintf("value from <%s>", saPayload.String())
			}
			return v.validationError(
				fmt.Sprintf(
					"invalid value for search attribute {saAlias} of type %s: %v",
					saType,
					invalidValue,
				),
				saFieldName,
				namespace,
			)
		}
	}
	return nil
}

// ValidateSize validate search attributes are valid for writing and not exceed limits.
// The search attributes must be unaliased before calling validation.
func (v *Validator) ValidateSize(searchAttributes *commonpb.SearchAttributes, namespace string) error {
	if searchAttributes == nil {
		return nil
	}

	for saFieldName, saPayload := range searchAttributes.GetIndexedFields() {
		if len(saPayload.GetData()) > v.searchAttributesSizeOfValueLimit(namespace) {
			saAlias, err := v.getAlias(saFieldName, namespace)
			if err != nil {
				return err
			}
			return fmt.Errorf(
				"search attribute %s value of size %d: %w %d",
				saAlias,
				len(saPayload.GetData()),
				ErrExceedSizeLimit,
				v.searchAttributesSizeOfValueLimit(namespace),
			)
		}
	}

	if searchAttributes.Size() > v.searchAttributesTotalSizeLimit(namespace) {
		return fmt.Errorf(
			"total size of search attributes %d: %w %d",
			searchAttributes.Size(),
			ErrExceedSizeLimit,
			v.searchAttributesTotalSizeLimit(namespace),
		)
	}

	return nil
}
