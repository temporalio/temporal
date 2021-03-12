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

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payload"
)

// Validator is used to validate search attributes
type Validator struct {
	logger log.Logger

	validSearchAttributes             dynamicconfig.MapPropertyFn
	searchAttributesNumberOfKeysLimit dynamicconfig.IntPropertyFnWithNamespaceFilter
	searchAttributesSizeOfValueLimit  dynamicconfig.IntPropertyFnWithNamespaceFilter
	searchAttributesTotalSizeLimit    dynamicconfig.IntPropertyFnWithNamespaceFilter
}

// NewValidator create Validator
func NewValidator(
	logger log.Logger,
	validSearchAttributes dynamicconfig.MapPropertyFn,
	searchAttributesNumberOfKeysLimit dynamicconfig.IntPropertyFnWithNamespaceFilter,
	searchAttributesSizeOfValueLimit dynamicconfig.IntPropertyFnWithNamespaceFilter,
	searchAttributesTotalSizeLimit dynamicconfig.IntPropertyFnWithNamespaceFilter,
) *Validator {
	return &Validator{
		logger:                            logger,
		validSearchAttributes:             validSearchAttributes,
		searchAttributesNumberOfKeysLimit: searchAttributesNumberOfKeysLimit,
		searchAttributesSizeOfValueLimit:  searchAttributesSizeOfValueLimit,
		searchAttributesTotalSizeLimit:    searchAttributesTotalSizeLimit,
	}
}

// Validate validate search attributes are valid for writing and not exceed limits
func (v *Validator) ValidateAndLog(searchAttributes *commonpb.SearchAttributes, namespace string) error {
	err := v.Validate(searchAttributes, namespace)
	if err != nil {
		v.logger.Error("Error while validating search attributes.", tag.Error(err), tag.WorkflowNamespace(namespace))
	}
	return err
}

// Validate validate search attributes are valid for writing and not exceed limits
func (v *Validator) Validate(searchAttributes *commonpb.SearchAttributes, namespace string) error {
	if searchAttributes == nil {
		return nil
	}

	// verify: number of keys <= limit
	lengthOfFields := len(searchAttributes.GetIndexedFields())
	if lengthOfFields > v.searchAttributesNumberOfKeysLimit(namespace) {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("number of search attributes %d exceeds limit %d", lengthOfFields, v.searchAttributesNumberOfKeysLimit(namespace)))
	}

	totalSize := 0
	typeMap, err := BuildTypeMap(v.validSearchAttributes)
	if err != nil {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("unable to parse search attributes from config: %v", err))
	}

	for saName, saPayload := range searchAttributes.GetIndexedFields() {
		// verify: saName is whitelisted
		saType, err := GetType(saName, typeMap)
		if err != nil {
			return serviceerror.NewInvalidArgument(fmt.Sprintf("%s is not a valid search attribute name", saName))
		}
		_, err = DecodeValue(saPayload, saType)
		if err != nil {
			var invalidValue interface{}
			if err := payload.Decode(saPayload, &invalidValue); err != nil {
				invalidValue = fmt.Sprintf("value from <%s>", saPayload.String())
			}
			return serviceerror.NewInvalidArgument(fmt.Sprintf("%v is not a valid value for search attribute %s", invalidValue, saName))
		}

		// verify: saName is not system reserved
		if IsSystem(saName) {
			return serviceerror.NewInvalidArgument(fmt.Sprintf("%s is read-only Temporal reserved search attribute", saName))
		}
		// verify: size of single value <= limit
		dataSize := len(saPayload.GetData())
		if dataSize > v.searchAttributesSizeOfValueLimit(namespace) {
			return serviceerror.NewInvalidArgument(fmt.Sprintf("search attribute %s exceeds size limit %d", saName, v.searchAttributesSizeOfValueLimit(namespace)))
		}
		totalSize += len(saName) + dataSize
	}

	// verify: total size <= limit
	if totalSize > v.searchAttributesTotalSizeLimit(namespace) {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("total search attributes size %d exceeds limit %d", totalSize, v.searchAttributesTotalSizeLimit(namespace)))
	}

	return nil
}
