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

package validator

import (
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/service/dynamicconfig"
)

// SearchAttributesValidator is used to validate search attributes
type SearchAttributesValidator struct {
	logger log.Logger

	validSearchAttributes             dynamicconfig.MapPropertyFn
	searchAttributesNumberOfKeysLimit dynamicconfig.IntPropertyFnWithNamespaceFilter
	searchAttributesSizeOfValueLimit  dynamicconfig.IntPropertyFnWithNamespaceFilter
	searchAttributesTotalSizeLimit    dynamicconfig.IntPropertyFnWithNamespaceFilter
}

// NewSearchAttributesValidator create SearchAttributesValidator
func NewSearchAttributesValidator(
	logger log.Logger,
	validSearchAttributes dynamicconfig.MapPropertyFn,
	searchAttributesNumberOfKeysLimit dynamicconfig.IntPropertyFnWithNamespaceFilter,
	searchAttributesSizeOfValueLimit dynamicconfig.IntPropertyFnWithNamespaceFilter,
	searchAttributesTotalSizeLimit dynamicconfig.IntPropertyFnWithNamespaceFilter,
) *SearchAttributesValidator {
	return &SearchAttributesValidator{
		logger:                            logger,
		validSearchAttributes:             validSearchAttributes,
		searchAttributesNumberOfKeysLimit: searchAttributesNumberOfKeysLimit,
		searchAttributesSizeOfValueLimit:  searchAttributesSizeOfValueLimit,
		searchAttributesTotalSizeLimit:    searchAttributesTotalSizeLimit,
	}
}

// ValidateSearchAttributes validate search attributes are valid for writing and not exceed limits
func (sv *SearchAttributesValidator) ValidateSearchAttributes(input *commonpb.SearchAttributes, namespace string) error {
	if input == nil {
		return nil
	}

	// verify: number of keys <= limit
	lengthOfFields := len(input.GetIndexedFields())
	if lengthOfFields > sv.searchAttributesNumberOfKeysLimit(namespace) {
		sv.logger.WithTags(tag.Number(int64(lengthOfFields)), tag.WorkflowNamespace(namespace)).
			Error("number of keys in search attributes exceed limit")
		return serviceerror.NewInvalidArgument(fmt.Sprintf("number of keys %d exceed limit", lengthOfFields))
	}

	totalSize := 0
	validAttr := sv.validSearchAttributes()
	for key, val := range input.GetIndexedFields() {
		// verify: key is whitelisted
		if !sv.isValidSearchAttributeName(key, validAttr) {
			sv.logger.WithTags(tag.ESKey(key), tag.WorkflowNamespace(namespace)).
				Error("invalid search attribute key")
			return serviceerror.NewInvalidArgument(fmt.Sprintf("%s is not valid search attribute key", key))
		}
		// verify: value has the correct type
		if !sv.isValidSearchAttributeValue(val) {
			var invalidValue interface{}
			if err := payload.Decode(val, &invalidValue); err != nil {
				invalidValue = fmt.Sprintf("value from %q", val.String())
			}

			sv.logger.WithTags(tag.ESKey(key), tag.Value(invalidValue), tag.WorkflowNamespace(namespace)).
				Error("invalid search attribute value")
			return serviceerror.NewInvalidArgument(fmt.Sprintf("%q is not a valid for search attribute %s", invalidValue, key))
		}
		// verify: key is not system reserved
		if definition.IsSystemIndexedKey(key) {
			sv.logger.WithTags(tag.ESKey(key), tag.WorkflowNamespace(namespace)).
				Error("illegal update of system reserved attribute")
			return serviceerror.NewInvalidArgument(fmt.Sprintf("%s is read-only Temporal reservered attribute", key))
		}
		// verify: size of single value <= limit
		dataSize := len(val.GetData())
		if dataSize > sv.searchAttributesSizeOfValueLimit(namespace) {
			sv.logger.WithTags(tag.ESKey(key), tag.Number(int64(dataSize)), tag.WorkflowNamespace(namespace)).
				Error("value size of search attribute exceed limit")
			return serviceerror.NewInvalidArgument(fmt.Sprintf("size limit exceed for key %s", key))
		}
		totalSize += len(key) + dataSize
	}

	// verify: total size <= limit
	if totalSize > sv.searchAttributesTotalSizeLimit(namespace) {
		sv.logger.WithTags(tag.Number(int64(totalSize)), tag.WorkflowNamespace(namespace)).
			Error("total size of search attributes exceed limit")
		return serviceerror.NewInvalidArgument(fmt.Sprintf("total size %d exceed limit", totalSize))
	}

	return nil
}

// isValidSearchAttributeName return true if key is registered
func (sv *SearchAttributesValidator) isValidSearchAttributeName(searchAttributeName string, validSearchAttributes map[string]interface{}) bool {
	_, isValidKey := validSearchAttributes[searchAttributeName]
	return isValidKey
}

// isValidSearchAttributeValue return true if value has the correct representation for the attribute key
func (sv *SearchAttributesValidator) isValidSearchAttributeValue(value *commonpb.Payload) bool {
	_, err := searchattribute.DecodeValue(value)
	return err == nil
}
