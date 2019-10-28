// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to qvom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, qvETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package validator

import (
	"fmt"

	gen "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/definition"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

// SearchAttributesValidator is used to validate search attributes
type SearchAttributesValidator struct {
	logger log.Logger

	validSearchAttributes             dynamicconfig.MapPropertyFn
	searchAttributesNumberOfKeysLimit dynamicconfig.IntPropertyFnWithDomainFilter
	searchAttributesSizeOfValueLimit  dynamicconfig.IntPropertyFnWithDomainFilter
	searchAttributesTotalSizeLimit    dynamicconfig.IntPropertyFnWithDomainFilter
}

// NewSearchAttributesValidator create SearchAttributesValidator
func NewSearchAttributesValidator(
	logger log.Logger,
	validSearchAttributes dynamicconfig.MapPropertyFn,
	searchAttributesNumberOfKeysLimit dynamicconfig.IntPropertyFnWithDomainFilter,
	searchAttributesSizeOfValueLimit dynamicconfig.IntPropertyFnWithDomainFilter,
	searchAttributesTotalSizeLimit dynamicconfig.IntPropertyFnWithDomainFilter,
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
func (sv *SearchAttributesValidator) ValidateSearchAttributes(input *gen.SearchAttributes, domain string) error {
	if input == nil {
		return nil
	}

	// verify: number of keys <= limit
	fields := input.GetIndexedFields()
	lengthOfFields := len(fields)
	if lengthOfFields > sv.searchAttributesNumberOfKeysLimit(domain) {
		sv.logger.WithTags(tag.Number(int64(lengthOfFields)), tag.WorkflowDomainName(domain)).
			Error("number of keys in search attributes exceed limit")
		return &gen.BadRequestError{Message: fmt.Sprintf("number of keys %d exceed limit", lengthOfFields)}
	}

	totalSize := 0
	for key, val := range fields {
		// verify: key is whitelisted
		if !sv.isValidSearchAttributes(key) {
			sv.logger.WithTags(tag.ESKey(key), tag.WorkflowDomainName(domain)).
				Error("invalid search attribute")
			return &gen.BadRequestError{Message: fmt.Sprintf("%s is not valid search attribute", key)}
		}
		// verify: key is not system reserved
		if definition.IsSystemIndexedKey(key) {
			sv.logger.WithTags(tag.ESKey(key), tag.WorkflowDomainName(domain)).
				Error("illegal update of system reserved attribute")
			return &gen.BadRequestError{Message: fmt.Sprintf("%s is read-only Cadence reservered attribute", key)}
		}
		// verify: size of single value <= limit
		if len(val) > sv.searchAttributesSizeOfValueLimit(domain) {
			sv.logger.WithTags(tag.ESKey(key), tag.Number(int64(len(val))), tag.WorkflowDomainName(domain)).
				Error("value size of search attribute exceed limit")
			return &gen.BadRequestError{Message: fmt.Sprintf("size limit exceed for key %s", key)}
		}
		totalSize += len(key) + len(val)
	}

	// verify: total size <= limit
	if totalSize > sv.searchAttributesTotalSizeLimit(domain) {
		sv.logger.WithTags(tag.Number(int64(totalSize)), tag.WorkflowDomainName(domain)).
			Error("total size of search attributes exceed limit")
		return &gen.BadRequestError{Message: fmt.Sprintf("total size %d exceed limit", totalSize)}
	}

	return nil
}

// isValidSearchAttributes return true if key is registered
func (sv *SearchAttributesValidator) isValidSearchAttributes(key string) bool {
	validAttr := sv.validSearchAttributes()
	_, isValidKey := validAttr[key]
	return isValidKey
}
