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
	"testing"

	"github.com/stretchr/testify/suite"

	gen "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/definition"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

type searchAttributesValidatorSuite struct {
	suite.Suite
}

func TestSearchAttributesValidatorSuite(t *testing.T) {
	s := new(searchAttributesValidatorSuite)
	suite.Run(t, s)
}

func (s *searchAttributesValidatorSuite) TestValidateSearchAttributes() {
	numOfKeysLimit := 2
	sizeOfValueLimit := 5
	sizeOfTotalLimit := 20

	validator := NewSearchAttributesValidator(log.NewNoop(),
		dynamicconfig.GetMapPropertyFn(definition.GetDefaultIndexedKeys()),
		dynamicconfig.GetIntPropertyFilteredByDomain(numOfKeysLimit),
		dynamicconfig.GetIntPropertyFilteredByDomain(sizeOfValueLimit),
		dynamicconfig.GetIntPropertyFilteredByDomain(sizeOfTotalLimit))

	domain := "domain"
	var attr *gen.SearchAttributes

	err := validator.ValidateSearchAttributes(attr, domain)
	s.Nil(err)

	fields := map[string][]byte{
		"CustomIntField": []byte(`1`),
	}
	attr = &gen.SearchAttributes{
		IndexedFields: fields,
	}
	err = validator.ValidateSearchAttributes(attr, domain)
	s.Nil(err)

	fields = map[string][]byte{
		"CustomIntField":     []byte(`1`),
		"CustomKeywordField": []byte(`keyword`),
		"CustomBoolField":    []byte(`true`),
	}
	attr.IndexedFields = fields
	err = validator.ValidateSearchAttributes(attr, domain)
	s.Equal("BadRequestError{Message: number of keys 3 exceed limit}", err.Error())

	fields = map[string][]byte{
		"InvalidKey": []byte(`1`),
	}
	attr.IndexedFields = fields
	err = validator.ValidateSearchAttributes(attr, domain)
	s.Equal(`BadRequestError{Message: InvalidKey is not valid search attribute}`, err.Error())

	fields = map[string][]byte{
		"StartTime": []byte(`1`),
	}
	attr.IndexedFields = fields
	err = validator.ValidateSearchAttributes(attr, domain)
	s.Equal(`BadRequestError{Message: StartTime is read-only Cadence reservered attribute}`, err.Error())

	fields = map[string][]byte{
		"CustomKeywordField": []byte(`123456`),
	}
	attr.IndexedFields = fields
	err = validator.ValidateSearchAttributes(attr, domain)
	s.Equal(`BadRequestError{Message: size limit exceed for key CustomKeywordField}`, err.Error())

	fields = map[string][]byte{
		"CustomKeywordField": []byte(`123`),
		"CustomStringField":  []byte(`12`),
	}
	attr.IndexedFields = fields
	err = validator.ValidateSearchAttributes(attr, domain)
	s.Equal(`BadRequestError{Message: total size 40 exceed limit}`, err.Error())
}
