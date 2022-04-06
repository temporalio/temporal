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
	"testing"

	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payload"
)

type searchAttributesValidatorSuite struct {
	suite.Suite
}

func TestSearchAttributesValidatorSuite(t *testing.T) {
	s := &searchAttributesValidatorSuite{}
	suite.Run(t, s)
}

func (s *searchAttributesValidatorSuite) TestSearchAttributesValidate() {
	numOfKeysLimit := 2
	sizeOfValueLimit := 5
	sizeOfTotalLimit := 20

	saValidator := NewValidator(
		NewTestProvider(),
		nil,
		dynamicconfig.GetIntPropertyFilteredByNamespace(numOfKeysLimit),
		dynamicconfig.GetIntPropertyFilteredByNamespace(sizeOfValueLimit),
		dynamicconfig.GetIntPropertyFilteredByNamespace(sizeOfTotalLimit))

	namespace := "namespace"
	var attr *commonpb.SearchAttributes

	err := saValidator.Validate(attr, namespace, "")
	s.NoError(err)

	intPayload, err := payload.Encode(1)
	s.NoError(err)
	fields := map[string]*commonpb.Payload{
		"CustomIntField": intPayload,
	}
	attr = &commonpb.SearchAttributes{
		IndexedFields: fields,
	}
	err = saValidator.Validate(attr, namespace, "")
	s.NoError(err)

	fields = map[string]*commonpb.Payload{
		"CustomIntField":     intPayload,
		"CustomKeywordField": payload.EncodeString("keyword"),
		"CustomBoolField":    payload.EncodeString("true"),
	}
	attr.IndexedFields = fields
	err = saValidator.Validate(attr, namespace, "")
	s.Error(err)
	s.Equal("number of search attributes 3 exceeds limit 2", err.Error())

	fields = map[string]*commonpb.Payload{
		"InvalidKey": payload.EncodeString("1"),
	}
	attr.IndexedFields = fields
	err = saValidator.Validate(attr, namespace, "")
	s.Error(err)
	s.Equal("search attribute InvalidKey is not defined", err.Error())

	fields = map[string]*commonpb.Payload{
		"CustomTextField": payload.EncodeString("1"),
		"CustomBoolField": payload.EncodeString("123"),
	}
	attr.IndexedFields = fields
	err = saValidator.Validate(attr, namespace, "")
	s.Error(err)
	s.Equal("invalid value for search attribute CustomBoolField of type Bool: 123", err.Error())

	intArrayPayload, err := payload.Encode([]int{1, 2})
	s.NoError(err)
	fields = map[string]*commonpb.Payload{
		"CustomIntField": intArrayPayload,
	}
	attr.IndexedFields = fields
	err = saValidator.Validate(attr, namespace, "")
	s.NoError(err)

	fields = map[string]*commonpb.Payload{
		"StartTime": intPayload,
	}
	attr.IndexedFields = fields
	err = saValidator.Validate(attr, namespace, "")
	s.Error(err)
	s.Equal("StartTime attribute can't be set in SearchAttributes", err.Error())
}

func (s *searchAttributesValidatorSuite) TestSearchAttributesValidate_Mapper() {
	numOfKeysLimit := 2
	sizeOfValueLimit := 5
	sizeOfTotalLimit := 20

	saValidator := NewValidator(
		NewTestProvider(),
		&TestMapper{},
		dynamicconfig.GetIntPropertyFilteredByNamespace(numOfKeysLimit),
		dynamicconfig.GetIntPropertyFilteredByNamespace(sizeOfValueLimit),
		dynamicconfig.GetIntPropertyFilteredByNamespace(sizeOfTotalLimit))

	namespace := "test-namespace"
	var attr *commonpb.SearchAttributes

	err := saValidator.Validate(attr, namespace, "")
	s.Nil(err)

	intPayload, err := payload.Encode(1)
	s.NoError(err)
	fields := map[string]*commonpb.Payload{
		"alias_of_CustomIntField": intPayload,
	}
	attr = &commonpb.SearchAttributes{
		IndexedFields: fields,
	}
	err = saValidator.Validate(attr, namespace, "")
	s.NoError(err)

	err = saValidator.Validate(attr, "error-namespace", "")
	s.Error(err)
	s.EqualError(err, "mapper error")

	fields = map[string]*commonpb.Payload{
		"CustomIntField": intPayload,
	}
	attr = &commonpb.SearchAttributes{
		IndexedFields: fields,
	}
	err = saValidator.Validate(attr, "test-namespace", "")
	s.NoError(err)

	fields = map[string]*commonpb.Payload{
		"alias_of_InvalidKey": payload.EncodeString("1"),
	}
	attr.IndexedFields = fields
	err = saValidator.Validate(attr, namespace, "")
	s.Error(err)
	s.Equal("search attribute alias_of_InvalidKey is not defined", err.Error())

	fields = map[string]*commonpb.Payload{
		"alias_of_CustomTextField": payload.EncodeString("1"),
		"alias_of_CustomBoolField": payload.EncodeString("123"),
	}
	attr.IndexedFields = fields
	err = saValidator.Validate(attr, namespace, "")
	s.Error(err)
	s.Equal("invalid value for search attribute alias_of_CustomBoolField of type Bool: 123", err.Error())
}

func (s *searchAttributesValidatorSuite) TestSearchAttributesValidateSize() {
	numOfKeysLimit := 2
	sizeOfValueLimit := 5
	sizeOfTotalLimit := 20

	saValidator := NewValidator(
		NewTestProvider(),
		nil,
		dynamicconfig.GetIntPropertyFilteredByNamespace(numOfKeysLimit),
		dynamicconfig.GetIntPropertyFilteredByNamespace(sizeOfValueLimit),
		dynamicconfig.GetIntPropertyFilteredByNamespace(sizeOfTotalLimit))

	namespace := "namespace"

	fields := map[string]*commonpb.Payload{
		"CustomKeywordField": payload.EncodeString("123456"),
	}
	attr := &commonpb.SearchAttributes{
		IndexedFields: fields,
	}

	attr.IndexedFields = fields
	err := saValidator.ValidateSize(attr, namespace)
	s.Error(err)
	s.Equal("search attribute CustomKeywordField value of size 8: exceeds size limit 5", err.Error())

	fields = map[string]*commonpb.Payload{
		"CustomKeywordField": payload.EncodeString("123"),
		"CustomTextField":    payload.EncodeString("12"),
	}
	attr.IndexedFields = fields
	err = saValidator.ValidateSize(attr, namespace)
	s.Error(err)
	s.Equal("total size of search attributes 106: exceeds size limit 20", err.Error())
}
