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
	"testing"

	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/temporal-proto/common/v1"

	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/service/dynamicconfig"
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
		dynamicconfig.GetIntPropertyFilteredByNamespace(numOfKeysLimit),
		dynamicconfig.GetIntPropertyFilteredByNamespace(sizeOfValueLimit),
		dynamicconfig.GetIntPropertyFilteredByNamespace(sizeOfTotalLimit))

	namespace := "namespace"
	var attr *commonpb.SearchAttributes

	err := validator.ValidateSearchAttributes(attr, namespace)
	s.Nil(err)

	intPayload, err := payload.Encode(1)
	s.NoError(err)
	fields := map[string]*commonpb.Payload{
		"CustomIntField": intPayload,
	}
	attr = &commonpb.SearchAttributes{
		IndexedFields: fields,
	}
	err = validator.ValidateSearchAttributes(attr, namespace)
	s.Nil(err)

	fields = map[string]*commonpb.Payload{
		"CustomIntField":     intPayload,
		"CustomKeywordField": payload.EncodeString("keyword"),
		"CustomBoolField":    payload.EncodeString("true"),
	}
	attr.IndexedFields = fields
	err = validator.ValidateSearchAttributes(attr, namespace)
	s.Equal("number of keys 3 exceed limit", err.Error())

	fields = map[string]*commonpb.Payload{
		"InvalidKey": payload.EncodeString("1"),
	}
	attr.IndexedFields = fields
	err = validator.ValidateSearchAttributes(attr, namespace)
	s.Equal("InvalidKey is not valid search attribute key", err.Error())

	fields = map[string]*commonpb.Payload{
		"CustomStringField": payload.EncodeString("1"),
		"CustomBoolField":   payload.EncodeString("123"),
	}
	attr.IndexedFields = fields
	err = validator.ValidateSearchAttributes(attr, namespace)
	s.Equal("123 is not a valid search attribute value for key CustomBoolField", err.Error())

	intArrayPayload, err := payload.Encode([]int{1, 2})
	s.NoError(err)
	fields = map[string]*commonpb.Payload{
		"CustomIntField": intArrayPayload,
	}
	attr.IndexedFields = fields
	err = validator.ValidateSearchAttributes(attr, namespace)
	s.NoError(err)

	fields = map[string]*commonpb.Payload{
		"StartTime": intPayload,
	}
	attr.IndexedFields = fields
	err = validator.ValidateSearchAttributes(attr, namespace)
	s.Equal("StartTime is read-only Temporal reservered attribute", err.Error())

	fields = map[string]*commonpb.Payload{
		"CustomKeywordField": payload.EncodeString("123456"),
	}
	attr.IndexedFields = fields
	err = validator.ValidateSearchAttributes(attr, namespace)
	s.Equal("size limit exceed for key CustomKeywordField", err.Error())

	fields = map[string]*commonpb.Payload{
		"CustomKeywordField": payload.EncodeString("123"),
		"CustomStringField":  payload.EncodeString("12"),
	}
	attr.IndexedFields = fields
	err = validator.ValidateSearchAttributes(attr, namespace)
	s.Equal("total size 44 exceed limit", err.Error())
}
