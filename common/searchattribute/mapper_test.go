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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
)

type (
	TestMapper struct {
	}
)

func (t *TestMapper) GetAlias(fieldName string, namespace string) (string, error) {
	if fieldName == "wrong_field" {
		return "", serviceerror.NewInvalidArgument("unmapped field")
	}
	if namespace == "test" {
		return "alias_of_" + fieldName, nil
	}
	if namespace == "error" {
		return "", serviceerror.NewInternal("mapper error")
	}
	return fieldName, nil
}

func (t *TestMapper) GetFieldName(alias string, namespace string) (string, error) {
	if namespace == "test" {
		return strings.TrimPrefix(alias, "alias_of_"), nil
	}
	if namespace == "error" {
		return "", serviceerror.NewInternal("mapper error")
	}
	return alias, nil
}

func Test_ApplyAliases(t *testing.T) {
	sa := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"field1":      {Data: []byte("data1")},
			"field2":      {Data: []byte("data2")},
			"wrong_field": {Data: []byte("data23")},
		},
	}
	err := ApplyAliases(&TestMapper{}, sa, "error")
	assert.Error(t, err)
	var invalidArgumentErr *serviceerror.Internal
	assert.ErrorAs(t, err, &invalidArgumentErr)

	err = ApplyAliases(&TestMapper{}, sa, "namespace1")
	assert.NoError(t, err)
	assert.Len(t, sa.GetIndexedFields(), 2)
	assert.EqualValues(t, "data1", sa.GetIndexedFields()["field1"].GetData())
	assert.EqualValues(t, "data2", sa.GetIndexedFields()["field2"].GetData())

	err = ApplyAliases(&TestMapper{}, sa, "test")
	assert.NoError(t, err)
	assert.Len(t, sa.GetIndexedFields(), 2)
	assert.EqualValues(t, "data1", sa.GetIndexedFields()["alias_of_field1"].GetData())
	assert.EqualValues(t, "data2", sa.GetIndexedFields()["alias_of_field2"].GetData())
}

func Test_SubstituteAliases(t *testing.T) {
	sa := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"alias_of_field1": {Data: []byte("data1")},
			"alias_of_field2": {Data: []byte("data2")},
		},
	}
	err := SubstituteAliases(&TestMapper{}, sa, "error")
	assert.Error(t, err)
	var invalidArgumentErr *serviceerror.Internal
	assert.ErrorAs(t, err, &invalidArgumentErr)

	err = SubstituteAliases(&TestMapper{}, sa, "namespace1")
	assert.NoError(t, err)
	assert.Len(t, sa.GetIndexedFields(), 2)
	assert.EqualValues(t, "data1", sa.GetIndexedFields()["alias_of_field1"].GetData())
	assert.EqualValues(t, "data2", sa.GetIndexedFields()["alias_of_field2"].GetData())

	err = SubstituteAliases(&TestMapper{}, sa, "test")
	assert.NoError(t, err)
	assert.Len(t, sa.GetIndexedFields(), 2)
	assert.EqualValues(t, "data1", sa.GetIndexedFields()["field1"].GetData())
	assert.EqualValues(t, "data2", sa.GetIndexedFields()["field2"].GetData())
}
