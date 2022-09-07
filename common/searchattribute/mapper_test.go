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
		// This error must be always ignored.
		return "", serviceerror.NewInvalidArgument("unmapped field")
	}
	if namespace == "error-namespace" {
		return "", serviceerror.NewInternal("mapper error")
	} else if namespace == "test-namespace" {
		if fieldName == "pass-through" {
			return fieldName, nil
		}

		return "alias_of_" + fieldName, nil
	}

	// This error must be always ignored.
	return "", serviceerror.NewInvalidArgument("unknown namespace")
}

func (t *TestMapper) GetFieldName(alias string, namespace string) (string, error) {
	if alias == "wrong_alias" {
		// This error must be always ignored.
		return "", serviceerror.NewInvalidArgument("unmapped alias")
	}
	if namespace == "error-namespace" {
		return "", serviceerror.NewInternal("mapper error")
	} else if namespace == "test-namespace" {
		if alias == "pass-through" {
			return alias, nil
		}
		return strings.TrimPrefix(alias, "alias_of_"), nil
	}
	return "", serviceerror.NewInvalidArgument("unknown namespace")
}

func Test_AliasFields(t *testing.T) {
	sa := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"field1":      {Data: []byte("data1")},
			"wrong_field": {Data: []byte("data23")}, // Wrong unknown name must be ignored.
		},
	}
	_, err := AliasFields(&TestMapper{}, sa, "error-namespace")
	assert.Error(t, err)
	var internalErr *serviceerror.Internal
	assert.ErrorAs(t, err, &internalErr)

	sa = &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"field1":      {Data: []byte("data1")},
			"wrong_field": {Data: []byte("data23")}, // Wrong unknown name must be ignored.
		},
	}
	sa, err = AliasFields(&TestMapper{}, sa, "unknown-namespace")
	assert.NoError(t, err)
	assert.Nil(t, sa)

	sa = &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"field1":      {Data: []byte("data1")},
			"field2":      {Data: []byte("data2")},
			"wrong_field": {Data: []byte("data23")}, // Wrong unknown name must be ignored.
		},
	}
	sa, err = AliasFields(&TestMapper{}, sa, "test-namespace")
	assert.NoError(t, err)
	assert.NotNil(t, sa)
	assert.Len(t, sa.GetIndexedFields(), 2)
	assert.EqualValues(t, "data1", sa.GetIndexedFields()["alias_of_field1"].GetData())
	assert.EqualValues(t, "data2", sa.GetIndexedFields()["alias_of_field2"].GetData())

	// Empty search attributes are not validated with mapper.
	sa = &commonpb.SearchAttributes{
		IndexedFields: nil,
	}
	sa, err = AliasFields(&TestMapper{}, sa, "error-namespace")
	assert.NoError(t, err)
	assert.Nil(t, sa)
	sa, err = AliasFields(&TestMapper{}, sa, "unknown-namespace")
	assert.NoError(t, err)
	assert.Nil(t, sa)

	// Pass through search attributes are not mapped.
	sa = &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"pass-through": {Data: []byte("data1")},
		},
	}
	sa, err = AliasFields(&TestMapper{}, sa, "test-namespace")
	assert.NoError(t, err)
	assert.Nil(t, sa)
}

func Test_UnaliasFields(t *testing.T) {
	sa := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"alias_of_field1": {Data: []byte("data1")},
		},
	}
	_, err := UnaliasFields(&TestMapper{}, sa, "error-namespace")
	assert.Error(t, err)
	var internalErr *serviceerror.Internal
	assert.ErrorAs(t, err, &internalErr)

	sa = &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"alias_of_field1": {Data: []byte("data1")},
			"alias_of_field2": {Data: []byte("data2")},
		},
	}
	_, err = UnaliasFields(&TestMapper{}, sa, "unknown-namespace")
	assert.Error(t, err)
	var invalidArgumentErr *serviceerror.InvalidArgument
	assert.ErrorAs(t, err, &invalidArgumentErr)

	sa = &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"alias_of_field1": {Data: []byte("data1")},
			"alias_of_field2": {Data: []byte("data2")},
		},
	}
	sa, err = UnaliasFields(&TestMapper{}, sa, "test-namespace")
	assert.NoError(t, err)
	assert.NotNil(t, sa)
	assert.Len(t, sa.GetIndexedFields(), 2)
	assert.EqualValues(t, "data1", sa.GetIndexedFields()["field1"].GetData())
	assert.EqualValues(t, "data2", sa.GetIndexedFields()["field2"].GetData())

	sa = &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"alias_of_field1": {Data: []byte("data1")},
			"alias_of_field2": {Data: []byte("data2")},
			"wrong_alias":     {Data: []byte("data3")},
		},
	}
	_, err = UnaliasFields(&TestMapper{}, sa, "test-namespace")
	assert.Error(t, err)
	assert.ErrorAs(t, err, &invalidArgumentErr)

	// Empty search attributes are not validated with mapper.
	sa = &commonpb.SearchAttributes{
		IndexedFields: nil,
	}
	sa, err = UnaliasFields(&TestMapper{}, sa, "error-namespace")
	assert.NoError(t, err)
	assert.Nil(t, sa)
	sa, err = UnaliasFields(&TestMapper{}, sa, "unknown-namespace")
	assert.NoError(t, err)
	assert.Nil(t, sa)

	// Pass through aliases are not substituted.
	sa = &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"pass-through": {Data: []byte("data1")},
		},
	}
	sa, err = UnaliasFields(&TestMapper{}, sa, "test-namespace")
	assert.NoError(t, err)
	assert.Nil(t, sa)
}
