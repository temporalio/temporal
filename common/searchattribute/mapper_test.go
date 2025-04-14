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

	"github.com/stretchr/testify/assert"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
)

func Test_AliasFields(t *testing.T) {
	mapperProvider := NewTestMapperProvider(&TestMapper{})

	sa := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"Field1":      {Data: []byte("data1")},
			"wrong_field": {Data: []byte("data23")}, // Wrong unknown name must be ignored.
		},
	}
	_, err := AliasFields(mapperProvider, sa, "error-namespace")
	assert.Error(t, err)
	var internalErr *serviceerror.Internal
	assert.ErrorAs(t, err, &internalErr)

	sa = &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"Field1":      {Data: []byte("data1")},
			"wrong_field": {Data: []byte("data23")}, // Wrong unknown name must be ignored.
		},
	}
	sb, err := AliasFields(mapperProvider, sa, "unknown-namespace")
	assert.NoError(t, err)
	assert.Equal(t, sa, sb)

	sa = &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"Field1":      {Data: []byte("data1")},
			"Field2":      {Data: []byte("data2")},
			"wrong_field": {Data: []byte("data23")}, // Wrong unknown name must be ignored.
		},
	}
	sb, err = AliasFields(mapperProvider, sa, "test-namespace")
	assert.NoError(t, err)
	assert.NotEqual(t, sa, sb)
	assert.Len(t, sb.GetIndexedFields(), 2)
	assert.EqualValues(t, "data1", sb.GetIndexedFields()["AliasForField1"].GetData())
	assert.EqualValues(t, "data2", sb.GetIndexedFields()["AliasForField2"].GetData())

	// Empty search attributes are not validated with mapper.
	sa = &commonpb.SearchAttributes{
		IndexedFields: nil,
	}
	sb, err = AliasFields(mapperProvider, sa, "error-namespace")
	assert.NoError(t, err)
	assert.Equal(t, sa, sb)
	sb, err = AliasFields(mapperProvider, sa, "unknown-namespace")
	assert.NoError(t, err)
	assert.Equal(t, sa, sb)

	// Pass through search attributes are not mapped.
	sa = &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"pass-through": {Data: []byte("data1")},
		},
	}
	sb, err = AliasFields(mapperProvider, sa, "test-namespace")
	assert.NoError(t, err)
	assert.Equal(t, sa, sb)
}

func Test_UnaliasFields(t *testing.T) {
	mapperProvider := NewTestMapperProvider(&TestMapper{})

	sa := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"AliasForField1": {Data: []byte("data1")},
		},
	}
	_, err := UnaliasFields(mapperProvider, sa, "error-namespace")
	assert.Error(t, err)
	var internalErr *serviceerror.Internal
	assert.ErrorAs(t, err, &internalErr)

	sa = &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"AliasForField1": {Data: []byte("data1")},
			"AliasForField2": {Data: []byte("data2")},
		},
	}
	_, err = UnaliasFields(mapperProvider, sa, "unknown-namespace")
	assert.Error(t, err)
	var invalidArgumentErr *serviceerror.InvalidArgument
	assert.ErrorAs(t, err, &invalidArgumentErr)

	sa = &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"AliasForField1": {Data: []byte("data1")},
			"AliasForField2": {Data: []byte("data2")},
		},
	}
	sa, err = UnaliasFields(mapperProvider, sa, "test-namespace")
	assert.NoError(t, err)
	assert.NotNil(t, sa)
	assert.Len(t, sa.GetIndexedFields(), 2)
	assert.EqualValues(t, "data1", sa.GetIndexedFields()["Field1"].GetData())
	assert.EqualValues(t, "data2", sa.GetIndexedFields()["Field2"].GetData())

	sa = &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"AliasForField1": {Data: []byte("data1")},
			"AliasForField2": {Data: []byte("data2")},
			"wrong_alias":    {Data: []byte("data3")},
		},
	}
	_, err = UnaliasFields(mapperProvider, sa, "test-namespace")
	assert.Error(t, err)
	assert.ErrorAs(t, err, &invalidArgumentErr)

	// Empty search attributes are not validated with mapper.
	sa = &commonpb.SearchAttributes{
		IndexedFields: nil,
	}
	sb, err := UnaliasFields(mapperProvider, sa, "error-namespace")
	assert.NoError(t, err)
	assert.Equal(t, sa, sb, "when there is nothin to unalias should return received attributes")

	sb, err = UnaliasFields(mapperProvider, sa, "unknown-namespace")
	assert.NoError(t, err)
	assert.Equal(t, sa, sb, "when there is nothin to unalias should return received attributes")

	// Pass through aliases are not substituted.
	sa = &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"pass-through": {Data: []byte("data1")},
		},
	}
	sb, err = UnaliasFields(mapperProvider, sa, "test-namespace")
	assert.NoError(t, err)
	assert.Equal(t, sb, sb, "when there is nothin to unalias should return received attributes")
}
