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
	"testing"

	"github.com/stretchr/testify/assert"
	enumspb "go.temporal.io/api/enums/v1"
)

func Test_IsValid(t *testing.T) {
	assert := assert.New(t)
	typeMap := NameTypeMap{customSearchAttributes: map[string]enumspb.IndexedValueType{
		"key1": enumspb.INDEXED_VALUE_TYPE_TEXT,
		"key2": enumspb.INDEXED_VALUE_TYPE_INT,
		"key3": enumspb.INDEXED_VALUE_TYPE_BOOL,
	}}

	isDefined := typeMap.IsDefined("RunId")
	assert.True(isDefined)
	isDefined = typeMap.IsDefined("TemporalChangeVersion")
	assert.True(isDefined)
	isDefined = typeMap.IsDefined("key1")
	assert.True(isDefined)

	isDefined = NameTypeMap{}.IsDefined("key1")
	assert.False(isDefined)
	isDefined = typeMap.IsDefined("key4")
	assert.False(isDefined)
	isDefined = typeMap.IsDefined("NamespaceId")
	assert.False(isDefined)
}

func Test_GetType(t *testing.T) {
	assert := assert.New(t)
	typeMap := NameTypeMap{customSearchAttributes: map[string]enumspb.IndexedValueType{
		"key1": enumspb.INDEXED_VALUE_TYPE_TEXT,
		"key2": enumspb.INDEXED_VALUE_TYPE_INT,
		"key3": enumspb.INDEXED_VALUE_TYPE_BOOL,
	}}

	ivt, err := typeMap.GetType("key1")
	assert.NoError(err)
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_TEXT, ivt)
	ivt, err = typeMap.GetType("key2")
	assert.NoError(err)
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_INT, ivt)
	ivt, err = typeMap.GetType("key3")
	assert.NoError(err)
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_BOOL, ivt)
	ivt, err = typeMap.GetType("RunId")
	assert.NoError(err)
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_KEYWORD, ivt)
	ivt, err = typeMap.GetType("TemporalChangeVersion")
	assert.NoError(err)
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_KEYWORD, ivt)
	ivt, err = typeMap.GetType("NamespaceId")
	assert.Error(err)
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, ivt)

	ivt, err = NameTypeMap{}.GetType("key1")
	assert.Error(err)
	assert.True(errors.Is(err, ErrInvalidName))
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, ivt)
	ivt, err = typeMap.GetType("key4")
	assert.Error(err)
	assert.True(errors.Is(err, ErrInvalidName))
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, ivt)
}
