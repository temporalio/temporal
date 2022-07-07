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

func Test_Encode_Success(t *testing.T) {
	assert := assert.New(t)

	sa, err := Encode(map[string]interface{}{
		"key1": "val1",
		"key2": 2,
		"key3": true,
		"key4": nil,
		"key5": []string{"val2", "val3"},
		"key6": []string{},
	}, &NameTypeMap{customSearchAttributes: map[string]enumspb.IndexedValueType{
		"key1": enumspb.INDEXED_VALUE_TYPE_TEXT,
		"key2": enumspb.INDEXED_VALUE_TYPE_INT,
		"key3": enumspb.INDEXED_VALUE_TYPE_BOOL,
		"key4": enumspb.INDEXED_VALUE_TYPE_DOUBLE,
		"key5": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"key6": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	}})

	assert.NoError(err)
	assert.Len(sa.IndexedFields, 6)
	assert.Equal(`"val1"`, string(sa.IndexedFields["key1"].GetData()))
	assert.Equal("Text", string(sa.IndexedFields["key1"].GetMetadata()["type"]))
	assert.Equal("2", string(sa.IndexedFields["key2"].GetData()))
	assert.Equal("Int", string(sa.IndexedFields["key2"].GetMetadata()["type"]))
	assert.Equal("true", string(sa.IndexedFields["key3"].GetData()))
	assert.Equal("Bool", string(sa.IndexedFields["key3"].GetMetadata()["type"]))
	assert.Equal("", string(sa.IndexedFields["key4"].GetData()))
	assert.Equal("Double", string(sa.IndexedFields["key4"].GetMetadata()["type"]))
	assert.Equal("binary/null", string(sa.IndexedFields["key4"].GetMetadata()["encoding"]))
	assert.Equal(`["val2","val3"]`, string(sa.IndexedFields["key5"].GetData()))
	assert.Equal("Keyword", string(sa.IndexedFields["key5"].GetMetadata()["type"]))
	assert.Equal("json/plain", string(sa.IndexedFields["key5"].GetMetadata()["encoding"]))
	assert.Equal("[]", string(sa.IndexedFields["key6"].GetData()))
	assert.Equal("Keyword", string(sa.IndexedFields["key6"].GetMetadata()["type"]))
	assert.Equal("json/plain", string(sa.IndexedFields["key6"].GetMetadata()["encoding"]))
}
func Test_Encode_NilMap(t *testing.T) {
	assert := assert.New(t)

	sa, err := Encode(map[string]interface{}{
		"key1": "val1",
		"key2": 2,
		"key3": true,
		"key4": nil,
		"key5": []string{"val2", "val3"},
		"key6": []string{},
	}, nil)

	assert.NoError(err)
	assert.Len(sa.IndexedFields, 6)
	assert.Equal(`"val1"`, string(sa.IndexedFields["key1"].GetData()))
	assert.Equal("2", string(sa.IndexedFields["key2"].GetData()))
	assert.Equal("true", string(sa.IndexedFields["key3"].GetData()))
	assert.Equal("", string(sa.IndexedFields["key4"].GetData()))
	assert.Equal("binary/null", string(sa.IndexedFields["key4"].GetMetadata()["encoding"]))
	assert.Equal(`["val2","val3"]`, string(sa.IndexedFields["key5"].GetData()))
	assert.Equal("json/plain", string(sa.IndexedFields["key5"].GetMetadata()["encoding"]))
	assert.Equal("[]", string(sa.IndexedFields["key6"].GetData()))
	assert.Equal("json/plain", string(sa.IndexedFields["key6"].GetMetadata()["encoding"]))
}

func Test_Encode_Error(t *testing.T) {
	assert := assert.New(t)
	sa, err := Encode(map[string]interface{}{
		"key1": "val1",
		"key2": 2,
		"key3": true,
	}, &NameTypeMap{customSearchAttributes: map[string]enumspb.IndexedValueType{
		"key1": enumspb.INDEXED_VALUE_TYPE_TEXT,
		"key4": enumspb.INDEXED_VALUE_TYPE_INT,
		"key3": enumspb.INDEXED_VALUE_TYPE_BOOL,
	}})

	assert.Error(err)
	assert.True(errors.Is(err, ErrInvalidName))
	assert.Len(sa.IndexedFields, 3)
	assert.Equal(`"val1"`, string(sa.IndexedFields["key1"].GetData()))
	assert.Equal("Text", string(sa.IndexedFields["key1"].GetMetadata()["type"]))
	assert.Equal("2", string(sa.IndexedFields["key2"].GetData()))
	assert.Equal("true", string(sa.IndexedFields["key3"].GetData()))
	assert.Equal("Bool", string(sa.IndexedFields["key3"].GetMetadata()["type"]))
}

func Test_Decode_Success(t *testing.T) {
	assert := assert.New(t)

	typeMap := &NameTypeMap{customSearchAttributes: map[string]enumspb.IndexedValueType{
		"key1": enumspb.INDEXED_VALUE_TYPE_TEXT,
		"key2": enumspb.INDEXED_VALUE_TYPE_INT,
		"key3": enumspb.INDEXED_VALUE_TYPE_BOOL,
		"key4": enumspb.INDEXED_VALUE_TYPE_DOUBLE,
		"key5": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"key6": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	}}
	sa, err := Encode(map[string]interface{}{
		"key1": "val1",
		"key2": 2,
		"key3": true,
		"key4": nil,
		"key5": []string{"val2", "val3"},
		"key6": []string{},
	}, typeMap)
	assert.NoError(err)

	vals, err := Decode(sa, typeMap)
	assert.NoError(err)
	assert.Len(vals, 6)
	assert.Equal("val1", vals["key1"])
	assert.Equal(int64(2), vals["key2"])
	assert.Equal(true, vals["key3"])
	assert.Nil(vals["key4"])
	assert.Equal([]string{"val2", "val3"}, vals["key5"])
	assert.Nil(vals["key6"])

	delete(sa.IndexedFields["key1"].Metadata, "type")
	delete(sa.IndexedFields["key2"].Metadata, "type")
	delete(sa.IndexedFields["key3"].Metadata, "type")
	delete(sa.IndexedFields["key4"].Metadata, "type")
	delete(sa.IndexedFields["key5"].Metadata, "type")
	delete(sa.IndexedFields["key6"].Metadata, "type")

	vals, err = Decode(sa, typeMap)
	assert.NoError(err)
	assert.Len(vals, 6)
	assert.Equal("val1", vals["key1"])
	assert.Equal(int64(2), vals["key2"])
	assert.Equal(true, vals["key3"])
	assert.Nil(vals["key4"])
	assert.Equal([]string{"val2", "val3"}, vals["key5"])
	assert.Nil(vals["key6"])
}

func Test_Decode_NilMap(t *testing.T) {
	assert := assert.New(t)
	typeMap := &NameTypeMap{customSearchAttributes: map[string]enumspb.IndexedValueType{
		"key1": enumspb.INDEXED_VALUE_TYPE_TEXT,
		"key2": enumspb.INDEXED_VALUE_TYPE_INT,
		"key3": enumspb.INDEXED_VALUE_TYPE_BOOL,
		"key4": enumspb.INDEXED_VALUE_TYPE_DOUBLE,
		"key5": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"key6": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	}}
	sa, err := Encode(map[string]interface{}{
		"key1": "val1",
		"key2": 2,
		"key3": true,
		"key4": nil,
		"key5": []string{"val2", "val3"},
		"key6": []string{},
	}, typeMap)
	assert.NoError(err)

	vals, err := Decode(sa, nil)
	assert.NoError(err)
	assert.Len(sa.IndexedFields, 6)
	assert.Equal("val1", vals["key1"])
	assert.Equal(int64(2), vals["key2"])
	assert.Equal(true, vals["key3"])
	assert.Nil(vals["key4"])
	assert.Equal([]string{"val2", "val3"}, vals["key5"])
	assert.Nil(vals["key6"])
}

func Test_Decode_Error(t *testing.T) {
	assert := assert.New(t)

	typeMap := &NameTypeMap{customSearchAttributes: map[string]enumspb.IndexedValueType{
		"key1": enumspb.INDEXED_VALUE_TYPE_TEXT,
		"key2": enumspb.INDEXED_VALUE_TYPE_INT,
		"key3": enumspb.INDEXED_VALUE_TYPE_BOOL,
	}}
	sa, err := Encode(map[string]interface{}{
		"key1": "val1",
		"key2": 2,
		"key3": true,
	}, typeMap)
	assert.NoError(err)

	vals, err := Decode(sa, &NameTypeMap{customSearchAttributes: map[string]enumspb.IndexedValueType{
		"key1": enumspb.INDEXED_VALUE_TYPE_TEXT,
		"key4": enumspb.INDEXED_VALUE_TYPE_INT,
		"key3": enumspb.INDEXED_VALUE_TYPE_BOOL,
	}})
	assert.Error(err)
	assert.True(errors.Is(err, ErrInvalidName))
	assert.Len(sa.IndexedFields, 3)
	assert.Equal("val1", vals["key1"])
	assert.Equal(int64(2), vals["key2"])
	assert.Equal(true, vals["key3"])

	delete(sa.IndexedFields["key1"].Metadata, "type")
	delete(sa.IndexedFields["key2"].Metadata, "type")
	delete(sa.IndexedFields["key3"].Metadata, "type")

	vals, err = Decode(sa, nil)
	assert.Error(err)
	assert.True(errors.Is(err, ErrInvalidType))
	assert.Len(vals, 3)
	assert.Nil(vals["key1"])
	assert.Nil(vals["key2"])
	assert.Nil(vals["key3"])
}
