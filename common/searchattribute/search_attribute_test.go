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
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/converter"

	"go.temporal.io/server/common/payload"
)

func Test_ConvertDynamicConfigToIndexedValueTypes(t *testing.T) {
	assert := assert.New(t)
	m := map[string]interface{}{
		"key1":  float64(1),
		"key2":  float64(2),
		"key3":  float64(3),
		"key4":  float64(4),
		"key5":  float64(5),
		"key6":  float64(6),
		"key1i": 1,
		"key2i": 2,
		"key3i": 3,
		"key4i": 4,
		"key5i": 5,
		"key6i": 6,
		"key1t": enumspb.INDEXED_VALUE_TYPE_STRING,
		"key2t": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"key3t": enumspb.INDEXED_VALUE_TYPE_INT,
		"key4t": enumspb.INDEXED_VALUE_TYPE_DOUBLE,
		"key5t": enumspb.INDEXED_VALUE_TYPE_BOOL,
		"key6t": enumspb.INDEXED_VALUE_TYPE_DATETIME,
		"key1s": "String",
		"key2s": "Keyword",
		"key3s": "Int",
		"key4s": "Double",
		"key5s": "Bool",
		"key6s": "Datetime",
	}
	result := ConvertDynamicConfigToIndexedValueTypes(m)
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_STRING, result["key1"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_KEYWORD, result["key2"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_INT, result["key3"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_DOUBLE, result["key4"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_BOOL, result["key5"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_DATETIME, result["key6"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_STRING, result["key1i"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_KEYWORD, result["key2i"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_INT, result["key3i"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_DOUBLE, result["key4i"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_BOOL, result["key5i"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_DATETIME, result["key6i"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_STRING, result["key1t"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_KEYWORD, result["key2t"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_INT, result["key3t"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_DOUBLE, result["key4t"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_BOOL, result["key5t"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_DATETIME, result["key6t"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_STRING, result["key1s"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_KEYWORD, result["key2s"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_INT, result["key3s"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_DOUBLE, result["key4s"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_BOOL, result["key5s"])
	assert.Equal(enumspb.INDEXED_VALUE_TYPE_DATETIME, result["key6s"])
	assert.Panics(func() {
		ConvertDynamicConfigToIndexedValueTypes(map[string]interface{}{
			"invalidType": "unknown",
		})
	})
}

func Test_DecodeSuccess(t *testing.T) {
	assert := assert.New(t)

	payloadStr := payload.EncodeString("qwe")
	payloadStr.Metadata["type"] = []byte("String")
	decodedStr, err := Decode(payloadStr)
	assert.NoError(err)
	assert.Equal("qwe", decodedStr)

	payloadInt, err := payload.Encode(123)
	assert.NoError(err)
	payloadInt.Metadata["type"] = []byte("Int")
	decodedInt, err := Decode(payloadInt)
	assert.NoError(err)
	assert.Equal(int64(123), decodedInt)

	payloadBool, err := payload.Encode(true)
	assert.NoError(err)
	payloadBool.Metadata["type"] = []byte("Bool")
	decodedBool, err := Decode(payloadBool)
	assert.NoError(err)
	assert.Equal(true, decodedBool)
}

func Test_DecodeError(t *testing.T) {
	assert := assert.New(t)

	payloadStr := payload.EncodeString("qwe")
	decodedStr, err := Decode(payloadStr)
	assert.Error(err)
	assert.True(errors.Is(err, ErrMissingMetadataType))
	assert.Nil(decodedStr)

	payloadInt, err := payload.Encode(123)
	assert.NoError(err)
	payloadInt.Metadata["type"] = []byte("UnknownType")
	decodedInt, err := Decode(payloadInt)
	assert.Error(err)
	assert.True(errors.Is(err, ErrInvalidMetadataType))
	assert.Nil(decodedInt)

	payloadInt, err = payload.Encode(123)
	assert.NoError(err)
	payloadInt.Metadata["type"] = []byte("String")
	decodedInt, err = Decode(payloadInt)
	assert.Error(err)
	assert.True(errors.Is(err, converter.ErrUnableToDecode))
	assert.Nil(decodedInt)
}

func Test_SetTypeSuccess(t *testing.T) {
	assert := assert.New(t)

	payloadInt, err := payload.Encode(123)
	assert.NoError(err)

	sa := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"key1": payload.EncodeString("str"),
			"key2": payload.EncodeString("keyword"),
			"key3": payloadInt,
		},
	}

	validSearchAttributes := map[string]enumspb.IndexedValueType{
		"key1": enumspb.INDEXED_VALUE_TYPE_STRING,
		"key2": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"key3": enumspb.INDEXED_VALUE_TYPE_INT,
		"key4": enumspb.INDEXED_VALUE_TYPE_DOUBLE,
	}

	SetType(sa, validSearchAttributes)
	assert.Equal("String", string(sa.GetIndexedFields()["key1"].Metadata["type"]))
	assert.Equal("Keyword", string(sa.GetIndexedFields()["key2"].Metadata["type"]))
	assert.Equal("Int", string(sa.GetIndexedFields()["key3"].Metadata["type"]))
}

func Test_SetTypeSkip(t *testing.T) {
	assert := assert.New(t)

	payloadInt, err := payload.Encode(123)
	assert.NoError(err)
	payloadInt.Metadata["type"] = []byte("String")

	sa := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"UnknownKey": payload.EncodeString("str"),
			"key4":       payload.EncodeString("invalid IndexValueType"),
			"key3":       payloadInt, // Another type already set
		},
	}

	validSearchAttributes := map[string]enumspb.IndexedValueType{
		"key1": enumspb.INDEXED_VALUE_TYPE_STRING,
		"key2": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"key3": enumspb.INDEXED_VALUE_TYPE_INT,
		"key4": enumspb.IndexedValueType(100),
	}

	SetType(sa, validSearchAttributes)
	assert.Nil(sa.GetIndexedFields()["UnknownKey"].Metadata["type"])
	assert.Nil(sa.GetIndexedFields()["key4"].Metadata["type"])
	assert.Equal("String", string(sa.GetIndexedFields()["key3"].Metadata["type"]))
}
