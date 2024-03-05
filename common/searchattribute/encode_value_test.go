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
	"time"

	"github.com/stretchr/testify/assert"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/converter"

	"go.temporal.io/server/common/payload"
)

func Test_DecodeValue_AllowList_FromMetadata_Success(t *testing.T) {
	s := assert.New(t)
	allowList := true

	payloadStr := payload.EncodeString("qwe")
	payloadStr.Metadata["type"] = []byte("Text")
	decodedStr, err := DecodeValue(payloadStr, enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, allowList) // MetadataType is used.
	s.NoError(err)
	s.Equal("qwe", decodedStr)

	payloadBool, err := payload.Encode(true)
	s.NoError(err)
	payloadBool.Metadata["type"] = []byte("Bool")
	decodedBool, err := DecodeValue(payloadBool, enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, allowList) // MetadataType is used.
	s.NoError(err)
	s.Equal(true, decodedBool)

	payloadNil, err := payload.Encode(nil)
	s.NoError(err)
	payloadNil.Metadata["type"] = []byte("Double")
	decodedNil, err := DecodeValue(payloadNil, enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, allowList)
	s.NoError(err)
	s.Nil(decodedNil)

	payloadSlice, err := payload.Encode([]string{"val1", "val2"})
	s.NoError(err)
	payloadSlice.Metadata["type"] = []byte("Keyword")
	decodedSlice, err := DecodeValue(payloadSlice, enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, allowList)
	s.NoError(err)
	s.Equal([]string{"val1", "val2"}, decodedSlice)

	payloadEmptySlice, err := payload.Encode([]string{})
	s.NoError(err)
	payloadEmptySlice.Metadata["type"] = []byte("Keyword")
	decodedNil, err = DecodeValue(payloadEmptySlice, enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, allowList)
	s.NoError(err)
	s.Nil(decodedNil)

	var expectedEncodedRepresentation = "2022-03-07T21:27:35.986848-05:00"
	timeValue, err := time.Parse(time.RFC3339, expectedEncodedRepresentation)
	s.NoError(err)
	payloadDatetime, err := payload.Encode(timeValue)
	s.NoError(err)
	payloadDatetime.Metadata["type"] = []byte("Datetime")
	decodedDatetime, err := DecodeValue(payloadDatetime, enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, allowList)
	s.NoError(err)
	s.Equal(timeValue, decodedDatetime)
}

func Test_DecodeValue_AllowList_FromParameter_Success(t *testing.T) {
	s := assert.New(t)
	allowList := true

	payloadStr := payload.EncodeString("qwe")
	decodedStr, err := DecodeValue(payloadStr, enumspb.INDEXED_VALUE_TYPE_TEXT, allowList)
	s.NoError(err)
	s.Equal("qwe", decodedStr)

	payloadInt, err := payload.Encode(123)
	s.NoError(err)
	decodedInt, err := DecodeValue(payloadInt, enumspb.INDEXED_VALUE_TYPE_INT, allowList)
	s.NoError(err)
	s.Equal(int64(123), decodedInt)

	payloadNil, err := payload.Encode(nil)
	s.NoError(err)
	decodedNil, err := DecodeValue(payloadNil, enumspb.INDEXED_VALUE_TYPE_DOUBLE, allowList)
	s.NoError(err)
	s.Nil(decodedNil)

	payloadSlice, err := payload.Encode([]string{"val1", "val2"})
	s.NoError(err)
	decodedSlice, err := DecodeValue(payloadSlice, enumspb.INDEXED_VALUE_TYPE_KEYWORD, allowList)
	s.NoError(err)
	s.Equal([]string{"val1", "val2"}, decodedSlice)

	payloadEmptySlice, err := payload.Encode([]string{})
	s.NoError(err)
	decodedNil, err = DecodeValue(payloadEmptySlice, enumspb.INDEXED_VALUE_TYPE_KEYWORD, allowList)
	s.NoError(err)
	s.Nil(decodedNil)

	var expectedEncodedRepresentation = "2022-03-07T21:27:35.986848-05:00"
	timeValue, err := time.Parse(time.RFC3339, expectedEncodedRepresentation)
	s.NoError(err)
	payloadDatetime, err := payload.Encode(timeValue)
	s.NoError(err)
	decodedDatetime, err := DecodeValue(payloadDatetime, enumspb.INDEXED_VALUE_TYPE_DATETIME, allowList)
	s.NoError(err)
	s.Equal(timeValue, decodedDatetime)

	payloadInt, err = payload.Encode(123)
	s.NoError(err)
	payloadInt.Metadata["type"] = []byte("String") // MetadataType is not used.
	decodedInt, err = DecodeValue(payloadInt, enumspb.INDEXED_VALUE_TYPE_INT, allowList)
	s.NoError(err)
	s.Equal(int64(123), decodedInt)

	payloadInt, err = payload.Encode(123)
	s.NoError(err)
	payloadInt.Metadata["type"] = []byte("UnknownType") // MetadataType is not used.
	decodedInt, err = DecodeValue(payloadInt, enumspb.INDEXED_VALUE_TYPE_INT, allowList)
	s.NoError(err)
	s.Equal(int64(123), decodedInt)

	payloadBool, err := payload.Encode(true)
	s.NoError(err)
	decodedBool, err := DecodeValue(payloadBool, enumspb.INDEXED_VALUE_TYPE_BOOL, allowList)
	s.NoError(err)
	s.Equal(true, decodedBool)
}

func Test_DecodeValue_AllowList_Error(t *testing.T) {
	s := assert.New(t)
	allowList := true

	payloadStr := payload.EncodeString("qwe")
	decodedStr, err := DecodeValue(payloadStr, enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, allowList)
	s.Error(err)
	s.ErrorIs(err, ErrInvalidType)
	s.Nil(decodedStr)

	payloadInt, err := payload.Encode(123)
	s.NoError(err)
	payloadInt.Metadata["type"] = []byte("UnknownType")
	decodedInt, err := DecodeValue(payloadInt, enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, allowList)
	s.Error(err)
	s.ErrorIs(err, ErrInvalidType)
	s.Nil(decodedInt)

	payloadInt, err = payload.Encode(123)
	s.NoError(err)
	payloadInt.Metadata["type"] = []byte("Text")
	decodedInt, err = DecodeValue(payloadInt, enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, allowList)
	s.Error(err)
	s.ErrorIs(err, converter.ErrUnableToDecode, err.Error())
	s.Nil(decodedInt)
}

func Test_DecodeValue_NotAllowList_FromMetadata_Success(t *testing.T) {
	s := assert.New(t)
	allowList := false

	payloadStr := payload.EncodeString("qwe")
	payloadStr.Metadata["type"] = []byte("Text")
	decodedStr, err := DecodeValue(payloadStr, enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, allowList)
	s.NoError(err)
	s.Equal("qwe", decodedStr)

	payloadBool, err := payload.Encode(true)
	s.NoError(err)
	payloadBool.Metadata["type"] = []byte("Bool")
	decodedBool, err := DecodeValue(payloadBool, enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, allowList)
	s.NoError(err)
	s.Equal(true, decodedBool)

	payloadNil, err := payload.Encode(nil)
	s.NoError(err)
	payloadNil.Metadata["type"] = []byte("Double")
	decodedNil, err := DecodeValue(payloadNil, enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, allowList)
	s.NoError(err)
	s.Nil(decodedNil)

	payloadKeyword, err := payload.Encode([]string{"Keyword"})
	s.NoError(err)
	payloadKeyword.Metadata["type"] = []byte("Keyword")
	decodedKeyword, err := DecodeValue(payloadKeyword, enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, allowList)
	s.NoError(err)
	s.Equal("Keyword", decodedKeyword)

	payloadSlice, err := payload.Encode([]string{"val1", "val2"})
	s.NoError(err)
	payloadSlice.Metadata["type"] = []byte("KeywordList")
	decodedSlice, err := DecodeValue(payloadSlice, enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, allowList)
	s.NoError(err)
	s.Equal([]string{"val1", "val2"}, decodedSlice)

	payloadEmptySlice, err := payload.Encode([]string{})
	s.NoError(err)
	payloadEmptySlice.Metadata["type"] = []byte("Keyword")
	decodedNil, err = DecodeValue(payloadEmptySlice, enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, allowList)
	s.NoError(err)
	s.Nil(decodedNil)

	var expectedEncodedRepresentation = "2022-03-07T21:27:35.986848-05:00"
	timeValue, err := time.Parse(time.RFC3339, expectedEncodedRepresentation)
	s.NoError(err)
	payloadDatetime, err := payload.Encode(timeValue)
	s.NoError(err)
	payloadDatetime.Metadata["type"] = []byte("Datetime")
	decodedDatetime, err := DecodeValue(payloadDatetime, enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, allowList)
	s.NoError(err)
	s.Equal(timeValue, decodedDatetime)
}

func Test_DecodeValue_NotAllowList_FromParameter_Success(t *testing.T) {
	s := assert.New(t)
	allowList := false

	payloadStr := payload.EncodeString("qwe")
	decodedStr, err := DecodeValue(payloadStr, enumspb.INDEXED_VALUE_TYPE_TEXT, allowList)
	s.NoError(err)
	s.Equal("qwe", decodedStr)

	payloadInt, err := payload.Encode(123)
	s.NoError(err)
	decodedInt, err := DecodeValue(payloadInt, enumspb.INDEXED_VALUE_TYPE_INT, allowList)
	s.NoError(err)
	s.Equal(int64(123), decodedInt)

	payloadNil, err := payload.Encode(nil)
	s.NoError(err)
	decodedNil, err := DecodeValue(payloadNil, enumspb.INDEXED_VALUE_TYPE_DOUBLE, allowList)
	s.NoError(err)
	s.Nil(decodedNil)

	payloadKeyword, err := payload.Encode([]string{"Keyword"})
	s.NoError(err)
	decodedKeyword, err := DecodeValue(payloadKeyword, enumspb.INDEXED_VALUE_TYPE_KEYWORD, allowList)
	s.NoError(err)
	s.Equal("Keyword", decodedKeyword)

	payloadSlice, err := payload.Encode([]string{"val1", "val2"})
	s.NoError(err)
	decodedSlice, err := DecodeValue(payloadSlice, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, allowList)
	s.NoError(err)
	s.Equal([]string{"val1", "val2"}, decodedSlice)

	payloadEmptySlice, err := payload.Encode([]string{})
	s.NoError(err)
	decodedNil, err = DecodeValue(payloadEmptySlice, enumspb.INDEXED_VALUE_TYPE_KEYWORD, allowList)
	s.NoError(err)
	s.Nil(decodedNil)

	var expectedEncodedRepresentation = "2022-03-07T21:27:35.986848-05:00"
	timeValue, err := time.Parse(time.RFC3339, expectedEncodedRepresentation)
	s.NoError(err)
	payloadDatetime, err := payload.Encode(timeValue)
	s.NoError(err)
	decodedDatetime, err := DecodeValue(payloadDatetime, enumspb.INDEXED_VALUE_TYPE_DATETIME, allowList)
	s.NoError(err)
	s.Equal(timeValue, decodedDatetime)

	payloadInt, err = payload.Encode(123)
	s.NoError(err)
	payloadInt.Metadata["type"] = []byte("String") // MetadataType is not used.
	decodedInt, err = DecodeValue(payloadInt, enumspb.INDEXED_VALUE_TYPE_INT, allowList)
	s.NoError(err)
	s.Equal(int64(123), decodedInt)

	payloadInt, err = payload.Encode(123)
	s.NoError(err)
	payloadInt.Metadata["type"] = []byte("UnknownType") // MetadataType is not used.
	decodedInt, err = DecodeValue(payloadInt, enumspb.INDEXED_VALUE_TYPE_INT, allowList)
	s.NoError(err)
	s.Equal(int64(123), decodedInt)

	payloadBool, err := payload.Encode(true)
	s.NoError(err)
	decodedBool, err := DecodeValue(payloadBool, enumspb.INDEXED_VALUE_TYPE_BOOL, allowList)
	s.NoError(err)
	s.Equal(true, decodedBool)
}

func Test_DecodeValue_NotAllowList_Error(t *testing.T) {
	s := assert.New(t)
	allowList := false

	payloadStr := payload.EncodeString("qwe")
	decodedStr, err := DecodeValue(payloadStr, enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, allowList)
	s.Error(err)
	s.ErrorIs(err, ErrInvalidType)
	s.Nil(decodedStr)

	payloadInt, err := payload.Encode(123)
	s.NoError(err)
	payloadInt.Metadata["type"] = []byte("UnknownType")
	decodedInt, err := DecodeValue(payloadInt, enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, allowList)
	s.Error(err)
	s.ErrorIs(err, ErrInvalidType)
	s.Nil(decodedInt)

	payloadInt, err = payload.Encode(123)
	s.NoError(err)
	payloadInt.Metadata["type"] = []byte("Text")
	decodedInt, err = DecodeValue(payloadInt, enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, allowList)
	s.Error(err)
	s.ErrorIs(err, converter.ErrUnableToDecode, err.Error())
	s.Nil(decodedInt)

	payloadSlice, err := payload.Encode([]string{"val1", "val2"})
	s.NoError(err)
	decodedSlice, err := DecodeValue(payloadSlice, enumspb.INDEXED_VALUE_TYPE_KEYWORD, allowList)
	s.Error(err)
	s.Nil(decodedSlice)
}

func Test_EncodeValue(t *testing.T) {
	s := assert.New(t)

	encodedPayload, err := EncodeValue(123, enumspb.INDEXED_VALUE_TYPE_INT)
	s.NoError(err)
	s.Equal("123", string(encodedPayload.GetData()))
	s.Equal("Int", string(encodedPayload.Metadata["type"]))

	encodedPayload, err = EncodeValue("qwe", enumspb.INDEXED_VALUE_TYPE_TEXT)
	s.NoError(err)
	s.Equal(`"qwe"`, string(encodedPayload.GetData()))
	s.Equal("Text", string(encodedPayload.Metadata["type"]))

	encodedPayload, err = EncodeValue(nil, enumspb.INDEXED_VALUE_TYPE_DOUBLE)
	s.NoError(err)
	s.Equal("", string(encodedPayload.GetData()))
	s.Equal("Double", string(encodedPayload.Metadata["type"]))
	s.Equal("binary/null", string(encodedPayload.Metadata["encoding"]))

	encodedPayload, err = EncodeValue([]string{"val1", "val2"}, enumspb.INDEXED_VALUE_TYPE_KEYWORD)
	s.NoError(err)
	s.Equal(`["val1","val2"]`, string(encodedPayload.GetData()))
	s.Equal("Keyword", string(encodedPayload.Metadata["type"]))
	s.Equal("json/plain", string(encodedPayload.Metadata["encoding"]))

	encodedPayload, err = EncodeValue([]string{}, enumspb.INDEXED_VALUE_TYPE_KEYWORD)
	s.NoError(err)
	s.Equal("[]", string(encodedPayload.GetData()))
	s.Equal("Keyword", string(encodedPayload.Metadata["type"]))
	s.Equal("json/plain", string(encodedPayload.Metadata["encoding"]))

	var expectedEncodedRepresentation = "2022-03-07T21:27:35.986848-05:00"
	timeValue, err := time.Parse(time.RFC3339, expectedEncodedRepresentation)
	s.NoError(err)
	encodedPayload, err = EncodeValue(timeValue, enumspb.INDEXED_VALUE_TYPE_DATETIME)
	s.NoError(err)
	s.Equal(`"`+expectedEncodedRepresentation+`"`, string(encodedPayload.GetData()),
		"Datetime Search Attribute is expected to be encoded in RFC 3339 format")
	s.Equal("Datetime", string(encodedPayload.Metadata["type"]))
}

func Test_ValidateStrings(t *testing.T) {
	_, err := validateStrings("anything here", errors.New("test error"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "test error")

	_, err = validateStrings("\x87\x01", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "is not a valid UTF-8 string")

	value, err := validateStrings("anything here", nil)
	assert.Nil(t, err)
	assert.Equal(t, "anything here", value)

	_, err = validateStrings([]string{"abc", "\x87\x01"}, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "is not a valid UTF-8 string")
}
