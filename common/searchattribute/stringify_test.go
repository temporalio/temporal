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

	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
)

type StringifySuite struct {
	suite.Suite
}

func TestStringifySuite(t *testing.T) {
	s := &StringifySuite{}
	suite.Run(t, s)
}

func (s *StringifySuite) SetupSuite() {
}

func (s *StringifySuite) SetupTest() {
}

func (s *StringifySuite) TearDownTest() {
}

func (s *StringifySuite) Test_Stringify() {
	typeMap := NameTypeMap{
		customSearchAttributes: map[string]enumspb.IndexedValueType{
			"key1": enumspb.INDEXED_VALUE_TYPE_TEXT,
			"key2": enumspb.INDEXED_VALUE_TYPE_INT,
			"key3": enumspb.INDEXED_VALUE_TYPE_BOOL,
		},
	}

	sa, err := Encode(map[string]interface{}{
		"key1": "val1",
		"key2": 2,
		"key3": true,
	}, &typeMap)
	s.NoError(err)

	saStr, err := Stringify(sa, nil)
	s.NoError(err)
	s.Len(saStr, 3)
	s.Equal("val1", saStr["key1"])
	s.Equal("2", saStr["key2"])
	s.Equal("true", saStr["key3"])

	// Clean Metadata type and use typeMap.
	delete(sa.IndexedFields["key1"].Metadata, "type")
	delete(sa.IndexedFields["key2"].Metadata, "type")
	delete(sa.IndexedFields["key3"].Metadata, "type")

	saStr, err = Stringify(sa, &typeMap)
	s.NoError(err)
	s.Len(saStr, 3)
	s.Equal("val1", saStr["key1"])
	s.Equal("2", saStr["key2"])
	s.Equal("true", saStr["key3"])

	// Even w/o typeMap error is returned but string values are set with  raw JSON from GetData().
	saStr, err = Stringify(sa, nil)
	s.Error(err)
	s.True(errors.Is(err, ErrInvalidType))
	s.Len(saStr, 3)
	s.Equal(`"val1"`, saStr["key1"])
	s.Equal("2", saStr["key2"])
	s.Equal("true", saStr["key3"])
}

func (s *StringifySuite) Test_Stringify_Array() {
	typeMap := NameTypeMap{
		customSearchAttributes: map[string]enumspb.IndexedValueType{
			"key1": enumspb.INDEXED_VALUE_TYPE_TEXT,
			"key2": enumspb.INDEXED_VALUE_TYPE_INT,
			"key3": enumspb.INDEXED_VALUE_TYPE_BOOL,
		},
	}

	sa, err := Encode(map[string]interface{}{
		"key1": []string{"val1", "val2"},
		"key2": []int64{2, 3, 4},
		"key3": []bool{true, false, true},
	}, &typeMap)
	s.NoError(err)

	saStr, err := Stringify(sa, nil)
	s.NoError(err)
	s.Len(saStr, 3)
	s.Equal(`["val1","val2"]`, saStr["key1"])
	s.Equal("[2,3,4]", saStr["key2"])
	s.Equal("[true,false,true]", saStr["key3"])

	// Clean Metadata type and use typeMap.
	delete(sa.IndexedFields["key1"].Metadata, "type")
	delete(sa.IndexedFields["key2"].Metadata, "type")
	delete(sa.IndexedFields["key3"].Metadata, "type")

	saStr, err = Stringify(sa, &typeMap)
	s.NoError(err)
	s.Len(saStr, 3)
	s.Equal(`["val1","val2"]`, saStr["key1"])
	s.Equal("[2,3,4]", saStr["key2"])
	s.Equal("[true,false,true]", saStr["key3"])

	// Even w/o typeMap error is returned but string values are set with  raw JSON from GetData().
	saStr, err = Stringify(sa, nil)
	s.Error(err)
	s.True(errors.Is(err, ErrInvalidType))
	s.Len(saStr, 3)
	s.Equal(`["val1","val2"]`, saStr["key1"])
	s.Equal("[2,3,4]", saStr["key2"])
	s.Equal("[true,false,true]", saStr["key3"])
}

func (s *StringifySuite) Test_Parse_ValidTypeMap() {
	sa, err := Parse(map[string]string{
		"key1": "val1",
		"key2": "2",
		"key3": "true",
	}, &NameTypeMap{
		customSearchAttributes: map[string]enumspb.IndexedValueType{
			"key1": enumspb.INDEXED_VALUE_TYPE_TEXT,
			"key2": enumspb.INDEXED_VALUE_TYPE_INT,
			"key3": enumspb.INDEXED_VALUE_TYPE_BOOL,
		}})

	s.NoError(err)
	s.Len(sa.IndexedFields, 3)
	s.Equal(`"val1"`, string(sa.IndexedFields["key1"].GetData()))
	s.Equal("Text", string(sa.IndexedFields["key1"].GetMetadata()["type"]))
	s.Equal("2", string(sa.IndexedFields["key2"].GetData()))
	s.Equal("Int", string(sa.IndexedFields["key2"].GetMetadata()["type"]))
	s.Equal("true", string(sa.IndexedFields["key3"].GetData()))
	s.Equal("Bool", string(sa.IndexedFields["key3"].GetMetadata()["type"]))
}

func (s *StringifySuite) Test_Parse_NilTypeMap() {
	sa, err := Parse(map[string]string{
		"key1": "val1",
		"key2": "2",
		"key3": "true",
	}, nil)

	s.NoError(err)
	s.Len(sa.IndexedFields, 3)
	s.Equal(`"val1"`, string(sa.IndexedFields["key1"].GetData()))
	s.Equal("2", string(sa.IndexedFields["key2"].GetData()))
	s.Equal("true", string(sa.IndexedFields["key3"].GetData()))

}
func (s *StringifySuite) Test_Parse_WrongTypesInTypeMap() {
	sa, err := Parse(map[string]string{
		"key1": "val1",
		"key2": "2",
	}, &NameTypeMap{
		customSearchAttributes: map[string]enumspb.IndexedValueType{
			"key1": enumspb.INDEXED_VALUE_TYPE_INT,
			"key2": enumspb.INDEXED_VALUE_TYPE_TEXT,
		}})

	s.Error(err)
	s.Len(sa.IndexedFields, 2)
	s.Nil(sa.IndexedFields["key1"])
	s.Equal(`"2"`, string(sa.IndexedFields["key2"].GetData()))
	s.Equal("Text", string(sa.IndexedFields["key2"].GetMetadata()["type"]))
}

func (s *StringifySuite) Test_Parse_MissedFieldsInTypeMap() {
	sa, err := Parse(map[string]string{
		"key1": "val1",
		"key2": "2",
	}, &NameTypeMap{
		customSearchAttributes: map[string]enumspb.IndexedValueType{
			"key3": enumspb.INDEXED_VALUE_TYPE_TEXT,
			"key2": enumspb.INDEXED_VALUE_TYPE_INT,
		}})

	s.NoError(err)
	s.Len(sa.IndexedFields, 2)
	s.Equal(`"val1"`, string(sa.IndexedFields["key1"].GetData()))
	s.Nil(sa.IndexedFields["key1"].GetMetadata()["type"])
	s.Equal("2", string(sa.IndexedFields["key2"].GetData()))
	s.Equal("Int", string(sa.IndexedFields["key2"].GetMetadata()["type"]))
}

func (s *StringifySuite) Test_Parse_Array() {
	sa, err := Parse(map[string]string{
		"key1": ` ["val1", "val2"] `,
		"key2": "[2,3,4]",
	}, &NameTypeMap{
		customSearchAttributes: map[string]enumspb.IndexedValueType{
			"key1": enumspb.INDEXED_VALUE_TYPE_TEXT,
			"key2": enumspb.INDEXED_VALUE_TYPE_INT,
		}})

	s.NoError(err)
	s.Len(sa.IndexedFields, 2)
	s.Equal(`["val1","val2"]`, string(sa.IndexedFields["key1"].GetData()))
	s.Equal("Text", string(sa.IndexedFields["key1"].GetMetadata()["type"]))
	s.Equal("[2,3,4]", string(sa.IndexedFields["key2"].GetData()))
	s.Equal("Int", string(sa.IndexedFields["key2"].GetMetadata()["type"]))
}

func (s *StringifySuite) Test_parseValueOrArray() {
	var res *commonpb.Payload
	var err error

	// int
	res, err = parseValueOrArray("1", enumspb.INDEXED_VALUE_TYPE_INT)
	s.NoError(err)
	s.Equal("Int", string(res.Metadata["type"]))
	s.Equal("1", string(res.Data))

	// array must be in JSON format.
	res, err = parseValueOrArray(`["qwe"]`, enumspb.INDEXED_VALUE_TYPE_TEXT)
	s.NoError(err)
	s.Equal("Text", string(res.Metadata["type"]))
	s.Equal(`["qwe"]`, string(res.Data))

	// array must be in JSON format.
	res, err = parseValueOrArray(`[qwe]`, enumspb.INDEXED_VALUE_TYPE_TEXT)
	s.Error(err)
	s.Nil(res)
}

func (s *StringifySuite) Test_parseValueTyped() {
	var res interface{}
	var err error

	// int
	res, err = parseValueTyped("1", enumspb.INDEXED_VALUE_TYPE_INT)
	s.NoError(err)
	s.Equal(int64(1), res)

	res, err = parseValueTyped("qwe", enumspb.INDEXED_VALUE_TYPE_INT)
	s.Error(err)
	s.Equal(int64(0), res)

	// bool
	res, err = parseValueTyped("true", enumspb.INDEXED_VALUE_TYPE_BOOL)
	s.NoError(err)
	s.Equal(true, res)
	res, err = parseValueTyped("false", enumspb.INDEXED_VALUE_TYPE_BOOL)
	s.NoError(err)
	s.Equal(false, res)
	res, err = parseValueTyped("qwe", enumspb.INDEXED_VALUE_TYPE_BOOL)
	s.Error(err)
	s.Equal(false, res)

	// double
	res, err = parseValueTyped("1.0", enumspb.INDEXED_VALUE_TYPE_DOUBLE)
	s.NoError(err)
	s.Equal(float64(1.0), res)

	res, err = parseValueTyped("qwe", enumspb.INDEXED_VALUE_TYPE_DOUBLE)
	s.Error(err)
	s.Equal(float64(0), res)

	// datetime
	res, err = parseValueTyped("2019-01-01T01:01:01Z", enumspb.INDEXED_VALUE_TYPE_DATETIME)
	s.NoError(err)
	s.Equal(time.Date(2019, 1, 1, 1, 1, 1, 0, time.UTC), res)

	res, err = parseValueTyped("qwe", enumspb.INDEXED_VALUE_TYPE_DATETIME)
	s.Error(err)
	s.Equal(time.Time{}, res)

	// string
	res, err = parseValueTyped("test string", enumspb.INDEXED_VALUE_TYPE_TEXT)
	s.NoError(err)
	s.Equal("test string", res)

	// unspecified
	res, err = parseValueTyped("test string", enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED)
	s.NoError(err)
	s.Equal("test string", res)
}

func (s *StringifySuite) Test_parseValueUnspecified() {
	var res interface{}

	// int
	res = parseValueUnspecified("1")
	s.Equal(int64(1), res)

	// bool
	res = parseValueUnspecified("true")
	s.Equal(true, res)
	res = parseValueUnspecified("false")
	s.Equal(false, res)

	// double
	res = parseValueUnspecified("1.0")
	s.Equal(float64(1.0), res)

	// datetime
	res = parseValueUnspecified("2019-01-01T01:01:01Z")
	s.Equal(time.Date(2019, 1, 1, 1, 1, 1, 0, time.UTC), res)

	// array
	res = parseValueUnspecified(`["a", "b", "c"]`)
	s.Equal([]interface{}{"a", "b", "c"}, res)

	// string
	res = parseValueUnspecified("test string")
	s.Equal("test string", res)
}

func (s *StringifySuite) Test_isJsonArray() {
	s.True(isJsonArray("[1,2,3]"))
	s.True(isJsonArray("  [1,2,3] "))
	s.True(isJsonArray(`  ["1","2","3"] `))
	s.True(isJsonArray("[]"))
	s.False(isJsonArray("["))
	s.False(isJsonArray("]"))
	s.False(isJsonArray("qwe"))
	s.False(isJsonArray("123"))
}

func (s *StringifySuite) Test_parseJsonArray() {
	t1, _ := time.Parse(time.RFC3339Nano, "2019-06-07T16:16:34-08:00")
	t2, _ := time.Parse(time.RFC3339Nano, "2019-06-07T17:16:34-08:00")
	testCases := []struct {
		name             string
		indexedValueType enumspb.IndexedValueType
		input            string
		expected         interface{}
	}{
		{
			name:             "string",
			indexedValueType: enumspb.INDEXED_VALUE_TYPE_TEXT,
			input:            `["a", "b", "c"]`,
			expected:         []string{"a", "b", "c"},
		},
		{
			name:             "int",
			indexedValueType: enumspb.INDEXED_VALUE_TYPE_INT,
			input:            `[1, 2, 3]`,
			expected:         []int64{1, 2, 3},
		},
		{
			name:             "double",
			indexedValueType: enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			input:            `[1.1, 2.2, 3.3]`,
			expected:         []float64{1.1, 2.2, 3.3},
		},
		{
			name:             "bool",
			indexedValueType: enumspb.INDEXED_VALUE_TYPE_BOOL,
			input:            `[true, false]`,
			expected:         []bool{true, false},
		},
		{
			name:             "datetime",
			indexedValueType: enumspb.INDEXED_VALUE_TYPE_DATETIME,
			input:            `["2019-06-07T16:16:34-08:00", "2019-06-07T17:16:34-08:00"]`,
			expected:         []time.Time{t1, t2},
		},
		{
			name:             "unspecified",
			indexedValueType: enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED,
			input:            `["a", "b", "c"]`,
			expected:         []interface{}{"a", "b", "c"},
		},
	}
	for _, testCase := range testCases {
		s.Run(testCase.name, func() {
			res, err := parseJsonArray(testCase.input, testCase.indexedValueType)
			s.NoError(err)
			s.Equal(testCase.expected, res)
		})
	}

	testCases2 := []struct {
		name             string
		indexedValueType enumspb.IndexedValueType
		input            string
		expected         error
	}{
		{
			name:             "not array",
			indexedValueType: enumspb.INDEXED_VALUE_TYPE_TEXT,
			input:            "normal string",
		},
		{
			name:             "empty string",
			indexedValueType: enumspb.INDEXED_VALUE_TYPE_TEXT,
			input:            "",
		},
		{
			name:             "not json array",
			indexedValueType: enumspb.INDEXED_VALUE_TYPE_TEXT,
			input:            "[a, b, c]",
		},
	}
	for _, testCase := range testCases2 {
		res, err := parseJsonArray(testCase.input, testCase.indexedValueType)
		s.NotNil(err)
		s.Nil(res)
	}
}
