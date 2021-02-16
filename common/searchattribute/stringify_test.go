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
	"time"

	"github.com/stretchr/testify/suite"
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

func (s *StringifySuite) Test_parseJsonArray() {
	t1, _ := time.Parse(time.RFC3339, "2019-06-07T16:16:34-08:00")
	t2, _ := time.Parse(time.RFC3339, "2019-06-07T17:16:34-08:00")
	testCases := []struct {
		name             string
		indexedValueType enumspb.IndexedValueType
		input            string
		expected         interface{}
	}{
		{
			name:             "string",
			indexedValueType: enumspb.INDEXED_VALUE_TYPE_STRING,
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
			indexedValueType: enumspb.INDEXED_VALUE_TYPE_STRING,
			input:            "normal string",
		},
		{
			name:             "empty string",
			indexedValueType: enumspb.INDEXED_VALUE_TYPE_STRING,
			input:            "",
		},
		{
			name:             "not json array",
			indexedValueType: enumspb.INDEXED_VALUE_TYPE_STRING,
			input:            "[a, b, c]",
		},
	}
	for _, testCase := range testCases2 {
		res, err := parseJsonArray(testCase.input, testCase.indexedValueType)
		s.NotNil(err)
		s.Nil(res)
	}
}
