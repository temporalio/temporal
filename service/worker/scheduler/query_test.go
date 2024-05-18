// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package scheduler

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
)

func TestFieldNameAggInterceptor(t *testing.T) {
	s := require.New(t)
	fnInterceptor := newFieldNameAggInterceptor()

	_, err := fnInterceptor.Name("foo", query.FieldNameFilter)
	s.NoError(err)
	s.Equal(map[string]bool{"foo": true}, fnInterceptor.names)

	_, err = fnInterceptor.Name("bar", query.FieldNameFilter)
	s.NoError(err)
	s.Equal(map[string]bool{"foo": true, "bar": true}, fnInterceptor.names)

	_, err = fnInterceptor.Name("foo", query.FieldNameFilter)
	s.NoError(err)
	s.Equal(map[string]bool{"foo": true, "bar": true}, fnInterceptor.names)
}

func TestGetQueryFields(t *testing.T) {
	testCases := []struct {
		name           string
		input          string
		expectedFields []string
		expectedErrMsg string
	}{
		{
			name:           "empty query string",
			input:          "",
			expectedFields: []string{},
			expectedErrMsg: "",
		},
		{
			name:           "filter custom search attribute",
			input:          "CustomKeywordField = 'foo'",
			expectedFields: []string{"CustomKeywordField"},
			expectedErrMsg: "",
		},
		{
			name:           "filter multiple custom search attribute",
			input:          "(CustomKeywordField = 'foo' AND CustomIntField = 123) OR CustomKeywordField = 'bar'",
			expectedFields: []string{"CustomKeywordField", "CustomIntField"},
			expectedErrMsg: "",
		},
		{
			name:           "filter TemporalSchedulePaused",
			input:          "TemporalSchedulePaused = true",
			expectedFields: []string{"TemporalSchedulePaused"},
			expectedErrMsg: "",
		},
		{
			name:           "filter TemporalSchedulePaused",
			input:          "TemporalSchedulePaused = true",
			expectedFields: []string{"TemporalSchedulePaused"},
			expectedErrMsg: "",
		},
		{
			name:           "filter TemporalSchedulePaused and custom search attribute",
			input:          "TemporalSchedulePaused = true AND CustomKeywordField = 'foo'",
			expectedFields: []string{"TemporalSchedulePaused", "CustomKeywordField"},
			expectedErrMsg: "",
		},
		{
			name:           "filter system search attribute",
			input:          "ExecutionDuration > '1s'",
			expectedFields: []string{"ExecutionDuration"},
			expectedErrMsg: "",
		},
		{
			name:           "invalid query filter",
			input:          "CustomKeywordField = foo",
			expectedFields: nil,
			expectedErrMsg: "invalid query",
		},
	}

	for _, tc := range testCases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				s := require.New(t)
				fields, err := getQueryFields(tc.input, searchattribute.TestNameTypeMap)
				if tc.expectedErrMsg == "" {
					s.NoError(err)
					s.Equal(len(tc.expectedFields), len(fields))
					for _, f := range fields {
						s.Contains(tc.expectedFields, f)
					}
				} else {
					var invalidArgErr *serviceerror.InvalidArgument
					s.ErrorAs(err, &invalidArgErr)
					s.ErrorContains(err, tc.expectedErrMsg)
				}
			},
		)
	}
}

func TestValidateVisibilityQuery(t *testing.T) {
	testCases := []struct {
		name           string
		input          string
		expectedErrMsg string
	}{
		{
			name:           "empty query string",
			input:          "",
			expectedErrMsg: "",
		},
		{
			name:           "filter custom search attribute",
			input:          "CustomKeywordField = 'foo'",
			expectedErrMsg: "",
		},
		{
			name:           "filter multiple custom search attribute",
			input:          "(CustomKeywordField = 'foo' AND CustomIntField = 123) OR CustomKeywordField = 'bar'",
			expectedErrMsg: "",
		},
		{
			name:           "filter TemporalSchedulePaused",
			input:          "TemporalSchedulePaused = true",
			expectedErrMsg: "",
		},
		{
			name:           "filter TemporalSchedulePaused",
			input:          "TemporalSchedulePaused = true",
			expectedErrMsg: "",
		},
		{
			name:           "filter TemporalSchedulePaused and custom search attribute",
			input:          "TemporalSchedulePaused = true AND CustomKeywordField = 'foo'",
			expectedErrMsg: "",
		},
		{
			name:           "invalid filter system search attribute",
			input:          "ExecutionDuration > '1s'",
			expectedErrMsg: "invalid query filter for schedules: cannot filter on \"ExecutionDuration\"",
		},
		{
			name:           "invalid query filter",
			input:          "CustomKeywordField = foo",
			expectedErrMsg: "invalid query",
		},
	}

	for _, tc := range testCases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				s := require.New(t)
				err := ValidateVisibilityQuery(tc.input, searchattribute.TestNameTypeMap)
				if tc.expectedErrMsg == "" {
					s.NoError(err)
				} else {
					var invalidArgErr *serviceerror.InvalidArgument
					s.ErrorAs(err, &invalidArgErr)
					s.ErrorContains(err, tc.expectedErrMsg)
				}
			},
		)
	}
}
