package scheduler

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
)

const (
	testNamespace = namespace.Name("test-namespace")
)

func TestFieldNameAggInterceptor(t *testing.T) {
	s := require.New(t)
	fnInterceptor := newFieldNameAggInterceptor(
		testNamespace,
		searchattribute.TestNameTypeMap,
		searchattribute.NewTestMapperProvider(nil),
	)

	_, err := fnInterceptor.Name("CustomIntField", query.FieldNameFilter)
	s.NoError(err)
	s.Equal(map[string]bool{"CustomIntField": true}, fnInterceptor.names)

	_, err = fnInterceptor.Name("CustomKeywordField", query.FieldNameFilter)
	s.NoError(err)
	s.Equal(map[string]bool{"CustomIntField": true, "CustomKeywordField": true}, fnInterceptor.names)

	_, err = fnInterceptor.Name("CustomIntField", query.FieldNameFilter)
	s.NoError(err)
	s.Equal(map[string]bool{"CustomIntField": true, "CustomKeywordField": true}, fnInterceptor.names)

	_, err = fnInterceptor.Name("search-attribute-not-found", query.FieldNameFilter)
	s.Error(err)
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
		{
			name:           "invalid custom search attribute",
			input:          "Foo = 'bar'",
			expectedFields: nil,
			expectedErrMsg: "invalid search attribute: Foo",
		},
	}

	for _, tc := range testCases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				s := require.New(t)
				fields, err := getQueryFields(
					testNamespace,
					searchattribute.TestNameTypeMap,
					searchattribute.NewTestMapperProvider(nil),
					tc.input,
				)
				if tc.expectedErrMsg == "" {
					s.NoError(err)
					s.Len(fields, len(tc.expectedFields))
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
		{
			name:           "invalid custom search attribute",
			input:          "Foo = foo",
			expectedErrMsg: "invalid search attribute: Foo",
		},
	}

	for _, tc := range testCases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				s := require.New(t)
				err := ValidateVisibilityQuery(
					testNamespace,
					searchattribute.TestNameTypeMap,
					searchattribute.NewTestMapperProvider(nil),
					tc.input,
				)
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
