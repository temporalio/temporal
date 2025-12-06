package scheduler

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

const (
	testNamespace = namespace.Name("test-namespace")
)

func TestFieldNameAggInterceptor(t *testing.T) {
	s := require.New(t)
	fnInterceptor := newFieldNameAggInterceptor(
		testNamespace,
		searchattribute.TestEsNameTypeMap(),
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

func TestFieldSaAggInterceptor(t *testing.T) {
	s := require.New(t)
	saInterceptor := newSaAggInterceptor()
	intCol := query.NewSAColumn(
		"AliasForCustomIntField",
		"CustomIntField",
		enumspb.INDEXED_VALUE_TYPE_INT,
	)
	keywordCol := query.NewSAColumn(
		"AliasForCustomKeywordField",
		"CustomKeywordField",
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)
	startTimeCol := query.NewSAColumn(
		sadefs.StartTime,
		sadefs.StartTime,
		enumspb.INDEXED_VALUE_TYPE_DATETIME,
	)

	err := saInterceptor.Intercept(intCol)
	s.NoError(err)
	s.Equal(map[string]struct{}{"AliasForCustomIntField": struct{}{}}, saInterceptor.names)

	err = saInterceptor.Intercept(keywordCol)
	s.NoError(err)
	s.Equal(
		map[string]struct{}{
			"AliasForCustomIntField":     struct{}{},
			"AliasForCustomKeywordField": struct{}{},
		},
		saInterceptor.names,
	)

	err = saInterceptor.Intercept(intCol)
	s.NoError(err)
	s.Equal(
		map[string]struct{}{
			"AliasForCustomIntField":     struct{}{},
			"AliasForCustomKeywordField": struct{}{},
		},
		saInterceptor.names,
	)

	err = saInterceptor.Intercept(startTimeCol)
	s.NoError(err)
	s.Equal(
		map[string]struct{}{
			"AliasForCustomIntField":     struct{}{},
			"AliasForCustomKeywordField": struct{}{},
			sadefs.StartTime:             struct{}{},
		},
		saInterceptor.names,
	)
}

func TestGetQueryFieldsLegacy(t *testing.T) {
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
				fields, err := getQueryFieldsLegacy(
					testNamespace,
					searchattribute.TestEsNameTypeMap(),
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
			input:          "AliasForKeyword01 = 'foo'",
			expectedFields: []string{"AliasForKeyword01"},
			expectedErrMsg: "",
		},
		{
			name:           "filter multiple custom search attribute",
			input:          "(AliasForKeyword01 = 'foo' AND AliasForInt01 = 123) OR AliasForKeyword01 = 'bar'",
			expectedFields: []string{"AliasForKeyword01", "AliasForInt01"},
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
			input:          "TemporalSchedulePaused = true AND AliasForKeyword01 = 'foo'",
			expectedFields: []string{"TemporalSchedulePaused", "AliasForKeyword01"},
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
			input:          "AliasForKeyword01 = foo",
			expectedFields: nil,
			expectedErrMsg: "invalid query",
		},
		{
			name:           "invalid custom search attribute",
			input:          "Foo = 'bar'",
			expectedFields: nil,
			expectedErrMsg: "'Foo' is not a valid search attribute",
		},
	}

	for _, tc := range testCases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				s := require.New(t)
				fields, err := getQueryFields(
					testNamespace,
					searchattribute.TestNameTypeMap(),
					searchattribute.NewTestMapperProvider(&searchattribute.TestMapper{}),
					tc.input,
				)
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
			input:          "AliasForKeyword01 = 'foo'",
			expectedErrMsg: "",
		},
		{
			name:           "filter multiple custom search attribute",
			input:          "(AliasForKeyword01 = 'foo' AND AliasForInt01 = 123) OR AliasForKeyword01 = 'bar'",
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
			input:          "TemporalSchedulePaused = true AND AliasForKeyword01 = 'foo'",
			expectedErrMsg: "",
		},
		{
			name:           "invalid filter system search attribute",
			input:          "ExecutionDuration > '1s'",
			expectedErrMsg: "invalid query filter for schedules: cannot filter on \"ExecutionDuration\"",
		},
		{
			name:           "invalid query filter",
			input:          "AliasForKeyword01 = foo",
			expectedErrMsg: "invalid query",
		},
		{
			name:           "invalid custom search attribute",
			input:          "Foo = foo",
			expectedErrMsg: "'Foo' is not a valid search attribute",
		},
	}

	for _, tc := range testCases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				s := require.New(t)
				err := ValidateVisibilityQuery(
					testNamespace,
					searchattribute.TestNameTypeMap(),
					searchattribute.NewTestMapperProvider(&searchattribute.TestMapper{}),
					dynamicconfig.GetBoolPropertyFn(true),
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
