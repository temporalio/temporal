package elasticsearch

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/searchattribute"
	"go.uber.org/mock/gomock"
)

type (
	QueryInterceptorSuite struct {
		suite.Suite
		*require.Assertions
		controller *gomock.Controller
	}
)

func TestQueryInterceptorSuite(t *testing.T) {
	suite.Run(t, &QueryInterceptorSuite{})
}

func (s *QueryInterceptorSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
}

func (s *QueryInterceptorSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *QueryInterceptorSuite) TestTimeProcessFunc() {
	vi := NewValuesInterceptor(
		"test-namespace",
		searchattribute.TestNameTypeMap,
	)

	cases := []struct {
		key   string
		value interface{}
	}{
		{key: searchattribute.StartTime, value: int64(1528358645123456789)},
		{key: searchattribute.CloseTime, value: "2018-06-07T15:04:05+07:00"},
		{key: searchattribute.ExecutionTime, value: "some invalid time string"},
		{key: searchattribute.WorkflowID, value: "should not be modified"},
	}
	expected := []struct {
		value     string
		returnErr bool
	}{
		{value: "2018-06-07T08:04:05.123456789Z", returnErr: false},
		{value: "2018-06-07T15:04:05+07:00", returnErr: false},
		{value: "", returnErr: true},
		{value: "should not be modified", returnErr: false},
	}

	for i, testCase := range cases {
		v, err := vi.Values(testCase.key, testCase.key, testCase.value)
		if expected[i].returnErr {
			s.Error(err)
			continue
		}
		s.NoError(err)
		s.Len(v, 1)
		s.Equal(expected[i].value, v[0])
	}
}

func (s *QueryInterceptorSuite) TestStatusProcessFunc() {
	vi := NewValuesInterceptor(
		"test-namespace",
		searchattribute.TestNameTypeMap,
	)

	cases := []struct {
		key   string
		value interface{}
	}{
		{key: searchattribute.ExecutionStatus, value: "Completed"},
		{key: searchattribute.ExecutionStatus, value: int64(1)},
		{key: searchattribute.ExecutionStatus, value: "1"},
		{key: searchattribute.ExecutionStatus, value: int64(100)},
		{key: searchattribute.ExecutionStatus, value: "100"},
		{key: searchattribute.ExecutionStatus, value: "BadStatus"},
		{key: searchattribute.WorkflowID, value: "should not be modified"},
	}
	expected := []struct {
		value     string
		returnErr bool
	}{
		{value: "Completed", returnErr: false},
		{value: "Running", returnErr: false},
		{value: "1", returnErr: false},
		{value: "", returnErr: true},
		{value: "100", returnErr: false},
		{value: "BadStatus", returnErr: false},
		{value: "should not be modified", returnErr: false},
	}

	for i, testCase := range cases {
		v, err := vi.Values(testCase.key, testCase.key, testCase.value)
		if expected[i].returnErr {
			s.Error(err)
			continue
		}
		s.NoError(err)
		s.Len(v, 1)
		s.Equal(expected[i].value, v[0])
	}
}

func (s *QueryInterceptorSuite) TestDurationProcessFunc() {
	vi := NewValuesInterceptor(
		"test-namespace",
		searchattribute.TestNameTypeMap,
	)

	cases := []struct {
		key   string
		value interface{}
	}{
		{key: searchattribute.ExecutionDuration, value: "1"},
		{key: searchattribute.ExecutionDuration, value: int64(1)},
		{key: searchattribute.ExecutionDuration, value: "5h3m"},
		{key: searchattribute.ExecutionDuration, value: "00:00:01"},
		{key: searchattribute.ExecutionDuration, value: "00:00:61"},
		{key: searchattribute.ExecutionDuration, value: "bad value"},
		{key: searchattribute.WorkflowID, value: "should not be modified"},
	}
	expected := []struct {
		value     interface{}
		returnErr bool
	}{
		{value: int64(1), returnErr: false},
		{value: int64(1), returnErr: false},
		{value: int64(18180000000000), returnErr: false},
		{value: int64(1000000000), returnErr: false},
		{value: nil, returnErr: true},
		{value: nil, returnErr: true},
		{value: "should not be modified", returnErr: false},
	}

	for i, testCase := range cases {
		v, err := vi.Values(testCase.key, testCase.key, testCase.value)
		if expected[i].returnErr {
			s.Error(err)
			var converterErr *query.ConverterError
			s.ErrorAs(err, &converterErr)
			continue
		}
		s.NoError(err)
		s.Len(v, 1)
		s.Equal(expected[i].value, v[0])
	}
}

// Verifies the nameInterceptor correctly transforms ScheduleID to WorkflowID
func (s *QueryInterceptorSuite) TestNameInterceptor_ScheduleIDToWorkflowID() {
	ni := s.createMockNameInterceptor(nil)

	fieldName, err := ni.Name(searchattribute.ScheduleID, query.FieldNameFilter)
	s.NoError(err)
	s.Equal(searchattribute.WorkflowID, fieldName)
}

// Ensures the valuesInterceptor applies the ScheduleID to WorkflowID transformation,
// including prepending the WorkflowIDPrefix.
func (s *QueryInterceptorSuite) TestValuesInterceptor_ScheduleIDToWorkflowID() {
	vi := NewValuesInterceptor(
		"test-namespace",
		searchattribute.TestNameTypeMap,
	)

	values, err := vi.Values(searchattribute.ScheduleID, searchattribute.WorkflowID, "test-schedule-id")
	s.NoError(err)
	s.Len(values, 1)
	s.Equal(primitives.ScheduleWorkflowIDPrefix+"test-schedule-id", values[0])

	values, err = vi.Values(searchattribute.ScheduleID,
		searchattribute.WorkflowID,
		"test-schedule-id-1",
		"test-schedule-id-2")
	s.NoError(err)
	s.Len(values, 2)
	s.Equal(primitives.ScheduleWorkflowIDPrefix+"test-schedule-id-1", values[0])
	s.Equal(primitives.ScheduleWorkflowIDPrefix+"test-schedule-id-2", values[1])
}

// Ensures the valuesInterceptor doesn't modify values when no transformation is needed.
func (s *QueryInterceptorSuite) TestValuesInterceptor_NoTransformation() {
	vi := NewValuesInterceptor(
		"test-namespace",
		searchattribute.TestNameTypeMapWithScheduleId,
	)

	values, err := vi.Values(searchattribute.ScheduleID, searchattribute.ScheduleID, "test-workflow-id")
	s.NoError(err)
	s.Len(values, 1)
	s.Equal("test-workflow-id", values[0])

	values, err = vi.Values(searchattribute.ScheduleID,
		searchattribute.ScheduleID,
		"test-workflow-id-1",
		"test-workflow-id-2")
	s.NoError(err)
	s.Len(values, 2)
	s.Equal("test-workflow-id-1", values[0])
	s.Equal("test-workflow-id-2", values[1])
}

func (s *QueryInterceptorSuite) createMockNameInterceptor(mapper searchattribute.Mapper) *nameInterceptor {
	return &nameInterceptor{
		namespace:                      "test-namespace",
		searchAttributesTypeMap:        searchattribute.TestNameTypeMap,
		searchAttributesMapperProvider: searchattribute.NewTestMapperProvider(mapper),
	}
}

func (s *QueryInterceptorSuite) TestResolveSearchAttributeAlias() {
	cases := []struct {
		name              string
		expectedFieldName string
		expectedFieldType enumspb.IndexedValueType
		expectedErr       bool
	}{
		{name: "MyCustomField", expectedFieldName: "MyCustomField", expectedFieldType: enumspb.INDEXED_VALUE_TYPE_KEYWORD, expectedErr: false},
		{name: "ExecutionStatus", expectedFieldName: "ExecutionStatus", expectedFieldType: enumspb.INDEXED_VALUE_TYPE_KEYWORD, expectedErr: false},
		{name: "ScheduledStartTime", expectedFieldName: "TemporalScheduledStartTime", expectedFieldType: enumspb.INDEXED_VALUE_TYPE_DATETIME, expectedErr: false},
		{name: "SchedulePaused", expectedFieldName: "TemporalSchedulePaused", expectedFieldType: enumspb.INDEXED_VALUE_TYPE_BOOL, expectedErr: false},
		{name: "TemporalPauseInfo", expectedFieldName: "TemporalPauseInfo", expectedFieldType: enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, expectedErr: false},
		{name: "NonExistentField", expectedFieldName: "", expectedFieldType: enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, expectedErr: true},
		{name: searchattribute.ScheduleID, expectedFieldName: searchattribute.WorkflowID, expectedFieldType: enumspb.INDEXED_VALUE_TYPE_KEYWORD, expectedErr: false},
	}

	ns := namespace.Name("test-namespace")
	saTypeMap := searchattribute.NewNameTypeMapStub(map[string]enumspb.IndexedValueType{
		"MyCustomField":   enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"ExecutionStatus": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	})
	mapper := customMapper{
		fieldToAlias: map[string]string{
			"MyCustomField":   "some-alias",
			"ExecutionStatus": "some-alias2",
		},
		aliasToField: map[string]string{
			"some-alias":  "MyCustomField",
			"some-alias2": "ExecutionStatus",
		},
	}
	mapperProvider := searchattribute.NewTestMapperProvider(&mapper)

	for _, tc := range cases {
		s.Run(tc.name, func() {
			fieldName, fieldType, err := resolveSearchAttributeAlias(tc.name, ns, mapperProvider, saTypeMap)
			s.Equal(tc.expectedFieldName, fieldName)
			s.Equal(tc.expectedFieldType, fieldType)
			s.Equal(tc.expectedErr, err != nil)
		})
	}
}

type customMapper struct {
	fieldToAlias map[string]string
	aliasToField map[string]string
}

func (m *customMapper) GetAlias(fieldName string, ns string) (string, error) {
	if fn, ok := m.fieldToAlias[fieldName]; ok {
		return fn, nil
	}
	return "", serviceerror.NewInvalidArgument("invalid field name")
}

func (m *customMapper) GetFieldName(alias string, ns string) (string, error) {
	if fn, ok := m.aliasToField[alias]; ok {
		return fn, nil
	}
	return "", serviceerror.NewInvalidArgument("invalid alias")
}
