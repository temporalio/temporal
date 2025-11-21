package elasticsearch

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
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
		searchattribute.TestEsNameTypeMap(),
		nil,
	)

	cases := []struct {
		key   string
		value interface{}
	}{
		{key: sadefs.StartTime, value: int64(1528358645123456789)},
		{key: sadefs.CloseTime, value: "2018-06-07T15:04:05+07:00"},
		{key: sadefs.ExecutionTime, value: "some invalid time string"},
		{key: sadefs.WorkflowID, value: "should not be modified"},
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
		searchattribute.TestEsNameTypeMap(),
		nil,
	)

	cases := []struct {
		key   string
		value interface{}
	}{
		{key: sadefs.ExecutionStatus, value: "Completed"},
		{key: sadefs.ExecutionStatus, value: int64(1)},
		{key: sadefs.ExecutionStatus, value: "1"},
		{key: sadefs.ExecutionStatus, value: int64(100)},
		{key: sadefs.ExecutionStatus, value: "100"},
		{key: sadefs.ExecutionStatus, value: "BadStatus"},
		{key: sadefs.WorkflowID, value: "should not be modified"},
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
		searchattribute.TestEsNameTypeMap(),
		nil,
	)

	cases := []struct {
		key   string
		value interface{}
	}{
		{key: sadefs.ExecutionDuration, value: "1"},
		{key: sadefs.ExecutionDuration, value: int64(1)},
		{key: sadefs.ExecutionDuration, value: "5h3m"},
		{key: sadefs.ExecutionDuration, value: "00:00:01"},
		{key: sadefs.ExecutionDuration, value: "00:00:61"},
		{key: sadefs.ExecutionDuration, value: "bad value"},
		{key: sadefs.WorkflowID, value: "should not be modified"},
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

	fieldName, err := ni.Name(sadefs.ScheduleID, query.FieldNameFilter)
	s.NoError(err)
	s.Equal(sadefs.WorkflowID, fieldName)
}

// Ensures the valuesInterceptor applies the ScheduleID to WorkflowID transformation,
// including prepending the WorkflowIDPrefix.
func (s *QueryInterceptorSuite) TestValuesInterceptor_ScheduleIDToWorkflowID() {
	vi := NewValuesInterceptor(
		"test-namespace",
		searchattribute.TestEsNameTypeMap(),
		nil,
	)

	values, err := vi.Values(sadefs.ScheduleID, sadefs.WorkflowID, "test-schedule-id")
	s.NoError(err)
	s.Len(values, 1)
	s.Equal(primitives.ScheduleWorkflowIDPrefix+"test-schedule-id", values[0])

	values, err = vi.Values(sadefs.ScheduleID,
		sadefs.WorkflowID,
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
		searchattribute.TestEsNameTypeMapWithScheduleID(),
		nil,
	)

	values, err := vi.Values(sadefs.ScheduleID, sadefs.ScheduleID, "test-workflow-id")
	s.NoError(err)
	s.Len(values, 1)
	s.Equal("test-workflow-id", values[0])

	values, err = vi.Values(sadefs.ScheduleID,
		sadefs.ScheduleID,
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
		searchAttributesTypeMap:        searchattribute.TestEsNameTypeMap(),
		searchAttributesMapperProvider: searchattribute.NewTestMapperProvider(mapper),
	}
}
