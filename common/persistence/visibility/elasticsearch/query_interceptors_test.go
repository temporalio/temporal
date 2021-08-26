package elasticsearch

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/searchattribute"
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
	vi := newValuesInterceptor()

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
		{value: "some invalid time string", returnErr: false},
		{value: "should not be modified", returnErr: false},
	}

	for i, testCase := range cases {
		v, err := vi.Values(testCase.key, testCase.value)
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
	vi := newValuesInterceptor()

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
		{value: "", returnErr: false},
		{value: "100", returnErr: false},
		{value: "BadStatus", returnErr: false},
		{value: "should not be modified", returnErr: false},
	}

	for i, testCase := range cases {
		v, err := vi.Values(testCase.key, testCase.value)
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
	vi := newValuesInterceptor()

	cases := []struct {
		key   string
		value interface{}
	}{
		{key: searchattribute.ExecutionDuration, value: "1"},
		{key: searchattribute.ExecutionDuration, value: 1},
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
		{value: "1", returnErr: false},
		{value: 1, returnErr: false},
		{value: int64(18180000000000), returnErr: false},
		{value: int64(1000000000), returnErr: false},
		{value: nil, returnErr: true},
		{value: "bad value", returnErr: false},
		{value: "should not be modified", returnErr: false},
	}

	for i, testCase := range cases {
		v, err := vi.Values(testCase.key, testCase.value)
		if expected[i].returnErr {
			s.Error(err)
			continue
		}
		s.NoError(err)
		s.Len(v, 1)
		s.Equal(expected[i].value, v[0])
	}
}
