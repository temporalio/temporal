package gcloud

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type queryParserSuite struct {
	*require.Assertions
	suite.Suite
	parser QueryParser
}

func TestQueryParserSuite(t *testing.T) {
	suite.Run(t, new(queryParserSuite))
}

func (s *queryParserSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.parser = NewQueryParser()
}

func (s *queryParserSuite) TestParse_TimeBasedQuery_Success() {
	query := "CloseTime = '2020-02-05T11:00:00Z' AND SearchPrecision = 'Day'"
	parsed, err := s.parser.Parse(query)
	s.NoError(err)
	s.NotNil(parsed)
	s.Nil(parsed.workflowID)
	s.Equal("Day", *parsed.searchPrecision)
	expectedTime, _ := time.Parse(time.RFC3339, "2020-02-05T11:00:00Z")
	s.Equal(expectedTime, parsed.closeTime)
}

func (s *queryParserSuite) TestParse_TimeBasedQuery_RunID_Error() {
	query := "CloseTime = '2020-02-05T11:00:00Z' AND SearchPrecision = 'Day' AND RunId = 'some-run-id'"
	parsed, err := s.parser.Parse(query)
	s.Error(err)
	s.Nil(parsed)
	s.Equal(errors.New("RunId is not supported when searching for a StartTime or CloseTime"), err)
}

func (s *queryParserSuite) TestParse_WorkflowIDBasedQuery_Success() {
	query := "WorkflowId = 'some-workflow-id'"
	parsed, err := s.parser.Parse(query)
	s.NoError(err)
	s.NotNil(parsed)
	s.Equal("some-workflow-id", *parsed.workflowID)
	s.True(parsed.startTime.IsZero())
	s.True(parsed.closeTime.IsZero())
}

func (s *queryParserSuite) TestParse_WorkflowIDBasedQuery_WithCloseTime_Success() {
	query := "WorkflowId = 'some-workflow-id' AND CloseTime = '2020-02-05T11:00:00Z' AND SearchPrecision = 'Day'"
	parsed, err := s.parser.Parse(query)
	s.NoError(err)
	s.NotNil(parsed)
	s.Equal("some-workflow-id", *parsed.workflowID)
	expectedTime, _ := time.Parse(time.RFC3339, "2020-02-05T11:00:00Z")
	s.Equal(expectedTime, parsed.closeTime)
	s.Equal("Day", *parsed.searchPrecision)
}

func (s *queryParserSuite) TestParse_WorkflowIDBasedQuery_WithCloseTimeMissingPrecision_Error() {
	query := "WorkflowId = 'some-workflow-id' AND CloseTime = '2020-02-05T11:00:00Z'"
	parsed, err := s.parser.Parse(query)
	s.Error(err)
	s.Nil(parsed)
	s.Equal(errors.New("SearchPrecision is required when also filtering by CloseTime"), err)
}

func (s *queryParserSuite) TestParse_WorkflowIDBasedQuery_WithStartTime_Error() {
	query := "WorkflowId = 'some-workflow-id' AND StartTime = '2020-02-05T11:00:00Z' AND SearchPrecision = 'Day'"
	parsed, err := s.parser.Parse(query)
	s.Error(err)
	s.Nil(parsed)
	s.Equal(errors.New("StartTime is not supported when searching for a WorkflowId"), err)
}
