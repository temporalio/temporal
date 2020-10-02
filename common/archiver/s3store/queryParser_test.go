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

package s3store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/primitives/timestamp"
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

func (s *queryParserSuite) TestParseWorkflowIDAndWorkflowTypeName() {
	testCases := []struct {
		query       string
		expectErr   bool
		parsedQuery *parsedQuery
	}{
		{
			query:     "WorkflowId = \"random workflowID\"",
			expectErr: false,
			parsedQuery: &parsedQuery{
				workflowID: convert.StringPtr("random workflowID"),
			},
		},
		{
			query:     "WorkflowTypeName = \"random workflowTypeName\"",
			expectErr: false,
			parsedQuery: &parsedQuery{
				workflowTypeName: convert.StringPtr("random workflowTypeName"),
			},
		},
		{
			query:     "WorkflowId = \"random workflowID\" and WorkflowTypeName = \"random workflowTypeName\"",
			expectErr: true,
		},
		{
			query:     "WorkflowId = \"random workflowID\" and WorkflowId = \"random workflowID\"",
			expectErr: true,
		},
		{
			query:     "RunId = \"random runID\"",
			expectErr: true,
		},
		{
			query:     "WorkflowId = 'random workflowID'",
			expectErr: false,
			parsedQuery: &parsedQuery{
				workflowID: convert.StringPtr("random workflowID"),
			},
		},
		{
			query:     "(WorkflowId = \"random workflowID\")",
			expectErr: false,
			parsedQuery: &parsedQuery{
				workflowID: convert.StringPtr("random workflowID"),
			},
		},
		{
			query:     "runId = random workflowID",
			expectErr: true,
		},
		{
			query:     "WorkflowId = \"random workflowID\" or WorkflowId = \"another workflowID\"",
			expectErr: true,
		},
		{
			query:     "WorkflowId = \"random workflowID\" or runId = \"random runID\"",
			expectErr: true,
		},
		{
			query:     "workflowid = \"random workflowID\"",
			expectErr: true,
		},
		{
			query:     "runId > \"random workflowID\"",
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		parsedQuery, err := s.parser.Parse(tc.query)
		if tc.expectErr {
			s.Error(err)
			continue
		}
		s.NoError(err)
		s.Equal(tc.parsedQuery.workflowID, parsedQuery.workflowID)
		s.Equal(tc.parsedQuery.workflowTypeName, parsedQuery.workflowTypeName)

	}
}

func (s *queryParserSuite) TestParsePrecision() {
	commonQueryPart := "WorkflowId = \"random workflowID\" AND "
	testCases := []struct {
		query       string
		expectErr   bool
		parsedQuery *parsedQuery
	}{
		{
			query:     commonQueryPart + "CloseTime = 1000 and SearchPrecision = 'Day'",
			expectErr: false,
			parsedQuery: &parsedQuery{
				searchPrecision: convert.StringPtr(PrecisionDay),
			},
		},
		{
			query:     commonQueryPart + "CloseTime = 1000 and SearchPrecision = 'Hour'",
			expectErr: false,
			parsedQuery: &parsedQuery{
				searchPrecision: convert.StringPtr(PrecisionHour),
			},
		},
		{
			query:     commonQueryPart + "CloseTime = 1000 and SearchPrecision = 'Minute'",
			expectErr: false,
			parsedQuery: &parsedQuery{
				searchPrecision: convert.StringPtr(PrecisionMinute),
			},
		},
		{
			query:     commonQueryPart + "StartTime = 1000 and SearchPrecision = 'Second'",
			expectErr: false,
			parsedQuery: &parsedQuery{
				searchPrecision: convert.StringPtr(PrecisionSecond),
			},
		},
		{
			query:     commonQueryPart + "SearchPrecision = 'Second'",
			expectErr: true,
		},
		{
			query:     commonQueryPart + "SearchPrecision = 'Invalid string'",
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		parsedQuery, err := s.parser.Parse(tc.query)
		if tc.expectErr {
			s.Error(err)
			continue
		}
		s.NoError(err)
		s.Equal(tc.parsedQuery.searchPrecision, parsedQuery.searchPrecision)
	}
}

func (s *queryParserSuite) TestParseCloseTime() {
	commonQueryPart := "WorkflowId = \"random workflowID\" AND SearchPrecision = 'Day' AND "

	testCases := []struct {
		query       string
		expectErr   bool
		parsedQuery *parsedQuery
	}{
		{
			query:     commonQueryPart + "CloseTime = 1000",
			expectErr: false,
			parsedQuery: &parsedQuery{
				closeTime: timestamp.TimePtr(time.Unix(0, 1000).UTC()),
			},
		},
		{
			query:     commonQueryPart + "CloseTime = \"2019-01-01T11:11:11Z\"",
			expectErr: false,
			parsedQuery: &parsedQuery{
				closeTime: timestamp.TimePtr(time.Date(2019, 1, 1, 11, 11, 11, 0, time.UTC)),
			},
		},
		{
			query:     commonQueryPart + "closeTime = 2000",
			expectErr: true,
		},
		{
			query:     commonQueryPart + "CloseTime > \"2019-01-01 00:00:00\"",
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		parsedQuery, err := s.parser.Parse(tc.query)
		if tc.expectErr {
			s.Error(err)
			continue
		}
		s.NoError(err)
		s.Equal(tc.parsedQuery.closeTime, parsedQuery.closeTime)

	}
}

func (s *queryParserSuite) TestParseStartTime() {
	commonQueryPart := "WorkflowId = \"random workflowID\" AND SearchPrecision = 'Day' AND "

	testCases := []struct {
		query       string
		expectErr   bool
		parsedQuery *parsedQuery
	}{
		{
			query:     commonQueryPart + "StartTime = 1000",
			expectErr: false,
			parsedQuery: &parsedQuery{
				startTime: timestamp.TimePtr(time.Unix(0, 1000)),
			},
		},
		{
			query:     commonQueryPart + "StartTime = \"2019-01-01T11:11:11Z\"",
			expectErr: false,
			parsedQuery: &parsedQuery{
				startTime: timestamp.TimePtr(time.Date(2019, 1, 1, 11, 11, 11, 0, time.UTC)),
			},
		},
		{
			query:     commonQueryPart + "startTime = 2000",
			expectErr: true,
		},
		{
			query:     commonQueryPart + "StartTime > \"2019-01-01 00:00:00\"",
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		parsedQuery, err := s.parser.Parse(tc.query)
		if tc.expectErr {
			s.Error(err)
			continue
		}
		s.NoError(err)
		s.Equal(tc.parsedQuery.closeTime, parsedQuery.closeTime)
	}
}
