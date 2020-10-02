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

package gcloud

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	historypb "go.temporal.io/api/history/v1"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/codec"
)

func (s *utilSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}
func TestUtilSuite(t *testing.T) {
	suite.Run(t, new(utilSuite))
}

type utilSuite struct {
	*require.Assertions
	suite.Suite
}

func (s *utilSuite) TestEncodeDecodeHistoryBatches() {
	now := time.Date(2020, 8, 22, 1, 2, 3, 4, time.UTC)
	historyBatches := []*historypb.History{
		{
			Events: []*historypb.HistoryEvent{
				{
					EventId: common.FirstEventID,
					Version: 1,
				},
			},
		},
		{
			Events: []*historypb.HistoryEvent{
				{
					EventId:   common.FirstEventID + 1,
					EventTime: &now,
					Version:   1,
				},
				{
					EventId: common.FirstEventID + 2,
					Version: 2,
					Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
						Identity: "some random identity",
					}},
				},
			},
		},
	}

	encoder := codec.NewJSONPBEncoder()
	encodedHistoryBatches, err := encoder.EncodeHistories(historyBatches)
	s.NoError(err)

	decodedHistoryBatches, err := encoder.DecodeHistories(encodedHistoryBatches)
	s.NoError(err)
	s.Equal(historyBatches, decodedHistoryBatches)
}

func (s *utilSuite) TestconstructHistoryFilename() {
	testCases := []struct {
		namespaceID          string
		workflowID           string
		runID                string
		closeFailoverVersion int64
		expectBuiltName      string
	}{
		{
			namespaceID:          "testNamespaceID",
			workflowID:           "testWorkflowID",
			runID:                "testRunID",
			closeFailoverVersion: 5,
			expectBuiltName:      "11936904199538907273367046253745284795510285995943906173973_5_0.history",
		},
	}

	for _, tc := range testCases {
		filename := constructHistoryFilenameMultipart(tc.namespaceID, tc.workflowID, tc.runID, tc.closeFailoverVersion, 0)
		s.Equal(tc.expectBuiltName, filename)
	}
}

func (s *utilSuite) TestSerializeDeserializeGetHistoryToken() {
	token := &getHistoryToken{
		CloseFailoverVersion: 101,
		BatchIdxOffset:       20,
	}

	serializedToken, err := serializeToken(token)
	s.Nil(err)

	deserializedToken, err := deserializeGetHistoryToken(serializedToken)
	s.Nil(err)
	s.Equal(token, deserializedToken)
}

func (s *utilSuite) TestConstructHistoryFilenamePrefix() {
	s.Equal("67753999582745295208344541402884576509131521284625246243", constructHistoryFilenamePrefix("namespaceID", "workflowID", "runID"))
}

func (s *utilSuite) TestConstructHistoryFilenameMultipart() {
	s.Equal("67753999582745295208344541402884576509131521284625246243_-24_0.history", constructHistoryFilenameMultipart("namespaceID", "workflowID", "runID", -24, 0))
}

func (s *utilSuite) TestConstructVisibilityFilenamePrefix() {
	s.Equal("namespaceID/startTimeout", constructVisibilityFilenamePrefix("namespaceID", indexKeyStartTimeout))
}

func (s *utilSuite) TestConstructTimeBasedSearchKey() {
	t, _ := time.Parse(time.RFC3339, "2019-10-04T11:00:00+00:00")
	s.Equal("namespaceID/startTimeout_2019-10-04T", constructTimeBasedSearchKey("namespaceID", indexKeyStartTimeout, t, "Day"))
}

func (s *utilSuite) TestConstructVisibilityFilename() {
	s.Equal("namespaceID/startTimeout_1970-01-01T00:24:32Z_4346151385925082125_8344541402884576509_131521284625246243.visibility", constructVisibilityFilename("namespaceID", "workflowTypeName", "workflowID", "runID", indexKeyStartTimeout, time.Date(1970, 01, 01, 0, 24, 32, 0, time.UTC)))
}

func (s *utilSuite) TestWorkflowIdPrecondition() {
	testCases := []struct {
		workflowID     string
		fileName       string
		expectedResult bool
	}{
		{
			workflowID:     "4418294404690464320",
			fileName:       "closeTimeout_2020-02-27T09:42:28Z_12851121011173788097_4418294404690464320_15619178330501475177.visibility",
			expectedResult: true,
		},
		{
			workflowID:     "testWorkflowID",
			fileName:       "closeTimeout_2020-02-27T09:42:28Z_12851121011173788097_4418294404690464320_15619178330501475177.visibility",
			expectedResult: false,
		},
		{
			workflowID:     "",
			fileName:       "closeTimeout_2020-02-27T09:42:28Z_12851121011173788097_4418294404690464320_15619178330501475177.visibility",
			expectedResult: true,
		},
	}

	for _, testCase := range testCases {
		s.Equal(newWorkflowIDPrecondition(testCase.workflowID)(testCase.fileName), testCase.expectedResult)
	}

}

func (s *utilSuite) TestRunIdPrecondition() {
	testCases := []struct {
		workflowID     string
		runID          string
		fileName       string
		expectedResult bool
	}{
		{
			workflowID:     "4418294404690464320",
			runID:          "15619178330501475177",
			fileName:       "closeTimeout_2020-02-27T09:42:28Z_12851121011173788097_4418294404690464320_15619178330501475177.visibility",
			expectedResult: true,
		},
		{
			workflowID:     "4418294404690464320",
			runID:          "15619178330501475177",
			fileName:       "closeTimeout_2020-02-27T09:42:28Z_12851121011173788097_4418294404690464320_unkonwnRunID.visibility",
			expectedResult: false,
		},
		{
			workflowID:     "4418294404690464320",
			runID:          "",
			fileName:       "closeTimeout_2020-02-27T09:42:28Z_12851121011173788097_4418294404690464320_unkonwnRunID.visibility",
			expectedResult: true,
		},
	}

	for _, testCase := range testCases {
		s.Equal(newRunIDPrecondition(testCase.runID)(testCase.fileName), testCase.expectedResult)
	}

}

func (s *utilSuite) TestWorkflowTypeNamePrecondition() {
	testCases := []struct {
		workflowID       string
		runID            string
		workflowTypeName string
		fileName         string
		expectedResult   bool
	}{
		{
			workflowID:       "4418294404690464320",
			runID:            "15619178330501475177",
			workflowTypeName: "12851121011173788097",
			fileName:         "closeTimeout_2020-02-27T09:42:28Z_12851121011173788097_4418294404690464320_15619178330501475177.visibility",
			expectedResult:   true,
		},
		{
			workflowID:       "4418294404690464320",
			runID:            "15619178330501475177",
			workflowTypeName: "12851121011173788097",
			fileName:         "closeTimeout_2020-02-27T09:42:28Z_12851121011173788098_4418294404690464320_15619178330501475177.visibility",
			expectedResult:   false,
		},
		{
			workflowID:       "4418294404690464320",
			runID:            "15619178330501475177",
			workflowTypeName: "",
			fileName:         "closeTimeout_2020-02-27T09:42:28Z_unkownWorkflowTypeName_4418294404690464320_15619178330501475177.visibility",
			expectedResult:   true,
		},
	}

	for _, testCase := range testCases {
		s.Equal(newWorkflowTypeNamePrecondition(testCase.workflowTypeName)(testCase.fileName), testCase.expectedResult)
	}

}
