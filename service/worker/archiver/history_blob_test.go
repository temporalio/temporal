// Copyright (c) 2017 Uber Technologies, Inc.
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

package archiver

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber/cadence/common"
)

type HistoryBlobSuite struct {
	*require.Assertions
	suite.Suite
}

func TestHistoryBlobSuite(t *testing.T) {
	suite.Run(t, new(HistoryBlobSuite))
}

func (s *HistoryBlobSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *HistoryBlobSuite) TestNewHistoryBlobKey() {
	testCases := []struct {
		domainID             string
		workflowID           string
		runID                string
		closeFailoverVersion int64
		pageToken            int
		expectError          bool
		expectBuiltKey       string
	}{
		{
			domainID:    "",
			expectError: true,
		},
		{
			domainID:             "testDomainID",
			workflowID:           "testWorkflowID",
			runID:                "testRunID",
			closeFailoverVersion: 5,
			pageToken:            common.FirstBlobPageToken,
			expectError:          false,
			expectBuiltKey:       "17971674567288329890367046253745284795510285995943906173973_5_1.history",
		},
		{
			domainID:    "testDomainID",
			workflowID:  "testWorkflowID",
			runID:       "testRunID",
			pageToken:   -1,
			expectError: true,
		},
	}

	for _, tc := range testCases {
		key, err := NewHistoryBlobKey(tc.domainID, tc.workflowID, tc.runID, tc.closeFailoverVersion, tc.pageToken)
		if tc.expectError {
			s.Error(err)
			s.Nil(key)
		} else {
			s.NoError(err)
			s.NotNil(key)
			s.Equal(tc.expectBuiltKey, key.String())
		}
	}
}

func (s *HistoryBlobSuite) TestConvertHeaderToTags() {
	testCases := []struct {
		header     *HistoryBlobHeader
		expectTags map[string]string
	}{
		{
			header:     nil,
			expectTags: map[string]string{},
		},
		{
			header:     &HistoryBlobHeader{},
			expectTags: map[string]string{},
		},
		{
			header: &HistoryBlobHeader{
				DomainID: nil,
			},
			expectTags: map[string]string{},
		},
		{
			header: &HistoryBlobHeader{
				DomainID: common.StringPtr("test-domain-id"),
			},
			expectTags: map[string]string{"domain_id": "test-domain-id"},
		},
		{
			header: &HistoryBlobHeader{
				EventCount: nil,
			},
			expectTags: map[string]string{},
		},
		{
			header: &HistoryBlobHeader{
				DomainID:   common.StringPtr("test-domain-id"),
				EventCount: common.Int64Ptr(9),
			},
			expectTags: map[string]string{
				"domain_id":   "test-domain-id",
				"event_count": "9",
			},
		},
	}

	for _, tc := range testCases {
		tags, err := ConvertHeaderToTags(tc.header)
		s.NoError(err)
		s.Equal(tc.expectTags, tags)
	}
}

func (s *HistoryBlobSuite) TestIsLast() {
	testCases := []struct {
		header *HistoryBlobHeader
		isLast bool
	}{
		{
			header: &HistoryBlobHeader{IsLast: common.BoolPtr(true)},
			isLast: true,
		},
		{
			header: &HistoryBlobHeader{IsLast: common.BoolPtr(false)},
			isLast: false,
		},
		{
			header: &HistoryBlobHeader{},
			isLast: false,
		},
	}
	for _, tc := range testCases {
		tags, err := ConvertHeaderToTags(tc.header)
		s.NoError(err)
		s.Equal(tc.isLast, IsLast(tags))
	}
}
