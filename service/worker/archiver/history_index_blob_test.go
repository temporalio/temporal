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
	"github.com/uber/cadence/common/blobstore/blob"
)

type HistoryIndexBlobSuite struct {
	*require.Assertions
	suite.Suite
}

func TestHistoryIndexBlobSuite(t *testing.T) {
	suite.Run(t, new(HistoryIndexBlobSuite))
}

func (s *HistoryIndexBlobSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *HistoryIndexBlobSuite) TestNewHistoryIndexBlobKey() {
	testCases := []struct {
		domainID       string
		workflowID     string
		runID          string
		expectError    bool
		expectBuiltKey string
	}{
		{
			domainID:    "",
			expectError: true,
		},
		{
			domainID:       "testDomainID",
			workflowID:     "testWorkflowID",
			runID:          "testRunID",
			expectError:    false,
			expectBuiltKey: "17971674567288329890367046253745284795510285995943906173973.index",
		},
	}

	for _, tc := range testCases {
		key, err := NewHistoryIndexBlobKey(tc.domainID, tc.workflowID, tc.runID)
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

func (s *HistoryIndexBlobSuite) TestAddVersion() {
	testCases := []struct {
		existingVersions     map[string]string
		closeFailoverVersion int64
		expectedBlob         *blob.Blob
	}{
		{
			existingVersions:     nil,
			closeFailoverVersion: 10,
			expectedBlob:         blob.NewBlob(nil, map[string]string{"10": ""}),
		},
		{
			existingVersions:     map[string]string{"10": ""},
			closeFailoverVersion: 10,
			expectedBlob:         nil,
		},
		{
			existingVersions:     map[string]string{"1": ""},
			closeFailoverVersion: 10,
			expectedBlob:         blob.NewBlob(nil, map[string]string{"1": "", "10": ""}),
		},
	}
	for _, tc := range testCases {
		indexBlob := addVersion(tc.closeFailoverVersion, tc.existingVersions)
		s.True(indexBlob.Equal(tc.expectedBlob))
	}
}

func (s *HistoryIndexBlobSuite) TestDeleteVersion() {
	testCases := []struct {
		existingVersions     map[string]string
		closeFailoverVersion int64
		expectedBlob         *blob.Blob
	}{
		{
			existingVersions:     nil,
			closeFailoverVersion: 10,
			expectedBlob:         nil,
		},
		{
			existingVersions:     map[string]string{"10": ""},
			closeFailoverVersion: 10,
			expectedBlob:         blob.NewBlob(nil, map[string]string{}),
		},
		{
			existingVersions:     map[string]string{"1": ""},
			closeFailoverVersion: 10,
			expectedBlob:         nil,
		},
		{
			existingVersions:     map[string]string{"1": "", "10": ""},
			closeFailoverVersion: 10,
			expectedBlob:         blob.NewBlob(nil, map[string]string{"1": ""}),
		},
	}
	for _, tc := range testCases {
		indexBlob := deleteVersion(tc.closeFailoverVersion, tc.existingVersions)
		s.True(indexBlob.Equal(tc.expectedBlob))
	}
}

func (s *HistoryIndexBlobSuite) TestGetHighestVersion() {
	testCases := []struct {
		tags            map[string]string
		expectError     bool
		expectedVersion int64
	}{
		{
			tags:        nil,
			expectError: true,
		},
		{
			tags:        map[string]string{"foo": "bar"},
			expectError: true,
		},
		{
			tags:            map[string]string{"1": ""},
			expectError:     false,
			expectedVersion: 1,
		},
		{
			tags:            map[string]string{"1": "", "foo": ""},
			expectError:     false,
			expectedVersion: 1,
		},
		{
			tags:            map[string]string{"1": "", "foo": "", "10": "", "7": ""},
			expectError:     false,
			expectedVersion: 10,
		},
	}
	for _, tc := range testCases {
		version, err := GetHighestVersion(tc.tags)
		if tc.expectError {
			s.Error(err)
			s.Nil(version)
		} else {
			s.NoError(err)
			s.Equal(tc.expectedVersion, *version)
		}
	}
}
