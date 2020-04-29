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

package filestore

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/util"
)

const (
	testFileMode = os.FileMode(0600)
	testDirMode  = os.FileMode(0700)
)

type UtilSuite struct {
	*require.Assertions
	suite.Suite
}

func TestUtilSuite(t *testing.T) {
	suite.Run(t, new(UtilSuite))
}

func (s *UtilSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *UtilSuite) TestEncodeDecodeHistoryBatches() {
	historyBatches := []*shared.History{
		&shared.History{
			Events: []*shared.HistoryEvent{
				&shared.HistoryEvent{
					EventId: common.Int64Ptr(common.FirstEventID),
					Version: common.Int64Ptr(1),
				},
			},
		},
		&shared.History{
			Events: []*shared.HistoryEvent{
				&shared.HistoryEvent{
					EventId:   common.Int64Ptr(common.FirstEventID + 1),
					Timestamp: common.Int64Ptr(time.Now().UnixNano()),
					Version:   common.Int64Ptr(1),
				},
				&shared.HistoryEvent{
					EventId: common.Int64Ptr(common.FirstEventID + 2),
					Version: common.Int64Ptr(2),
					DecisionTaskStartedEventAttributes: &shared.DecisionTaskStartedEventAttributes{
						Identity: common.StringPtr("some random identity"),
					},
				},
			},
		},
	}

	encodedHistoryBatches, err := encode(historyBatches)
	s.NoError(err)

	decodedHistoryBatches, err := decodeHistoryBatches(encodedHistoryBatches)
	s.NoError(err)
	s.Equal(historyBatches, decodedHistoryBatches)
}

func (s *UtilSuite) TestValidateDirPath() {
	dir, err := ioutil.TempDir("", "TestValidateDirPath")
	s.NoError(err)
	defer os.RemoveAll(dir)
	s.assertDirectoryExists(dir)
	filename := "test-file-name"
	s.createFile(dir, filename)
	fpath := filepath.Join(dir, filename)

	testCases := []struct {
		dirPath     string
		expectedErr error
	}{
		{
			dirPath:     "",
			expectedErr: errEmptyDirectoryPath,
		},
		{
			dirPath:     "/absolute/path",
			expectedErr: nil,
		},
		{
			dirPath:     "relative/path",
			expectedErr: nil,
		},
		{
			dirPath:     dir,
			expectedErr: nil,
		},
		{
			dirPath:     fpath,
			expectedErr: util.ErrDirectoryExpected,
		},
	}

	for _, tc := range testCases {
		s.Equal(tc.expectedErr, validateDirPath(tc.dirPath))
	}
}

func (s *UtilSuite) TestconstructHistoryFilename() {
	testCases := []struct {
		domainID             string
		workflowID           string
		runID                string
		closeFailoverVersion int64
		expectBuiltName      string
	}{
		{
			domainID:             "testDomainID",
			workflowID:           "testWorkflowID",
			runID:                "testRunID",
			closeFailoverVersion: 5,
			expectBuiltName:      "17971674567288329890367046253745284795510285995943906173973_5.history",
		},
	}

	for _, tc := range testCases {
		filename := constructHistoryFilename(tc.domainID, tc.workflowID, tc.runID, tc.closeFailoverVersion)
		s.Equal(tc.expectBuiltName, filename)
	}
}

func (s *UtilSuite) TestExtractCloseFailoverVersion() {
	testCases := []struct {
		filename        string
		expectedVersion int64
		expectedErr     bool
	}{
		{
			filename:        "17971674567288329890367046253745284795510285995943906173973_5.history",
			expectedVersion: 5,
			expectedErr:     false,
		},
		{
			filename:    "history",
			expectedErr: true,
		},
		{
			filename:    "some.random.filename",
			expectedErr: true,
		},
		{
			filename:        "some-random_101.filename",
			expectedVersion: 101,
			expectedErr:     false,
		},
		{
			filename:        "random_-100.filename",
			expectedVersion: -100,
			expectedErr:     false,
		},
	}

	for _, tc := range testCases {
		version, err := extractCloseFailoverVersion(tc.filename)
		if tc.expectedErr {
			s.Error(err)
		} else {
			s.NoError(err)
			s.Equal(tc.expectedVersion, version)
		}
	}
}

func (s *UtilSuite) TestHistoryMutated() {
	testCases := []struct {
		historyBatches []*shared.History
		request        *archiver.ArchiveHistoryRequest
		isLast         bool
		isMutated      bool
	}{
		{
			historyBatches: []*shared.History{
				&shared.History{
					Events: []*shared.HistoryEvent{
						&shared.HistoryEvent{
							Version: common.Int64Ptr(15),
						},
					},
				},
			},
			request: &archiver.ArchiveHistoryRequest{
				CloseFailoverVersion: 3,
			},
			isMutated: true,
		},
		{
			historyBatches: []*shared.History{
				&shared.History{
					Events: []*shared.HistoryEvent{
						&shared.HistoryEvent{
							EventId: common.Int64Ptr(33),
							Version: common.Int64Ptr(10),
						},
					},
				},
				&shared.History{
					Events: []*shared.HistoryEvent{
						&shared.HistoryEvent{
							EventId: common.Int64Ptr(49),
							Version: common.Int64Ptr(10),
						},
						&shared.HistoryEvent{
							EventId: common.Int64Ptr(50),
							Version: common.Int64Ptr(10),
						},
					},
				},
			},
			request: &archiver.ArchiveHistoryRequest{
				CloseFailoverVersion: 10,
				NextEventID:          34,
			},
			isLast:    true,
			isMutated: true,
		},
		{
			historyBatches: []*shared.History{
				&shared.History{
					Events: []*shared.HistoryEvent{
						&shared.HistoryEvent{
							Version: common.Int64Ptr(9),
						},
					},
				},
			},
			request: &archiver.ArchiveHistoryRequest{
				CloseFailoverVersion: 10,
			},
			isLast:    true,
			isMutated: true,
		},
		{
			historyBatches: []*shared.History{
				&shared.History{
					Events: []*shared.HistoryEvent{
						&shared.HistoryEvent{
							EventId: common.Int64Ptr(20),
							Version: common.Int64Ptr(10),
						},
					},
				},
				&shared.History{
					Events: []*shared.HistoryEvent{
						&shared.HistoryEvent{
							EventId: common.Int64Ptr(33),
							Version: common.Int64Ptr(10),
						},
					},
				},
			},
			request: &archiver.ArchiveHistoryRequest{
				CloseFailoverVersion: 10,
				NextEventID:          34,
			},
			isLast:    true,
			isMutated: false,
		},
	}
	for _, tc := range testCases {
		s.Equal(tc.isMutated, historyMutated(tc.request, tc.historyBatches, tc.isLast))
	}
}

func (s *UtilSuite) TestSerializeDeserializeGetHistoryToken() {
	token := &getHistoryToken{
		CloseFailoverVersion: 101,
		NextBatchIdx:         20,
	}

	serializedToken, err := serializeToken(token)
	s.Nil(err)

	deserializedToken, err := deserializeGetHistoryToken(serializedToken)
	s.Nil(err)
	s.Equal(token, deserializedToken)
}

func (s *UtilSuite) createFile(dir string, filename string) {
	err := ioutil.WriteFile(filepath.Join(dir, filename), []byte("file contents"), testFileMode)
	s.Nil(err)
}

func (s *UtilSuite) assertDirectoryExists(path string) {
	exists, err := util.DirectoryExists(path)
	s.NoError(err)
	s.True(exists)
}
