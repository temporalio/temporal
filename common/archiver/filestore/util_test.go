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

package filestore

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.temporal.io/server/tests/testhelper"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/codec"
)

const (
	testDirMode  = os.FileMode(0700)
	testFileMode = os.FileMode(0600)
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

func (s *UtilSuite) TestFileExists() {
	dir := testhelper.MkdirTemp(s.T(), "", "TestFileExists")
	s.assertDirectoryExists(dir)

	exists, err := fileExists(dir)
	s.Error(err)
	s.False(exists)

	filename := "test-file-name"
	exists, err = fileExists(filepath.Join(dir, filename))
	s.NoError(err)
	s.False(exists)

	s.createFile(dir, filename)
	exists, err = fileExists(filepath.Join(dir, filename))
	s.NoError(err)
	s.True(exists)
}

func (s *UtilSuite) TestDirectoryExists() {
	dir := testhelper.MkdirTemp(s.T(), "", "TestDirectoryExists")
	s.assertDirectoryExists(dir)

	subdir := "subdir"
	exists, err := directoryExists(filepath.Join(dir, subdir))
	s.NoError(err)
	s.False(exists)

	filename := "test-file-name"
	s.createFile(dir, filename)
	fpath := filepath.Join(dir, filename)
	exists, err = directoryExists(fpath)
	s.Error(err)
	s.False(exists)
}

func (s *UtilSuite) TestMkdirAll() {
	dir := testhelper.MkdirTemp(s.T(), "", "TestMkdirAll")
	s.assertDirectoryExists(dir)

	s.NoError(mkdirAll(dir, testDirMode))
	s.assertDirectoryExists(dir)

	subDirPath := filepath.Join(dir, "subdir_1", "subdir_2", "subdir_3")
	s.assertDirectoryNotExists(subDirPath)
	s.NoError(mkdirAll(subDirPath, testDirMode))
	s.assertDirectoryExists(subDirPath)
	s.assertCorrectFileMode(subDirPath)

	filename := "test-file-name"
	s.createFile(dir, filename)
	fpath := filepath.Join(dir, filename)
	s.Error(mkdirAll(fpath, testDirMode))
}

func (s *UtilSuite) TestWriteFile() {
	dir := testhelper.MkdirTemp(s.T(), "", "TestWriteFile")
	s.assertDirectoryExists(dir)

	filename := "test-file-name"
	fpath := filepath.Join(dir, filename)
	s.NoError(writeFile(fpath, []byte("file body 1"), testFileMode))
	s.assertFileExists(fpath)
	s.assertCorrectFileMode(fpath)

	s.NoError(writeFile(fpath, []byte("file body 2"), testFileMode))
	s.assertFileExists(fpath)
	s.assertCorrectFileMode(fpath)

	s.Error(writeFile(dir, []byte(""), testFileMode))
	s.assertFileExists(fpath)
}

func (s *UtilSuite) TestReadFile() {
	dir := testhelper.MkdirTemp(s.T(), "", "TestReadFile")
	s.assertDirectoryExists(dir)

	filename := "test-file-name"
	fpath := filepath.Join(dir, filename)
	data, err := readFile(fpath)
	s.Error(err)
	s.Empty(data)

	err = writeFile(fpath, []byte("file contents"), testFileMode)
	s.NoError(err)
	data, err = readFile(fpath)
	s.NoError(err)
	s.Equal("file contents", string(data))
}

func (s *UtilSuite) TestListFilesByPrefix() {
	dir := testhelper.MkdirTemp(s.T(), "", "TestListFiles")
	s.assertDirectoryExists(dir)

	filename := "test-file-name"
	fpath := filepath.Join(dir, filename)
	files, err := listFilesByPrefix(fpath, "test-")
	s.Error(err)
	s.Nil(files)

	subDirPath := filepath.Join(dir, "subdir")
	s.NoError(mkdirAll(subDirPath, testDirMode))
	s.assertDirectoryExists(subDirPath)
	expectedFileNames := []string{"file_1", "file_2", "file_3"}
	for _, f := range expectedFileNames {
		s.createFile(dir, f)
	}
	for _, f := range []string{"randomFile", "fileWithOtherPrefix"} {
		s.createFile(dir, f)
	}
	actualFileNames, err := listFilesByPrefix(dir, "file_")
	s.NoError(err)
	s.Equal(len(expectedFileNames), len(actualFileNames))
}

func (s *UtilSuite) TestEncodeDecodeHistoryBatches() {
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

func (s *UtilSuite) TestValidateDirPath() {
	dir := testhelper.MkdirTemp(s.T(), "", "TestValidateDirPath")
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
			expectedErr: errDirectoryExpected,
		},
	}

	for _, tc := range testCases {
		s.Equal(tc.expectedErr, validateDirPath(tc.dirPath))
	}
}

func (s *UtilSuite) TestconstructHistoryFilename() {
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
			expectBuiltName:      "11936904199538907273367046253745284795510285995943906173973_5.history",
		},
	}

	for _, tc := range testCases {
		filename := constructHistoryFilename(tc.namespaceID, tc.workflowID, tc.runID, tc.closeFailoverVersion)
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
			filename:        "11936904199538907273367046253745284795510285995943906173973_5.history",
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
		historyBatches []*historypb.History
		request        *archiver.ArchiveHistoryRequest
		isLast         bool
		isMutated      bool
	}{
		{
			historyBatches: []*historypb.History{
				{
					Events: []*historypb.HistoryEvent{
						{
							Version: 15,
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
			historyBatches: []*historypb.History{
				{
					Events: []*historypb.HistoryEvent{
						{
							EventId: 33,
							Version: 10,
						},
					},
				},
				{
					Events: []*historypb.HistoryEvent{
						{
							EventId: 49,
							Version: 10,
						},
						{
							EventId: 50,
							Version: 10,
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
			historyBatches: []*historypb.History{
				{
					Events: []*historypb.HistoryEvent{
						{
							Version: 9,
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
			historyBatches: []*historypb.History{
				{
					Events: []*historypb.HistoryEvent{
						{
							EventId: 20,
							Version: 10,
						},
					},
				},
				{
					Events: []*historypb.HistoryEvent{
						{
							EventId: 33,
							Version: 10,
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
	err := os.WriteFile(filepath.Join(dir, filename), []byte("file contents"), testFileMode)
	s.Nil(err)
}

func (s *UtilSuite) assertFileExists(filepath string) {
	exists, err := fileExists(filepath)
	s.NoError(err)
	s.True(exists)
}

func (s *UtilSuite) assertDirectoryExists(path string) {
	exists, err := directoryExists(path)
	s.NoError(err)
	s.True(exists)
}

func (s *UtilSuite) assertDirectoryNotExists(path string) {
	exists, err := directoryExists(path)
	s.NoError(err)
	s.False(exists)
}

func (s *UtilSuite) assertCorrectFileMode(path string) {
	info, err := os.Stat(path)
	s.NoError(err)
	mode := testFileMode
	if info.IsDir() {
		mode = testDirMode | os.ModeDir
	}
	s.Equal(mode, info.Mode())
}

func toWorkflowExecutionStatusPtr(in enumspb.WorkflowExecutionStatus) *enumspb.WorkflowExecutionStatus {
	return &in
}
