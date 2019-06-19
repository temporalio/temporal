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
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber/cadence/common"
	"go.uber.org/cadence"
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

func (s *UtilSuite) TestHashesEqual() {
	testCases := []struct {
		a     []uint64
		b     []uint64
		equal bool
	}{
		{
			a:     nil,
			b:     nil,
			equal: true,
		},
		{
			a:     []uint64{1, 2, 3},
			b:     []uint64{1, 2, 3},
			equal: true,
		},
		{
			a:     []uint64{1, 2},
			b:     []uint64{1, 2, 3},
			equal: false,
		},
		{
			a:     []uint64{1, 2, 3},
			b:     []uint64{1, 2},
			equal: false,
		},
		{
			a:     []uint64{1, 2, 5, 5, 5},
			b:     []uint64{1, 2, 5, 5, 5},
			equal: true,
		},
		{
			a:     []uint64{1, 2, 5, 5},
			b:     []uint64{1, 2, 5, 5, 5},
			equal: false,
		},
		{
			a:     []uint64{1, 2, 5, 5, 5, 5},
			b:     []uint64{1, 2, 5, 5, 5},
			equal: false,
		},
	}

	for _, tc := range testCases {
		s.Equal(tc.equal, hashesEqual(tc.a, tc.b))
	}
}

func (s *UtilSuite) TestHistoryMutated() {
	testCases := []struct {
		historyBlob *HistoryBlob
		request     *ArchiveRequest
		isMutated   bool
	}{
		{
			historyBlob: &HistoryBlob{
				Header: &HistoryBlobHeader{
					LastFailoverVersion: common.Int64Ptr(15),
				},
			},
			request: &ArchiveRequest{
				CloseFailoverVersion: 3,
			},
			isMutated: true,
		},
		{
			historyBlob: &HistoryBlob{
				Header: &HistoryBlobHeader{
					LastFailoverVersion: common.Int64Ptr(10),
					LastEventID:         common.Int64Ptr(50),
					IsLast:              common.BoolPtr(true),
				},
			},
			request: &ArchiveRequest{
				CloseFailoverVersion: 10,
				NextEventID:          34,
			},
			isMutated: true,
		},
		{
			historyBlob: &HistoryBlob{
				Header: &HistoryBlobHeader{
					LastFailoverVersion: common.Int64Ptr(9),
					IsLast:              common.BoolPtr(true),
				},
			},
			request: &ArchiveRequest{
				CloseFailoverVersion: 10,
			},
			isMutated: true,
		},
		{
			historyBlob: &HistoryBlob{
				Header: &HistoryBlobHeader{
					LastFailoverVersion: common.Int64Ptr(10),
					LastEventID:         common.Int64Ptr(33),
					IsLast:              common.BoolPtr(true),
				},
			},
			request: &ArchiveRequest{
				CloseFailoverVersion: 10,
				NextEventID:          34,
			},
			isMutated: false,
		},
	}
	for _, tc := range testCases {
		s.Equal(tc.isMutated, historyMutated(tc.historyBlob, tc.request))
	}
}

func (s *UtilSuite) TestValidateRequest() {
	testCases := []struct {
		request     *ArchiveRequest
		expectedErr error
	}{
		{
			request:     &ArchiveRequest{},
			expectedErr: cadence.NewCustomError(errInvalidRequest, errEmptyBucket),
		},
		{
			request:     &ArchiveRequest{BucketName: "some random bucket name"},
			expectedErr: nil,
		},
	}

	for _, tc := range testCases {
		s.Equal(tc.expectedErr, validateArchivalRequest(tc.request))
	}
}

func (s *UtilSuite) TestGetUploadResponse() {
	testErrReason := "some random reason"
	testCustomErr := cadence.NewCustomError(testErrReason, testErrDetails)
	testCases := []struct {
		progress          uploadProgress
		inputErr          error
		expectedResult    *uploadResult
		expectedOutputErr error
	}{
		{
			progress: uploadProgress{UploadedBlobs: []string{"key 1", "key 2"}},
			inputErr: testCustomErr,
			expectedResult: &uploadResult{
				BlobsToDelete:    []string{"key 1", "key 2"},
				ErrorWithDetails: fmt.Sprintf("%v: %v", testErrReason, testErrDetails),
			},
			expectedOutputErr: nil,
		},
		{
			progress: uploadProgress{UploadedBlobs: []string{"key 1"}},
			inputErr: errContextTimeout,
			expectedResult: &uploadResult{
				BlobsToDelete:    []string{"key 1"},
				ErrorWithDetails: errContextTimeout.Error(),
			},
			expectedOutputErr: nil,
		},
		{
			progress:          uploadProgress{UploadedBlobs: []string{"key 1"}},
			inputErr:          errors.New(errGetDomainByID),
			expectedResult:    nil,
			expectedOutputErr: errors.New(errGetDomainByID),
		},
		{
			progress:          uploadProgress{UploadedBlobs: []string{"key 1"}},
			inputErr:          errors.New(errInvalidRequest),
			expectedResult:    nil,
			expectedOutputErr: errors.New(errInvalidRequest),
		},
		{
			progress: uploadProgress{UploadedBlobs: []string{"key 1"}},
			inputErr: errors.New(testErrReason),
			expectedResult: &uploadResult{
				BlobsToDelete:    []string{"key 1"},
				ErrorWithDetails: testErrReason,
			},
			expectedOutputErr: nil,
		},
	}

	for _, tc := range testCases {
		actualResult, actualOutputErr := getUploadHistoryActivityResponse(tc.progress, tc.inputErr)
		s.Equal(tc.expectedResult, actualResult)
		s.Equal(tc.expectedOutputErr, actualOutputErr)
	}
}
