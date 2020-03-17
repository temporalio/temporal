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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/serviceerror"
	"go.uber.org/zap"

	archiverproto "github.com/temporalio/temporal/.gen/proto/archiver"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/archiver"
	"github.com/temporalio/temporal/common/archiver/s3store/mocks"
	"github.com/temporalio/temporal/common/codec"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/loggerimpl"
	"github.com/temporalio/temporal/common/metrics"
)

const (
	testDomainID             = "test-domain-id"
	testDomainName           = "test-domain-name"
	testWorkflowID           = "test-workflow-id"
	testRunID                = "test-run-id"
	testNextEventID          = 1800
	testCloseFailoverVersion = int64(100)
	testPageSize             = 100
	testBucket               = "test-bucket"
	testBucketURI            = "s3://test-bucket"
)

var (
	testBranchToken = []byte{1, 2, 3}
)

type historyArchiverSuite struct {
	*require.Assertions
	suite.Suite
	s3cli              *mocks.S3API
	container          *archiver.HistoryBootstrapContainer
	logger             log.Logger
	testArchivalURI    archiver.URI
	historyBatchesV1   []*archiverproto.HistoryBlob
	historyBatchesV100 []*archiverproto.HistoryBlob
}

func TestHistoryArchiverSuite(t *testing.T) {
	suite.Run(t, new(historyArchiverSuite))
}

func (s *historyArchiverSuite) SetupSuite() {
	var err error
	s.s3cli = &mocks.S3API{}
	setupFsEmulation(s.s3cli)
	s.setupHistoryDirectory()
	s.testArchivalURI, err = archiver.NewURI(testBucketURI)

	s.Require().NoError(err)
}

func (s *historyArchiverSuite) TearDownSuite() {
}

func (s *historyArchiverSuite) SetupTest() {
	scope := tally.NewTestScope("test", nil)
	s.Assertions = require.New(s.T())
	zapLogger := zap.NewNop()
	s.container = &archiver.HistoryBootstrapContainer{
		Logger:        loggerimpl.NewLogger(zapLogger),
		MetricsClient: metrics.NewClient(scope, metrics.HistoryArchiverScope),
	}
}

func setupFsEmulation(s3cli *mocks.S3API) {
	fs := make(map[string][]byte)

	putObjectFn := func(_ aws.Context, input *s3.PutObjectInput, _ ...request.Option) *s3.PutObjectOutput {
		buf := new(bytes.Buffer)
		buf.ReadFrom(input.Body)
		fs[*input.Bucket+*input.Key] = buf.Bytes()
		return &s3.PutObjectOutput{}
	}
	getObjectFn := func(_ aws.Context, input *s3.GetObjectInput, _ ...request.Option) *s3.GetObjectOutput {
		return &s3.GetObjectOutput{
			Body: ioutil.NopCloser(bytes.NewReader(fs[*input.Bucket+*input.Key])),
		}
	}
	s3cli.On("ListObjectsV2WithContext", mock.Anything, mock.Anything).
		Return(func(_ context.Context, input *s3.ListObjectsV2Input, opts ...request.Option) *s3.ListObjectsV2Output {
			objects := make([]*s3.Object, 0)
			commonPrefixMap := map[string]bool{}
			for k := range fs {
				if strings.HasPrefix(k, *input.Bucket+*input.Prefix) {
					key := k[len(*input.Bucket):]
					keyWithoutPrefix := key[len(*input.Prefix):]
					index := strings.Index(keyWithoutPrefix, "/")
					if index == -1 || input.Delimiter == nil {
						objects = append(objects, &s3.Object{
							Key: aws.String(key),
						})
					} else {
						commonPrefixMap[key[:len(*input.Prefix)+index]] = true
					}
				}
			}
			commonPrefixes := make([]*s3.CommonPrefix, 0)
			for k := range commonPrefixMap {
				commonPrefixes = append(commonPrefixes, &s3.CommonPrefix{
					Prefix: aws.String(k),
				})
			}

			sort.SliceStable(objects, func(i, j int) bool {
				return *objects[i].Key < *objects[j].Key
			})
			maxKeys := 1000
			if input.MaxKeys != nil {
				maxKeys = int(*input.MaxKeys)
			}
			start := 0
			if input.ContinuationToken != nil {
				start, _ = strconv.Atoi(*input.ContinuationToken)
			}

			if input.StartAfter != nil {
				for k, v := range objects {
					if *input.StartAfter == *v.Key {
						start = k + 1
					}
				}
			}

			isTruncated := false
			var nextContinuationToken *string
			if len(objects) > start+maxKeys {
				isTruncated = true
				nextContinuationToken = common.StringPtr(fmt.Sprintf("%d", start+maxKeys))
				objects = objects[start : start+maxKeys]
			} else {
				objects = objects[start:]
			}

			if input.StartAfter != nil {
				for k, v := range commonPrefixes {
					if *input.StartAfter == *v.Prefix {
						start = k + 1
					}
				}
			}

			if len(commonPrefixes) > start+maxKeys {
				isTruncated = true
				nextContinuationToken = common.StringPtr(fmt.Sprintf("%d", start+maxKeys))
				commonPrefixes = commonPrefixes[start : start+maxKeys]
			} else if len(commonPrefixes) > 0 {
				commonPrefixes = commonPrefixes[start:]
			}

			return &s3.ListObjectsV2Output{
				CommonPrefixes:        commonPrefixes,
				Contents:              objects,
				IsTruncated:           &isTruncated,
				NextContinuationToken: nextContinuationToken,
			}
		}, nil)
	s3cli.On("PutObjectWithContext", mock.Anything, mock.Anything).Return(putObjectFn, nil)

	s3cli.On("HeadObjectWithContext", mock.Anything, mock.MatchedBy(func(input *s3.HeadObjectInput) bool {
		_, ok := fs[*input.Bucket+*input.Key]
		return !ok
	})).Return(nil, awserr.New("NotFound", "", nil))
	s3cli.On("HeadObjectWithContext", mock.Anything, mock.Anything).Return(&s3.HeadObjectOutput{}, nil)

	s3cli.On("GetObjectWithContext", mock.Anything, mock.MatchedBy(func(input *s3.GetObjectInput) bool {
		_, ok := fs[*input.Bucket+*input.Key]
		return !ok
	})).Return(nil, awserr.New(s3.ErrCodeNoSuchKey, "", nil))
	s3cli.On("GetObjectWithContext", mock.Anything, mock.Anything).Return(getObjectFn, nil)
}

func (s *historyArchiverSuite) TestValidateURI() {
	testCases := []struct {
		URI         string
		expectedErr error
	}{
		{
			URI:         "wrongscheme:///a/b/c",
			expectedErr: archiver.ErrURISchemeMismatch,
		},
		{
			URI:         "s3://",
			expectedErr: errNoBucketSpecified,
		},
		{
			URI:         "s3://bucket/a/b/c",
			expectedErr: errBucketNotExists,
		},
		{
			URI:         testBucketURI,
			expectedErr: nil,
		},
	}

	s.s3cli.On("HeadBucketWithContext", mock.Anything, mock.MatchedBy(func(input *s3.HeadBucketInput) bool {
		return *input.Bucket != s.testArchivalURI.Hostname()
	})).Return(nil, awserr.New("NotFound", "", nil))
	s.s3cli.On("HeadBucketWithContext", mock.Anything, mock.Anything).Return(&s3.HeadBucketOutput{}, nil)

	historyArchiver := s.newTestHistoryArchiver(nil)
	for _, tc := range testCases {
		URI, err := archiver.NewURI(tc.URI)
		s.NoError(err)
		s.Equal(tc.expectedErr, historyArchiver.ValidateURI(URI))
	}
}

func (s *historyArchiverSuite) TestArchive_Fail_InvalidURI() {
	historyArchiver := s.newTestHistoryArchiver(nil)
	request := &archiver.ArchiveHistoryRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	URI, err := archiver.NewURI("wrongscheme://")
	s.NoError(err)
	err = historyArchiver.Archive(context.Background(), URI, request)
	s.Error(err)
}

func (s *historyArchiverSuite) TestArchive_Fail_InvalidRequest() {
	historyArchiver := s.newTestHistoryArchiver(nil)
	request := &archiver.ArchiveHistoryRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           "", // an invalid request
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	err := historyArchiver.Archive(context.Background(), s.testArchivalURI, request)
	s.Error(err)
}

func (s *historyArchiverSuite) TestArchive_Fail_ErrorOnReadHistory() {
	mockCtrl := gomock.NewController(s.T())
	defer mockCtrl.Finish()
	historyIterator := archiver.NewMockHistoryIterator(mockCtrl)
	gomock.InOrder(
		historyIterator.EXPECT().HasNext().Return(true),
		historyIterator.EXPECT().Next().Return(nil, errors.New("some random error")),
	)

	historyArchiver := s.newTestHistoryArchiver(historyIterator)
	request := &archiver.ArchiveHistoryRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	err := historyArchiver.Archive(context.Background(), s.testArchivalURI, request)
	s.Error(err)
}

func (s *historyArchiverSuite) TestArchive_Fail_TimeoutWhenReadingHistory() {
	mockCtrl := gomock.NewController(s.T())
	defer mockCtrl.Finish()
	historyIterator := archiver.NewMockHistoryIterator(mockCtrl)
	gomock.InOrder(
		historyIterator.EXPECT().HasNext().Return(true),
		historyIterator.EXPECT().Next().Return(nil, serviceerror.NewResourceExhausted("")),
	)

	historyArchiver := s.newTestHistoryArchiver(historyIterator)
	request := &archiver.ArchiveHistoryRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	err := historyArchiver.Archive(getCanceledContext(), s.testArchivalURI, request)
	s.Error(err)
}

func (s *historyArchiverSuite) TestArchive_Fail_HistoryMutated() {
	mockCtrl := gomock.NewController(s.T())
	defer mockCtrl.Finish()
	historyIterator := archiver.NewMockHistoryIterator(mockCtrl)
	historyBatches := []*commonproto.History{
		{
			Events: []*commonproto.HistoryEvent{
				{
					EventId:   common.FirstEventID + 1,
					Timestamp: time.Now().UnixNano(),
					Version:   testCloseFailoverVersion + 1,
				},
			},
		},
	}
	historyBlob := &archiverproto.HistoryBlob{
		Header: &archiverproto.HistoryBlobHeader{
			IsLast: true,
		},
		Body: historyBatches,
	}
	gomock.InOrder(
		historyIterator.EXPECT().HasNext().Return(true),
		historyIterator.EXPECT().Next().Return(historyBlob, nil),
	)

	historyArchiver := s.newTestHistoryArchiver(historyIterator)
	request := &archiver.ArchiveHistoryRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	err := historyArchiver.Archive(context.Background(), s.testArchivalURI, request)
	s.Error(err)
}

func (s *historyArchiverSuite) TestArchive_Fail_NonRetriableErrorOption() {
	mockCtrl := gomock.NewController(s.T())
	defer mockCtrl.Finish()
	historyIterator := archiver.NewMockHistoryIterator(mockCtrl)
	gomock.InOrder(
		historyIterator.EXPECT().HasNext().Return(true),
		historyIterator.EXPECT().Next().Return(nil, errors.New("some random error")),
	)

	historyArchiver := s.newTestHistoryArchiver(historyIterator)
	request := &archiver.ArchiveHistoryRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	nonRetryableErr := errors.New("some non-retryable error")
	err := historyArchiver.Archive(context.Background(), s.testArchivalURI, request, archiver.GetNonRetriableErrorOption(nonRetryableErr))
	s.Equal(nonRetryableErr, err)
}

func (s *historyArchiverSuite) TestArchive_Success() {
	mockCtrl := gomock.NewController(s.T())
	defer mockCtrl.Finish()
	historyIterator := archiver.NewMockHistoryIterator(mockCtrl)
	historyBatches := []*commonproto.History{
		{
			Events: []*commonproto.HistoryEvent{
				{
					EventId:   common.FirstEventID + 1,
					Timestamp: time.Now().UnixNano(),
					Version:   testCloseFailoverVersion,
				},
				{
					EventId:   common.FirstEventID + 2,
					Timestamp: time.Now().UnixNano(),
					Version:   testCloseFailoverVersion,
				},
			},
		},
		{
			Events: []*commonproto.HistoryEvent{
				{
					EventId:   testNextEventID - 1,
					Timestamp: time.Now().UnixNano(),
					Version:   testCloseFailoverVersion,
				},
			},
		},
	}
	historyBlob := &archiverproto.HistoryBlob{
		Header: &archiverproto.HistoryBlobHeader{
			IsLast: true,
		},
		Body: historyBatches,
	}
	gomock.InOrder(
		historyIterator.EXPECT().HasNext().Return(true),
		historyIterator.EXPECT().Next().Return(historyBlob, nil),
		historyIterator.EXPECT().HasNext().Return(false),
	)

	historyArchiver := s.newTestHistoryArchiver(historyIterator)
	request := &archiver.ArchiveHistoryRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	URI, err := archiver.NewURI(testBucketURI + "/TestArchive_Success")
	s.NoError(err)
	err = historyArchiver.Archive(context.Background(), URI, request)
	s.NoError(err)

	expectedkey := constructHistoryKey("", testDomainID, testWorkflowID, testRunID, testCloseFailoverVersion, 0)
	s.assertKeyExists(expectedkey)
}

func (s *historyArchiverSuite) TestGet_Fail_InvalidURI() {
	historyArchiver := s.newTestHistoryArchiver(nil)
	request := &archiver.GetHistoryRequest{
		DomainID:   testDomainID,
		WorkflowID: testWorkflowID,
		RunID:      testRunID,
		PageSize:   100,
	}
	URI, err := archiver.NewURI("wrongscheme://")
	s.NoError(err)
	response, err := historyArchiver.Get(context.Background(), URI, request)
	s.Nil(response)
	s.Error(err)
}

func (s *historyArchiverSuite) TestGet_Fail_InvalidRequest() {
	historyArchiver := s.newTestHistoryArchiver(nil)
	request := &archiver.GetHistoryRequest{
		DomainID:   testDomainID,
		WorkflowID: testWorkflowID,
		RunID:      testRunID,
		PageSize:   0, // pageSize should be greater than 0
	}
	response, err := historyArchiver.Get(context.Background(), s.testArchivalURI, request)
	s.Nil(response)
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *historyArchiverSuite) TestGet_Fail_InvalidToken() {
	historyArchiver := s.newTestHistoryArchiver(nil)
	request := &archiver.GetHistoryRequest{
		DomainID:      testDomainID,
		WorkflowID:    testWorkflowID,
		RunID:         testRunID,
		PageSize:      testPageSize,
		NextPageToken: []byte{'r', 'a', 'n', 'd', 'o', 'm'},
	}
	URI, err := archiver.NewURI(testBucketURI)
	s.NoError(err)
	response, err := historyArchiver.Get(context.Background(), URI, request)
	s.Nil(response)
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *historyArchiverSuite) TestGet_Fail_KeyNotExist() {
	historyArchiver := s.newTestHistoryArchiver(nil)
	testCloseFailoverVersion := testCloseFailoverVersion
	request := &archiver.GetHistoryRequest{
		DomainID:             testDomainID,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		PageSize:             testPageSize,
		CloseFailoverVersion: &testCloseFailoverVersion,
	}
	URI, err := archiver.NewURI("s3://test-bucket/non-existent")
	s.NoError(err)
	response, err := historyArchiver.Get(context.Background(), URI, request)
	s.Nil(response)
	s.Error(err)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *historyArchiverSuite) TestGet_Success_PickHighestVersion() {
	historyArchiver := s.newTestHistoryArchiver(nil)
	request := &archiver.GetHistoryRequest{
		DomainID:   testDomainID,
		WorkflowID: testWorkflowID,
		RunID:      testRunID,
		PageSize:   testPageSize,
	}
	URI, err := archiver.NewURI(testBucketURI)
	s.NoError(err)
	response, err := historyArchiver.Get(context.Background(), URI, request)
	s.NoError(err)
	s.Nil(response.NextPageToken)
	s.Equal(append(s.historyBatchesV100[0].Body, s.historyBatchesV100[1].Body...), response.HistoryBatches)
}

func (s *historyArchiverSuite) TestGet_Success_UseProvidedVersion() {
	historyArchiver := s.newTestHistoryArchiver(nil)
	testCloseFailoverVersion := int64(1)
	request := &archiver.GetHistoryRequest{
		DomainID:             testDomainID,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		PageSize:             testPageSize,
		CloseFailoverVersion: &testCloseFailoverVersion,
	}
	URI, err := archiver.NewURI(testBucketURI)
	s.NoError(err)
	response, err := historyArchiver.Get(context.Background(), URI, request)
	s.NoError(err)
	s.Nil(response.NextPageToken)
	s.Equal(s.historyBatchesV1[0].Body, response.HistoryBatches)
}

func (s *historyArchiverSuite) TestGet_Success_SmallPageSize() {
	historyArchiver := s.newTestHistoryArchiver(nil)
	testCloseFailoverVersion := testCloseFailoverVersion
	request := &archiver.GetHistoryRequest{
		DomainID:             testDomainID,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		PageSize:             1,
		CloseFailoverVersion: &testCloseFailoverVersion,
	}
	var combinedHistory []*commonproto.History

	URI, err := archiver.NewURI(testBucketURI)
	s.NoError(err)
	response, err := historyArchiver.Get(context.Background(), URI, request)
	s.NoError(err)
	s.NotNil(response)
	s.NotNil(response.NextPageToken)
	s.NotNil(response.HistoryBatches)
	s.Len(response.HistoryBatches, 1)
	combinedHistory = append(combinedHistory, response.HistoryBatches...)

	request.NextPageToken = response.NextPageToken
	response, err = historyArchiver.Get(context.Background(), URI, request)
	s.NoError(err)
	s.NotNil(response)
	s.Nil(response.NextPageToken)
	s.NotNil(response.HistoryBatches)
	s.Len(response.HistoryBatches, 1)
	combinedHistory = append(combinedHistory, response.HistoryBatches...)

	s.Equal(append(s.historyBatchesV100[0].Body, s.historyBatchesV100[1].Body...), combinedHistory)
}

func (s *historyArchiverSuite) TestArchiveAndGet() {
	mockCtrl := gomock.NewController(s.T())
	defer mockCtrl.Finish()
	historyIterator := archiver.NewMockHistoryIterator(mockCtrl)

	gomock.InOrder(
		historyIterator.EXPECT().HasNext().Return(true),
		historyIterator.EXPECT().Next().Return(s.historyBatchesV100[0], nil),
		historyIterator.EXPECT().HasNext().Return(true),
		historyIterator.EXPECT().Next().Return(s.historyBatchesV100[1], nil),
		historyIterator.EXPECT().HasNext().Return(false),
	)

	historyArchiver := s.newTestHistoryArchiver(historyIterator)
	archiveRequest := &archiver.ArchiveHistoryRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	URI, err := archiver.NewURI(testBucketURI + "/TestArchiveAndGet")
	s.NoError(err)
	err = historyArchiver.Archive(context.Background(), URI, archiveRequest)
	s.NoError(err)

	getRequest := &archiver.GetHistoryRequest{
		DomainID:   testDomainID,
		WorkflowID: testWorkflowID,
		RunID:      testRunID,
		PageSize:   testPageSize,
	}
	response, err := historyArchiver.Get(context.Background(), URI, getRequest)
	s.NoError(err)
	s.NotNil(response)
	s.Nil(response.NextPageToken)
	s.Equal(append(s.historyBatchesV100[0].Body, s.historyBatchesV100[1].Body...), response.HistoryBatches)
}

func (s *historyArchiverSuite) newTestHistoryArchiver(historyIterator archiver.HistoryIterator) *historyArchiver {
	//config := &config.S3Archiver{}
	//archiver, err := newHistoryArchiver(s.container, config, historyIterator)
	archiver := &historyArchiver{
		container:       s.container,
		s3cli:           s.s3cli,
		historyIterator: historyIterator,
	}
	return archiver
}

func (s *historyArchiverSuite) setupHistoryDirectory() {
	s.historyBatchesV1 = []*archiverproto.HistoryBlob{
		{
			Header: &archiverproto.HistoryBlobHeader{
				IsLast: true,
			},
			Body: []*commonproto.History{
				{
					Events: []*commonproto.HistoryEvent{
						{
							EventId:   testNextEventID - 1,
							Timestamp: time.Now().UnixNano(),
							Version:   1,
						},
					},
				},
			},
		},
	}

	s.historyBatchesV100 = []*archiverproto.HistoryBlob{
		{
			Header: &archiverproto.HistoryBlobHeader{
				IsLast: false,
			},
			Body: []*commonproto.History{
				{
					Events: []*commonproto.HistoryEvent{
						{
							EventId:   common.FirstEventID + 1,
							Timestamp: time.Now().UnixNano(),
							Version:   testCloseFailoverVersion,
						},
						{
							EventId:   common.FirstEventID + 1,
							Timestamp: time.Now().UnixNano(),
							Version:   testCloseFailoverVersion,
						},
					},
				},
			},
		},
		{
			Header: &archiverproto.HistoryBlobHeader{
				IsLast: true,
			},
			Body: []*commonproto.History{
				{
					Events: []*commonproto.HistoryEvent{
						{
							EventId:   testNextEventID - 1,
							Timestamp: time.Now().UnixNano(),
							Version:   testCloseFailoverVersion,
						},
					},
				},
			},
		},
	}

	s.writeHistoryBatchesForGetTest(s.historyBatchesV1, int64(1))
	s.writeHistoryBatchesForGetTest(s.historyBatchesV100, testCloseFailoverVersion)
}

func (s *historyArchiverSuite) writeHistoryBatchesForGetTest(historyBatches []*archiverproto.HistoryBlob, version int64) {
	for i, batch := range historyBatches {
		encoder := codec.NewJSONPBEncoder()
		data, err := encoder.Encode(batch)
		s.Require().NoError(err)
		key := constructHistoryKey("", testDomainID, testWorkflowID, testRunID, version, i)
		_, err = s.s3cli.PutObjectWithContext(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(data),
		})
		s.Require().NoError(err)
	}
}

func (s *historyArchiverSuite) assertKeyExists(key string) {
	_, err := s.s3cli.GetObjectWithContext(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(key),
	})
	s.NoError(err)
}

func getCanceledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}
