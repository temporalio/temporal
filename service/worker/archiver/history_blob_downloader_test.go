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
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/blobstore/blob"
	"github.com/uber/cadence/common/mocks"
)

const (
	testHighVersion    = 5
	testHighVersionStr = "5"
	testLowVersionStr  = "1"
)

type historyBlobDownloaderSuite struct {
	suite.Suite
	blobstoreClient *mocks.BlobstoreClient
}

func TestHistoryBlobDownloderSuite(t *testing.T) {
	suite.Run(t, new(historyBlobDownloaderSuite))
}

func (s *historyBlobDownloaderSuite) SetupTest() {
	s.blobstoreClient = &mocks.BlobstoreClient{}
}

func (s *historyBlobDownloaderSuite) TearDownTest() {
	s.blobstoreClient.AssertExpectations(s.T())
}

func (s *historyBlobDownloaderSuite) TestDownloadBlob_Failed_CouldNotDeserializeToken() {
	blobDownloader := NewHistoryBlobDownloader(s.blobstoreClient)
	resp, err := blobDownloader.DownloadBlob(context.Background(), &DownloadBlobRequest{
		NextPageToken: []byte{1},
	})
	s.Error(err)
	s.Nil(resp)
}

func (s *historyBlobDownloaderSuite) TestDownloadBlob_Failed_CouldNotGetHighestVersion() {
	s.blobstoreClient.On("GetTags", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to get tags")).Once()
	blobDownloader := NewHistoryBlobDownloader(s.blobstoreClient)
	resp, err := blobDownloader.DownloadBlob(context.Background(), &DownloadBlobRequest{
		ArchivalBucket: testArchivalBucket,
		DomainID:       testDomainID,
		WorkflowID:     testWorkflowID,
		RunID:          testRunID,
	})
	s.Error(err)
	s.Nil(resp)
}

func (s *historyBlobDownloaderSuite) TestDownloadBlob_Failed_CouldNotDownloadBlob() {
	s.blobstoreClient.On("GetTags", mock.Anything, mock.Anything, mock.Anything).Return(map[string]string{testLowVersionStr: ""}, nil).Once()
	s.blobstoreClient.On("Download", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to download blob")).Once()
	blobDownloader := NewHistoryBlobDownloader(s.blobstoreClient)
	resp, err := blobDownloader.DownloadBlob(context.Background(), &DownloadBlobRequest{
		ArchivalBucket: testArchivalBucket,
		DomainID:       testDomainID,
		WorkflowID:     testWorkflowID,
		RunID:          testRunID,
	})
	s.Error(err)
	s.Nil(resp)
}

func (s *historyBlobDownloaderSuite) TestDownloadBlob_Success_GetFirstPage() {
	expected, page := s.getBlob(common.FirstBlobPageToken, false)
	key := s.getHistoryKey(testHighVersion, common.FirstBlobPageToken)
	s.blobstoreClient.On("GetTags", mock.Anything, mock.Anything, mock.Anything).Return(map[string]string{testLowVersionStr: "", testHighVersionStr: ""}, nil).Once()
	s.blobstoreClient.On("Download", mock.Anything, mock.Anything, key).Return(page, nil).Once()
	blobDownloader := NewHistoryBlobDownloader(s.blobstoreClient)
	resp, err := blobDownloader.DownloadBlob(context.Background(), &DownloadBlobRequest{
		ArchivalBucket: testArchivalBucket,
		DomainID:       testDomainID,
		WorkflowID:     testWorkflowID,
		RunID:          testRunID,
	})
	s.NoError(err)
	s.Equal(hash(*expected), hash(*resp.HistoryBlob))
	s.Equal(s.getPageToken(common.FirstBlobPageToken+1, testHighVersion), resp.NextPageToken)
}

func (s *historyBlobDownloaderSuite) TestDownloadBlob_Success_ProvidedVersion() {
	expected, page := s.getBlob(common.FirstBlobPageToken, false)
	key := s.getHistoryKey(testHighVersion, common.FirstBlobPageToken)
	s.blobstoreClient.On("Download", mock.Anything, mock.Anything, key).Return(page, nil).Once()
	blobDownloader := NewHistoryBlobDownloader(s.blobstoreClient)
	resp, err := blobDownloader.DownloadBlob(context.Background(), &DownloadBlobRequest{
		ArchivalBucket:       testArchivalBucket,
		DomainID:             testDomainID,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		CloseFailoverVersion: common.Int64Ptr(testHighVersion),
	})
	s.NoError(err)
	s.Equal(hash(*expected), hash(*resp.HistoryBlob))
	s.Equal(s.getPageToken(common.FirstBlobPageToken+1, testHighVersion), resp.NextPageToken)
}

func (s *historyBlobDownloaderSuite) TestDownloadBlob_Success_UsePageToken() {
	expected, page := s.getBlob(common.FirstBlobPageToken+1, false)
	key := s.getHistoryKey(testHighVersion, common.FirstBlobPageToken+1)
	s.blobstoreClient.On("Download", mock.Anything, mock.Anything, key).Return(page, nil).Once()
	blobDownloader := NewHistoryBlobDownloader(s.blobstoreClient)
	resp, err := blobDownloader.DownloadBlob(context.Background(), &DownloadBlobRequest{
		ArchivalBucket: testArchivalBucket,
		DomainID:       testDomainID,
		WorkflowID:     testWorkflowID,
		RunID:          testRunID,
		NextPageToken:  s.getPageToken(common.FirstBlobPageToken+1, testHighVersion),
	})
	s.NoError(err)
	s.Equal(hash(*expected), hash(*resp.HistoryBlob))
	s.Equal(s.getPageToken(common.FirstBlobPageToken+2, testHighVersion), resp.NextPageToken)
}

func (s *historyBlobDownloaderSuite) TestDownloadBlob_Success_GetLastPage() {
	expected, page := s.getBlob(common.FirstBlobPageToken+1, true)
	key := s.getHistoryKey(testHighVersion, common.FirstBlobPageToken+1)
	s.blobstoreClient.On("Download", mock.Anything, mock.Anything, key).Return(page, nil).Once()
	blobDownloader := NewHistoryBlobDownloader(s.blobstoreClient)
	resp, err := blobDownloader.DownloadBlob(context.Background(), &DownloadBlobRequest{
		ArchivalBucket: testArchivalBucket,
		DomainID:       testDomainID,
		WorkflowID:     testWorkflowID,
		RunID:          testRunID,
		NextPageToken:  s.getPageToken(common.FirstBlobPageToken+1, testHighVersion),
	})
	s.NoError(err)
	s.Equal(hash(*expected), hash(*resp.HistoryBlob))
	s.Nil(resp.NextPageToken)
}

func (s *historyBlobDownloaderSuite) TestDownloadBlob_Success_DownloadMultiple() {
	var expectedBlobs []*HistoryBlob
	for i := common.FirstBlobPageToken; i <= 10; i++ {
		currExpected, currPage := s.getBlob(i, i == 10)
		key := s.getHistoryKey(testHighVersion, i)
		s.blobstoreClient.On("Download", mock.Anything, mock.Anything, key).Return(currPage, nil).Once()
		expectedBlobs = append(expectedBlobs, currExpected)
	}
	s.blobstoreClient.On("GetTags", mock.Anything, mock.Anything, mock.Anything).Return(map[string]string{testLowVersionStr: "", testHighVersionStr: ""}, nil).Once()
	blobDownloader := NewHistoryBlobDownloader(s.blobstoreClient)
	resp, err := blobDownloader.DownloadBlob(context.Background(), &DownloadBlobRequest{
		ArchivalBucket: testArchivalBucket,
		DomainID:       testDomainID,
		WorkflowID:     testWorkflowID,
		RunID:          testRunID,
	})
	s.NoError(err)
	s.Equal(hash(*expectedBlobs[0]), hash(*resp.HistoryBlob))
	index := 1
	for resp.NextPageToken != nil {
		resp, err = blobDownloader.DownloadBlob(context.Background(), &DownloadBlobRequest{
			ArchivalBucket: testArchivalBucket,
			DomainID:       testDomainID,
			WorkflowID:     testWorkflowID,
			RunID:          testRunID,
			NextPageToken:  resp.NextPageToken,
		})
		s.NoError(err)
		s.Equal(hash(*expectedBlobs[index]), hash(*resp.HistoryBlob))
		index++
	}
}

func (s *historyBlobDownloaderSuite) getIndexKey() blob.Key {
	key, err := NewHistoryIndexBlobKey(testDomainID, testWorkflowID, testRunID)
	s.NoError(err)
	return key
}

func (s *historyBlobDownloaderSuite) getHistoryKey(version int64, page int) blob.Key {
	key, err := NewHistoryBlobKey(testDomainID, testWorkflowID, testRunID, version, page)
	s.NoError(err)
	return key
}

func (s *historyBlobDownloaderSuite) getBlob(pageToken int, last bool) (*HistoryBlob, *blob.Blob) {
	historyBlob := &HistoryBlob{
		Header: &HistoryBlobHeader{
			CurrentPageToken: common.IntPtr(pageToken),
			NextPageToken:    common.IntPtr(pageToken + 1),
			IsLast:           common.BoolPtr(last),
		},
		Body: &shared.History{},
	}
	if last {
		historyBlob.Header.NextPageToken = nil
	}
	for i := 0; i < 10; i++ {
		historyBlob.Body.Events = append(historyBlob.Body.Events, &shared.HistoryEvent{EventId: common.Int64Ptr(int64(i))})
	}
	bytes, err := json.Marshal(historyBlob)
	s.NoError(err)
	result, err := blob.Wrap(blob.NewBlob(bytes, map[string]string{}), blob.JSONEncoded())
	s.NoError(err)
	return historyBlob, result
}

func (s *historyBlobDownloaderSuite) getPageToken(blobstorePageToken int, version int64) []byte {
	token, err := serializeArchivalToken(&archivalToken{
		BlobstorePageToken:   blobstorePageToken,
		CloseFailoverVersion: version,
	})
	s.NoError(err)
	return token
}
