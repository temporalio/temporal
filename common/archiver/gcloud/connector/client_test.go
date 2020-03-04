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

package connector_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/api/iterator"

	"github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/archiver/gcloud/connector"
	"github.com/uber/cadence/common/archiver/gcloud/connector/mocks"
	"github.com/uber/cadence/common/service/config"
)

func (s *clientSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	file, _ := json.MarshalIndent(&fakeData{data: "example"}, "", " ")

	os.MkdirAll("/tmp/cadence_archival/development", os.ModePerm)
	s.Require().NoError(ioutil.WriteFile("/tmp/cadence_archival/development/myfile.history", file, 0644))
}

func (s *clientSuite) TearDownTest() {
	os.Remove("/tmp/cadence_archival/development/myfile.history")
}

func TestClientSuite(t *testing.T) {
	suite.Run(t, new(clientSuite))
}

type clientSuite struct {
	*require.Assertions
	suite.Suite
}

type fakeData struct {
	data string
}

func (s *clientSuite) TestUpload() {
	ctx := context.Background()

	mockStorageClient := &mocks.GcloudStorageClient{}
	mockBucketHandleClient := &mocks.BucketHandleWrapper{}
	mockObjectHandler := &mocks.ObjectHandleWrapper{}
	mockWriter := &mocks.WriterWrapper{}

	storageWrapper, _ := connector.NewClientWithParams(mockStorageClient)

	mockStorageClient.On("Bucket", "my-bucket-cad").Return(mockBucketHandleClient).Times(1)
	mockBucketHandleClient.On("Object", "cadence_archival/development/myfile.history").Return(mockObjectHandler).Times(1)
	mockObjectHandler.On("NewWriter", ctx).Return(mockWriter).Times(1)
	mockWriter.On("Write", mock.Anything).Return(2, nil).Times(2)
	mockWriter.On("Close").Return(nil).Times(1)

	URI, err := archiver.NewURI("gs://my-bucket-cad/cadence_archival/development")
	err = storageWrapper.Upload(ctx, URI, "myfile.history", []byte("{}"))
	s.Require().NoError(err)
}

func (s *clientSuite) TestUploadWriterCloseError() {
	ctx := context.Background()

	mockStorageClient := &mocks.GcloudStorageClient{}
	mockBucketHandleClient := &mocks.BucketHandleWrapper{}
	mockObjectHandler := &mocks.ObjectHandleWrapper{}
	mockWriter := &mocks.WriterWrapper{}

	storageWrapper, _ := connector.NewClientWithParams(mockStorageClient)

	mockStorageClient.On("Bucket", "my-bucket-cad").Return(mockBucketHandleClient).Times(1)
	mockBucketHandleClient.On("Object", "cadence_archival/development/myfile.history").Return(mockObjectHandler).Times(1)
	mockObjectHandler.On("NewWriter", ctx).Return(mockWriter).Times(1)
	mockWriter.On("Write", mock.Anything).Return(2, nil).Times(2)
	mockWriter.On("Close").Return(errors.New("Not Found")).Times(1)

	URI, err := archiver.NewURI("gs://my-bucket-cad/cadence_archival/development")
	err = storageWrapper.Upload(ctx, URI, "myfile.history", []byte("{}"))
	s.Require().EqualError(err, "Not Found")
}

func (s *clientSuite) TestExist() {
	ctx := context.Background()
	testCases := []struct {
		callContext         context.Context
		URI                 string
		fileName            string
		bucketName          string
		bucketExists        bool
		objectExists        bool
		bucketExpectedError error
		objectExpectedError error
	}{
		{
			callContext:         ctx,
			URI:                 "gs://my-bucket-cad/cadence_archival/development",
			fileName:            "",
			bucketName:          "my-bucket-cad",
			bucketExists:        true,
			objectExists:        false,
			bucketExpectedError: nil,
			objectExpectedError: nil,
		},
		{
			callContext:         ctx,
			URI:                 "gs://my-bucket-cad/cadence_archival/development",
			fileName:            "",
			bucketName:          "my-bucket-cad",
			bucketExists:        false,
			objectExists:        false,
			bucketExpectedError: errors.New("bucket not found"),
			objectExpectedError: nil,
		},
		{
			callContext:         ctx,
			URI:                 "gs://my-bucket-cad/cadence_archival/development",
			fileName:            "myfile.history",
			bucketName:          "my-bucket-cad",
			bucketExists:        true,
			objectExists:        true,
			bucketExpectedError: nil,
			objectExpectedError: nil,
		},
		{
			callContext:         ctx,
			URI:                 "gs://my-bucket-cad/cadence_archival/development",
			fileName:            "myfile.history",
			bucketName:          "my-bucket-cad",
			bucketExists:        true,
			objectExists:        false,
			bucketExpectedError: nil,
			objectExpectedError: errors.New("object not found"),
		},
	}

	for _, tc := range testCases {

		mockStorageClient := &mocks.GcloudStorageClient{}
		mockBucketHandleClient := &mocks.BucketHandleWrapper{}
		mockObjectHandler := &mocks.ObjectHandleWrapper{}

		mockStorageClient.On("Bucket", tc.bucketName).Return(mockBucketHandleClient).Times(1)
		mockBucketHandleClient.On("Attrs", tc.callContext).Return(nil, tc.bucketExpectedError).Times(1)
		mockBucketHandleClient.On("Object", tc.fileName).Return(mockObjectHandler).Times(1)
		mockObjectHandler.On("Attrs", tc.callContext).Return(nil, tc.objectExpectedError).Times(1)
		URI, _ := archiver.NewURI(tc.URI)
		storageWrapper, _ := connector.NewClientWithParams(mockStorageClient)
		exists, err := storageWrapper.Exist(tc.callContext, URI, tc.fileName)
		if tc.bucketExists && tc.fileName == "" {
			s.Require().NoError(err)
			s.True(exists)
		} else if !tc.bucketExists {
			s.Require().Error(err)
			s.Require().EqualError(err, "bucket not found")
			s.False(exists)
		} else if tc.bucketExists && tc.fileName != "" && tc.objectExists {
			s.Require().NoError(err)
			s.True(exists)
		} else if tc.bucketExists && tc.fileName != "" && !tc.objectExists {
			s.Require().Error(err)
			s.Require().EqualError(err, "object not found")
			s.False(exists)
		}
	}

}

func (s *clientSuite) TestGet() {
	ctx := context.Background()
	mockStorageClient := &mocks.GcloudStorageClient{}
	mockBucketHandleClient := &mocks.BucketHandleWrapper{}
	mockObjectHandler := &mocks.ObjectHandleWrapper{}
	mockReader := &mocks.ReaderWrapper{}

	storageWrapper, _ := connector.NewClientWithParams(mockStorageClient)

	mockStorageClient.On("Bucket", "my-bucket-cad").Return(mockBucketHandleClient).Times(1)
	mockBucketHandleClient.On("Object", "cadence_archival/development/myfile.history").Return(mockObjectHandler).Times(1)
	mockObjectHandler.On("NewReader", ctx).Return(mockReader, nil).Times(1)
	mockReader.On("Read", mock.Anything).Return(2, io.EOF).Times(2)
	mockReader.On("Close").Return(nil).Times(1)

	URI, err := archiver.NewURI("gs://my-bucket-cad/cadence_archival/development")
	_, err = storageWrapper.Get(ctx, URI, "myfile.history")
	s.Require().NoError(err)
}

func (s *clientSuite) TestWrongGoogleCredentialsPath() {
	ctx := context.Background()
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/Wrong/path")
	_, err := connector.NewClient(ctx, &config.GstorageArchiver{})
	s.Require().Error(err)
}

func (s *clientSuite) TestQuery() {

	ctx := context.Background()
	mockBucketHandleClient := &mocks.BucketHandleWrapper{}
	mockStorageClient := &mocks.GcloudStorageClient{}
	mockObjectIterator := &mocks.ObjectIteratorWrapper{}
	storageWrapper, _ := connector.NewClientWithParams(mockStorageClient)

	attr := new(storage.ObjectAttrs)
	attr.Name = "fileName_01"

	mockStorageClient.On("Bucket", "my-bucket-cad").Return(mockBucketHandleClient).Times(1)
	mockBucketHandleClient.On("Objects", ctx, mock.Anything).Return(mockObjectIterator).Times(1)
	mockIterator := 0
	mockObjectIterator.On("Next").Return(func() *storage.ObjectAttrs {
		mockIterator++
		if mockIterator == 1 {
			return attr
		}
		return nil

	}, func() error {
		if mockIterator == 1 {
			return nil
		}
		return iterator.Done

	}).Times(2)

	var fileNames []string
	URI, err := archiver.NewURI("gs://my-bucket-cad/cadence_archival/development")
	fileNames, err = storageWrapper.Query(ctx, URI, "7478875943689868082123907395549832634615673687049942026838")
	s.Require().NoError(err)
	s.Equal(strings.Join(fileNames, ", "), "fileName_01")
}

func (s *clientSuite) TestQueryWithFilter() {

	ctx := context.Background()
	mockBucketHandleClient := &mocks.BucketHandleWrapper{}
	mockStorageClient := &mocks.GcloudStorageClient{}
	mockObjectIterator := &mocks.ObjectIteratorWrapper{}
	storageWrapper, _ := connector.NewClientWithParams(mockStorageClient)

	attr := new(storage.ObjectAttrs)
	attr.Name = "closeTimeout_2020-02-27T09:42:28Z_12851121011173788097_4418294404690464320_15619178330501475177.visibility"
	attrInvalid := new(storage.ObjectAttrs)
	attrInvalid.Name = "closeTimeout_2020-02-27T09:42:28Z_12851121011173788097_4418294404690464321_15619178330501475177.visibility"

	mockStorageClient.On("Bucket", "my-bucket-cad").Return(mockBucketHandleClient).Times(1)
	mockBucketHandleClient.On("Objects", ctx, mock.Anything).Return(mockObjectIterator).Times(1)
	mockIterator := 0
	mockObjectIterator.On("Next").Return(func() *storage.ObjectAttrs {
		mockIterator++
		switch mockIterator {
		case 1:
			return attr
		case 2:
			return attrInvalid
		default:
			return nil
		}

	}, func() error {
		switch mockIterator {
		case 1:
			return nil
		case 2:
			return nil
		default:
			return iterator.Done
		}

	}).Times(3)

	var fileNames []string
	URI, err := archiver.NewURI("gs://my-bucket-cad/cadence_archival/development")
	fileNames, _, _, err = storageWrapper.QueryWithFilters(ctx, URI, "closeTimeout_2020-02-27T09:42:28Z", 0, 0, []connector.Precondition{newWorkflowIDPrecondition("4418294404690464320")})

	s.Require().NoError(err)
	s.Equal(strings.Join(fileNames, ", "), "closeTimeout_2020-02-27T09:42:28Z_12851121011173788097_4418294404690464320_15619178330501475177.visibility")
}

func newWorkflowIDPrecondition(workflowID string) connector.Precondition {
	return func(subject interface{}) bool {

		if workflowID == "" {
			return true
		}

		fileName, ok := subject.(string)
		if !ok {
			return false
		}

		if strings.Contains(fileName, workflowID) {
			fileNameParts := strings.Split(fileName, "_")
			if len(fileNameParts) != 5 {
				return true
			}
			return strings.Contains(fileName, fileNameParts[3])
		}

		return false
	}
}
