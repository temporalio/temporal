package connector_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/gcloud/connector"
	"go.temporal.io/server/common/config"
	"go.uber.org/mock/gomock"
	"google.golang.org/api/iterator"
)

func (s *clientSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
	file, _ := json.MarshalIndent(&fakeData{data: "example"}, "", " ")

	path := "/tmp/temporal_archival/development"
	if err := os.MkdirAll(path, os.ModePerm); err != nil {
		s.FailNowf("Failed to create directory %s: %v", path, err)
	}
	s.Require().NoError(os.WriteFile("/tmp/temporal_archival/development/myfile.history", file, 0644))
}

func (s *clientSuite) TearDownTest() {
	s.controller.Finish()
	name := "/tmp/temporal_archival/development/myfile.history"
	if err := os.Remove(name); err != nil {
		s.Failf("Failed to remove file %s: %v", name, err)
	}
}

func TestClientSuite(t *testing.T) {
	suite.Run(t, new(clientSuite))
}

type clientSuite struct {
	*require.Assertions
	suite.Suite
	controller *gomock.Controller
}

type fakeData struct {
	data string
}

func (s *clientSuite) TestUpload() {
	ctx := context.Background()

	mockStorageClient := connector.NewMockGcloudStorageClient(s.controller)
	mockBucketHandleClient := connector.NewMockBucketHandleWrapper(s.controller)
	mockObjectHandler := connector.NewMockObjectHandleWrapper(s.controller)
	mockWriter := connector.NewMockWriterWrapper(s.controller)

	storageWrapper, _ := connector.NewClientWithParams(mockStorageClient)

	mockStorageClient.EXPECT().Bucket("my-bucket-cad").Return(mockBucketHandleClient)
	mockBucketHandleClient.EXPECT().Object("temporal_archival/development/myfile.history").Return(mockObjectHandler)
	mockObjectHandler.EXPECT().NewWriter(ctx).Return(mockWriter)
	mockWriter.EXPECT().Write(gomock.Any()).Return(2, nil)
	mockWriter.EXPECT().Close().Return(nil)

	URI, err := archiver.NewURI("gs://my-bucket-cad/temporal_archival/development")
	s.Require().NoError(err)
	err = storageWrapper.Upload(ctx, URI, "myfile.history", []byte("{}"))
	s.Require().NoError(err)
}

func (s *clientSuite) TestUploadWriterCloseError() {
	ctx := context.Background()

	mockStorageClient := connector.NewMockGcloudStorageClient(s.controller)
	mockBucketHandleClient := connector.NewMockBucketHandleWrapper(s.controller)
	mockObjectHandler := connector.NewMockObjectHandleWrapper(s.controller)
	mockWriter := connector.NewMockWriterWrapper(s.controller)

	storageWrapper, _ := connector.NewClientWithParams(mockStorageClient)

	mockStorageClient.EXPECT().Bucket("my-bucket-cad").Return(mockBucketHandleClient)
	mockBucketHandleClient.EXPECT().Object("temporal_archival/development/myfile.history").Return(mockObjectHandler)
	mockObjectHandler.EXPECT().NewWriter(ctx).Return(mockWriter)
	mockWriter.EXPECT().Write(gomock.Any()).Return(2, nil)
	mockWriter.EXPECT().Close().Return(errors.New("Not Found"))

	URI, err := archiver.NewURI("gs://my-bucket-cad/temporal_archival/development")
	s.Require().NoError(err)
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
			URI:                 "gs://my-bucket-cad/temporal_archival/development",
			fileName:            "",
			bucketName:          "my-bucket-cad",
			bucketExists:        true,
			objectExists:        false,
			bucketExpectedError: nil,
			objectExpectedError: nil,
		},
		{
			callContext:         ctx,
			URI:                 "gs://my-bucket-cad/temporal_archival/development",
			fileName:            "",
			bucketName:          "my-bucket-cad",
			bucketExists:        false,
			objectExists:        false,
			bucketExpectedError: errors.New("bucket not found"),
			objectExpectedError: nil,
		},
		{
			callContext:         ctx,
			URI:                 "gs://my-bucket-cad/temporal_archival/development",
			fileName:            "myfile.history",
			bucketName:          "my-bucket-cad",
			bucketExists:        true,
			objectExists:        true,
			bucketExpectedError: nil,
			objectExpectedError: nil,
		},
		{
			callContext:         ctx,
			URI:                 "gs://my-bucket-cad/temporal_archival/development",
			fileName:            "myfile.history",
			bucketName:          "my-bucket-cad",
			bucketExists:        true,
			objectExists:        false,
			bucketExpectedError: nil,
			objectExpectedError: errors.New("object not found"),
		},
	}

	for _, tc := range testCases {

		mockStorageClient := connector.NewMockGcloudStorageClient(s.controller)
		mockBucketHandleClient := connector.NewMockBucketHandleWrapper(s.controller)
		mockObjectHandler := connector.NewMockObjectHandleWrapper(s.controller)

		mockStorageClient.EXPECT().Bucket(tc.bucketName).Return(mockBucketHandleClient).MaxTimes(1)
		mockBucketHandleClient.EXPECT().Attrs(tc.callContext).Return(nil, tc.bucketExpectedError).MaxTimes(1)
		mockBucketHandleClient.EXPECT().Object(tc.fileName).Return(mockObjectHandler).MaxTimes(1)
		mockObjectHandler.EXPECT().Attrs(tc.callContext).Return(nil, tc.objectExpectedError).MaxTimes(1)
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
	mockStorageClient := connector.NewMockGcloudStorageClient(s.controller)
	mockBucketHandleClient := connector.NewMockBucketHandleWrapper(s.controller)
	mockObjectHandler := connector.NewMockObjectHandleWrapper(s.controller)
	mockReader := connector.NewMockReaderWrapper(s.controller)

	storageWrapper, _ := connector.NewClientWithParams(mockStorageClient)

	mockStorageClient.EXPECT().Bucket("my-bucket-cad").Return(mockBucketHandleClient)
	mockBucketHandleClient.EXPECT().Object("temporal_archival/development/myfile.history").Return(mockObjectHandler)
	mockObjectHandler.EXPECT().NewReader(ctx).Return(mockReader, nil)
	mockReader.EXPECT().Read(gomock.Any()).Return(2, io.EOF)
	mockReader.EXPECT().Close().Return(nil)

	URI, err := archiver.NewURI("gs://my-bucket-cad/temporal_archival/development")
	s.Require().NoError(err)
	_, err = storageWrapper.Get(ctx, URI, "myfile.history")
	s.Require().NoError(err)
}

func (s *clientSuite) TestWrongGoogleCredentialsPath() {
	ctx := context.Background()
	s.T().Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/Wrong/path")
	_, err := connector.NewClient(ctx, &config.GstorageArchiver{})
	s.Require().Error(err)
}

func (s *clientSuite) TestQuery() {

	ctx := context.Background()
	mockBucketHandleClient := connector.NewMockBucketHandleWrapper(s.controller)
	mockStorageClient := connector.NewMockGcloudStorageClient(s.controller)
	mockObjectIterator := connector.NewMockObjectIteratorWrapper(s.controller)
	storageWrapper, _ := connector.NewClientWithParams(mockStorageClient)

	attr := new(storage.ObjectAttrs)
	attr.Name = "fileName_01"

	mockStorageClient.EXPECT().Bucket("my-bucket-cad").Return(mockBucketHandleClient)
	mockBucketHandleClient.EXPECT().Objects(ctx, gomock.Any()).Return(mockObjectIterator)
	mockIterator := 0
	mockObjectIterator.EXPECT().Next().DoAndReturn(func() (*storage.ObjectAttrs, error) {
		mockIterator++
		if mockIterator == 1 {
			return attr, nil
		}
		return nil, iterator.Done

	}).Times(2)

	var fileNames []string
	URI, err := archiver.NewURI("gs://my-bucket-cad/temporal_archival/development")
	s.Require().NoError(err)
	fileNames, err = storageWrapper.Query(ctx, URI, "7478875943689868082123907395549832634615673687049942026838")
	s.Require().NoError(err)
	s.Equal(strings.Join(fileNames, ", "), "fileName_01")
}

func (s *clientSuite) TestQueryWithFilter() {
	ctx := context.Background()
	mockBucketHandleClient := connector.NewMockBucketHandleWrapper(s.controller)
	mockStorageClient := connector.NewMockGcloudStorageClient(s.controller)
	mockObjectIterator := connector.NewMockObjectIteratorWrapper(s.controller)
	storageWrapper, _ := connector.NewClientWithParams(mockStorageClient)

	attr := new(storage.ObjectAttrs)
	attr.Name = "closeTimeout_2020-02-27T09:42:28Z_12851121011173788097_4418294404690464320_15619178330501475177.visibility"
	attrInvalid := new(storage.ObjectAttrs)
	attrInvalid.Name = "closeTimeout_2020-02-27T09:42:28Z_12851121011173788097_4418294404690464321_15619178330501475177.visibility"

	mockStorageClient.EXPECT().Bucket("my-bucket-cad").Return(mockBucketHandleClient)
	mockBucketHandleClient.EXPECT().Objects(ctx, gomock.Any()).Return(mockObjectIterator)
	mockIterator := 0
	mockObjectIterator.EXPECT().Next().DoAndReturn(func() (*storage.ObjectAttrs, error) {
		mockIterator++
		switch mockIterator {
		case 1:
			return attr, nil
		case 2:
			return attrInvalid, nil
		default:
			return nil, iterator.Done
		}

	}).Times(3)

	var fileNames []string
	URI, err := archiver.NewURI("gs://my-bucket-cad/temporal_archival/development")
	s.Require().NoError(err)
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

// Ensures that no code in this package or its parent folder accidentally uses gRPC functions
// since they are stripped from the binary via `disable_grpc_modules`. This is crude but effective.
func (s *clientSuite) TestNoGRPCUsage() {
	currentPackageFiles, err := filepath.Glob("*.go")
	s.NoError(err)
	parentPackageFiles, err := filepath.Glob("../*.go")
	s.NoError(err)
	allFiles := append(currentPackageFiles, parentPackageFiles...)

	var checkedClientFile bool
	for _, file := range allFiles {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}

		content, err := os.ReadFile(file)
		s.NoError(err)

		if strings.Contains(string(content), "NewGRPCClient") {
			s.T().Errorf("❌ Found forbidden gRPC usage in file: %s", file)
		}

		// Check for client.go in both current and parent directories
		checkedClientFile = checkedClientFile || strings.HasSuffix(file, "client.go")
	}

	s.True(checkedClientFile, "should have checked client.go for gRPC usage")
}
