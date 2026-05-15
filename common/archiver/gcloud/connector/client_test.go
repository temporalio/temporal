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

// TestQueryWithFilters_Pagination verifies pagination correctness for QueryWithFilters
// across all meaningful combinations of page size, offset, filters, and result counts.
func (s *clientSuite) TestQueryWithFilters_Pagination() {
	ctx := context.Background()

	// Helper to build a mock iterator from a slice of names.
	// Each call to Next() returns the next item and the final call returns iterator.Done.
	makeIter := func(names []string) *connector.MockObjectIteratorWrapper {
		it := connector.NewMockObjectIteratorWrapper(s.controller)
		call := 0
		it.EXPECT().Next().DoAndReturn(func() (*storage.ObjectAttrs, error) {
			if call < len(names) {
				a := &storage.ObjectAttrs{Name: names[call]}
				call++
				return a, nil
			}
			return nil, iterator.Done
		}).AnyTimes()
		return it
	}

	// Helper to build a fresh storageWrapper+bucket mock wired to a given iterator.
	makeWrapper := func(it *connector.MockObjectIteratorWrapper) connector.Client {
		mockStorage := connector.NewMockGcloudStorageClient(s.controller)
		mockBucket := connector.NewMockBucketHandleWrapper(s.controller)
		mockStorage.EXPECT().Bucket("my-bucket-cad").Return(mockBucket)
		mockBucket.EXPECT().Objects(ctx, gomock.Any()).Return(it)
		w, _ := connector.NewClientWithParams(mockStorage)
		return w
	}

	URI, err := archiver.NewURI("gs://my-bucket-cad/temporal_archival/development")
	s.Require().NoError(err)

	matchAll := connector.Precondition(func(subject interface{}) bool { return true })
	matchNone := connector.Precondition(func(subject interface{}) bool { return false })
	files3 := []string{"file_a.visibility", "file_b.visibility", "file_c.visibility"}

	type result struct {
		files     []string
		completed bool
		nextPos   int
	}

	testCases := []struct {
		name     string
		rawItems []string
		pageSize int
		offset   int
		filters  []connector.Precondition
		want     result
	}{
		{
			// Fewer results than page size: iterator ends before page is full → completed.
			name:     "NoFilter_FewerResultsThanPageSize",
			rawItems: files3[:2],
			pageSize: 5,
			offset:   0,
			filters:  nil,
			want:     result{files: []string{"file_a.visibility", "file_b.visibility"}, completed: true, nextPos: 2},
		},
		{
			// Results exactly equal to page size and then iterator ends.
			// This is the original bug: must return completed=false so the caller
			// generates a NextPageToken — there may be more records at the next offset.
			name:     "NoFilter_ResultsExactlyPageSize_IteratorEndsSimultaneously",
			rawItems: files3[:2],
			pageSize: 2,
			offset:   0,
			filters:  nil,
			want:     result{files: []string{"file_a.visibility", "file_b.visibility"}, completed: false, nextPos: 2},
		},
		{
			// More results than page size: iterator still has items after page is full.
			name:     "NoFilter_MoreResultsThanPageSize",
			rawItems: files3,
			pageSize: 2,
			offset:   0,
			filters:  nil,
			want:     result{files: []string{"file_a.visibility", "file_b.visibility"}, completed: false, nextPos: 2},
		},
		{
			// Second page via offset: skip first 2 items, return the remaining 1.
			name:     "NoFilter_WithOffset_SecondPage",
			rawItems: files3,
			pageSize: 2,
			offset:   2,
			filters:  nil,
			want:     result{files: []string{"file_c.visibility"}, completed: true, nextPos: 3},
		},
		{
			// Filter matches fewer items than page size → exhausted, completed.
			name:     "WithFilter_FewerMatchesThanPageSize",
			rawItems: files3,
			pageSize: 5,
			offset:   0,
			filters:  []connector.Precondition{func(subject interface{}) bool { return subject.(string) == "file_a.visibility" }},
			want:     result{files: []string{"file_a.visibility"}, completed: true, nextPos: 1},
		},
		{
			// Filter matches exactly page-size items and then iterator returns Done.
			// This is the NEW bug case: even though iterator is done after the last match,
			// we must return completed=false because the caller cannot know there are no
			// more matching records without fetching another page.
			name:     "WithFilter_MatchesExactlyPageSize_IteratorEndsSimultaneously",
			rawItems: files3[:2],
			pageSize: 2,
			offset:   0,
			filters:  []connector.Precondition{matchAll},
			want:     result{files: []string{"file_a.visibility", "file_b.visibility"}, completed: false, nextPos: 2},
		},
		{
			// Filter skips some items; remaining matches exceed page size.
			name:     "WithFilter_MoreMatchesThanPageSize",
			rawItems: files3,
			pageSize: 2,
			offset:   0,
			filters: []connector.Precondition{func(subject interface{}) bool {
				return subject.(string) != "file_b.visibility" // skip b, match a and c
			}},
			want: result{files: []string{"file_a.visibility", "file_c.visibility"}, completed: false, nextPos: 2},
		},
		{
			// No filters, pageSize=0 (unlimited): returns all items, always completed.
			name:     "NoFilter_UnlimitedPageSize",
			rawItems: files3,
			pageSize: 0,
			offset:   0,
			filters:  nil,
			want:     result{files: files3, completed: true, nextPos: 3},
		},
		{
			// All items filtered out: empty result set, completed.
			name:     "WithFilter_NoMatches",
			rawItems: files3,
			pageSize: 5,
			offset:   0,
			filters:  []connector.Precondition{matchNone},
			want:     result{files: []string{}, completed: true, nextPos: 0},
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			it := makeIter(tc.rawItems)
			w := makeWrapper(it)

			gotFiles, gotCompleted, gotPos, gotErr := w.QueryWithFilters(ctx, URI, "", tc.pageSize, tc.offset, tc.filters)

			s.Require().NoError(gotErr, "unexpected error")
			s.Equal(tc.want.completed, gotCompleted, "completed flag mismatch")
			s.Equal(tc.want.nextPos, gotPos, "cursor position mismatch")
			s.Equal(tc.want.files, gotFiles, "returned filenames mismatch")
		})
	}
}

func newWorkflowIDPrecondition(workflowID string) connector.Precondition {
	return func(subject any) bool {

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
