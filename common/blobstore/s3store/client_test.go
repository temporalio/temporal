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

package s3store

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/url"
	"sort"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber/cadence/common/blobstore"
	"github.com/uber/cadence/common/blobstore/blob"
	"github.com/uber/cadence/common/blobstore/s3store/mocks"
)

const (
	defaultBucketName          = "default-bucket-name"
	defaultBucketOwner         = "default-bucket-owner"
	defaultBucketRetentionDays = 10
	errNotFound                = "NotFound"
)

type ClientSuite struct {
	*require.Assertions
	suite.Suite
	s3cli *mocks.S3API
}

func TestClientSuite(t *testing.T) {
	suite.Run(t, new(ClientSuite))
}

func (s *ClientSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.s3cli = &mocks.S3API{}
}

func (s *ClientSuite) TestUploadDownload_Success() {
	client := s.constructClient()
	s.setupFsEmulation()
	s.s3cli.On("DeleteObjectWithContext", mock.Anything, mock.Anything).Return(nil, nil)

	b := blob.NewBlob([]byte("body version 1"), map[string]string{})
	key, err := blob.NewKeyFromString("blob.blob")
	s.NoError(err)
	s.NoError(client.Upload(context.Background(), defaultBucketName, key, b))
	downloadBlob, err := client.Download(context.Background(), defaultBucketName, key)
	s.NoError(err)
	s.NotNil(downloadBlob)
	s.assertBlobEquals(map[string]string{}, "body version 1", downloadBlob)
	b = blob.NewBlob([]byte("body version 2"), map[string]string{"key": "value"})
	s.NoError(client.Upload(context.Background(), defaultBucketName, key, b))
	downloadBlob, err = client.Download(context.Background(), defaultBucketName, key)
	s.NoError(err)
	s.NotNil(downloadBlob)
	s.assertBlobEquals(map[string]string{"key": "value"}, "body version 2", downloadBlob)
}

func (s *ClientSuite) TestUpload_Fail_BucketNotExists() {
	client := s.constructClient()
	s.s3cli.On("PutObjectWithContext", mock.Anything, mock.Anything).Return(nil, awserr.New(s3.ErrCodeNoSuchBucket, "", nil))

	b := blob.NewBlob([]byte("blob body"), map[string]string{"tagKey": "tagValue"})
	key, err := blob.NewKeyFromString("blob.blob")
	s.NoError(err)
	s.Equal(blobstore.ErrBucketNotExists, client.Upload(context.Background(), "bucket-not-exists", key, b))
}

func (s *ClientSuite) TestDownload_Fail_BucketNotExists() {
	client := s.constructClient()
	s.s3cli.On("GetObjectWithContext", mock.Anything, mock.Anything).Return(nil, awserr.New(s3.ErrCodeNoSuchBucket, "", nil))

	key, err := blob.NewKeyFromString("blobname.ext")
	s.NoError(err)
	b, err := client.Download(context.Background(), "bucket-not-exists", key)
	s.Equal(blobstore.ErrBucketNotExists, err)
	s.Nil(b)
}

func (s *ClientSuite) TestGetTags_Fail_BucketNotExists() {
	client := s.constructClient()
	s.s3cli.On("GetObjectTaggingWithContext", mock.Anything, mock.Anything).Return(nil, awserr.New(s3.ErrCodeNoSuchBucket, "", nil))

	key, err := blob.NewKeyFromString("blobname.ext")
	s.NoError(err)
	b, err := client.GetTags(context.Background(), "bucket-not-exists", key)
	s.Equal(blobstore.ErrBucketNotExists, err)
	s.Nil(b)
}

func (s *ClientSuite) TestGetTags_Fail_BlobNotExists() {
	client := s.constructClient()
	s.s3cli.On("GetObjectTaggingWithContext", mock.Anything, mock.Anything).Return(nil, awserr.New(s3.ErrCodeNoSuchKey, "", nil))

	key, err := blob.NewKeyFromString("blobname.ext")
	s.NoError(err)
	b, err := client.GetTags(context.Background(), defaultBucketName, key)
	s.Equal(blobstore.ErrBlobNotExists, err)
	s.Nil(b)
}

func (s *ClientSuite) TestDownload_Fail_BlobNotExists() {
	client := s.constructClient()
	s.s3cli.On("GetObjectWithContext", mock.Anything, mock.Anything).Return(nil, awserr.New(s3.ErrCodeNoSuchKey, "", nil))

	key, err := blob.NewKeyFromString("blobname.ext")
	s.NoError(err)
	b, err := client.Download(context.Background(), defaultBucketName, key)
	s.Equal(blobstore.ErrBlobNotExists, err)
	s.Nil(b)
}

func (s *ClientSuite) TestExists_Fail_BucketNotExists() {
	client := s.constructClient()
	s.s3cli.On("HeadBucketWithContext", mock.Anything, mock.MatchedBy(func(input *s3.HeadBucketInput) bool {
		return *input.Bucket == "bucket-not-exists"
	})).Return(nil, awserr.New(errNotFound, "", nil))
	s.s3cli.On("HeadObjectWithContext", mock.Anything, mock.Anything).Return(nil, awserr.New(errNotFound, "", nil))

	key, err := blob.NewKeyFromString("blob.blob")
	s.NoError(err)
	exists, err := client.Exists(context.Background(), "bucket-not-exists", key)
	s.Equal(blobstore.ErrBucketNotExists, err)
	s.False(exists)
}

func (s *ClientSuite) TestExists_Success() {
	client := s.constructClient()
	s.s3cli.On("HeadBucketWithContext", mock.Anything, mock.Anything).Return(nil, nil)
	s.s3cli.On("HeadObjectWithContext", mock.Anything, mock.Anything).Return(nil, awserr.New(errNotFound, "", nil)).Once()
	s.s3cli.On("HeadObjectWithContext", mock.Anything, mock.Anything).Return(&s3.HeadObjectOutput{}, nil)
	s.s3cli.On("PutObjectWithContext", mock.Anything, mock.Anything).Return(nil, nil)

	key, err := blob.NewKeyFromString("TestExistsSuccess.blob")
	s.NoError(err)
	exists, err := client.Exists(context.Background(), defaultBucketName, key)
	s.NoError(err)
	s.False(exists)

	b := blob.NewBlob([]byte("body"), map[string]string{})
	s.NoError(client.Upload(context.Background(), defaultBucketName, key, b))
	exists, err = client.Exists(context.Background(), defaultBucketName, key)
	s.NoError(err)
	s.True(exists)
}

func (s *ClientSuite) TestDelete_Fail_BucketNotExists() {
	client := s.constructClient()
	s.s3cli.On("DeleteObjectWithContext", mock.Anything, mock.Anything).Return(nil, awserr.New(s3.ErrCodeNoSuchBucket, "", nil))

	key, err := blob.NewKeyFromString("blob.blob")
	s.NoError(err)
	deleted, err := client.Delete(context.Background(), "bucket-not-exists", key)
	s.Equal(blobstore.ErrBucketNotExists, err)
	s.False(deleted)
}

func (s *ClientSuite) TestDelete_Success() {
	client := s.constructClient()
	s.s3cli.On("HeadBucketWithContext", mock.Anything, mock.Anything).Return(nil, nil)
	s.s3cli.On("DeleteObjectWithContext", mock.Anything, mock.Anything).Return(nil, nil)
	s.s3cli.On("PutObjectWithContext", mock.Anything, mock.Anything).Return(nil, nil)
	s.s3cli.On("HeadObjectWithContext", mock.Anything, mock.Anything).Return(nil, awserr.New(errNotFound, "", nil))

	key, err := blob.NewKeyFromString("blobnonexistent.blob")
	s.NoError(err)
	deleted, err := client.Delete(context.Background(), defaultBucketName, key)
	s.NoError(err)
	s.True(deleted)

	b := blob.NewBlob([]byte("body"), map[string]string{})
	s.NoError(client.Upload(context.Background(), defaultBucketName, key, b))
	deleted, err = client.Delete(context.Background(), defaultBucketName, key)
	s.NoError(err)
	s.True(deleted)
	exists, err := client.Exists(context.Background(), defaultBucketName, key)
	s.NoError(err)
	s.False(exists)
}

func (s *ClientSuite) TestListByPrefix_Fail_BucketNotExists() {
	client := s.constructClient()
	s.s3cli.On("ListObjectsV2WithContext", mock.Anything, mock.Anything).Return(nil, awserr.New(s3.ErrCodeNoSuchBucket, "", nil))

	files, err := client.ListByPrefix(context.Background(), "bucket-not-exists", "foo")
	s.Equal(blobstore.ErrBucketNotExists, err)
	s.Nil(files)
}

func (s *ClientSuite) TestListByPrefix_Success() {
	client := s.constructClient()
	allFiles := []string{"matching_1.ext", "matching_2.ext", "not_matching_1.ext", "not_matching_2.ext", "matching_3.ext"}
	s.s3cli.On("PutObjectWithContext", mock.Anything, mock.Anything).Return(nil, nil)
	s.s3cli.On("ListObjectsV2WithContext", mock.Anything, mock.Anything).
		Return(func(_ context.Context, input *s3.ListObjectsV2Input, opts ...request.Option) *s3.ListObjectsV2Output {
			objects := make([]*s3.Object, 0)
			for _, k := range allFiles {
				if strings.HasPrefix(k, *input.Prefix) {
					objects = append(objects, &s3.Object{
						Key: aws.String(k),
					})
				}
			}
			sort.SliceStable(objects, func(i, j int) bool {
				return *objects[i].Key < *objects[j].Key

			})
			return &s3.ListObjectsV2Output{
				Contents: objects,
			}
		}, nil)

	for _, f := range allFiles {
		key, err := blob.NewKeyFromString(f)
		s.NoError(err)
		b := blob.NewBlob([]byte("blob body"), map[string]string{})
		s.NoError(client.Upload(context.Background(), defaultBucketName, key, b))
	}
	matchingKeys, err := client.ListByPrefix(context.Background(), defaultBucketName, "matching")
	s.NoError(err)
	var matchingFilenames []string
	for _, k := range matchingKeys {
		matchingFilenames = append(matchingFilenames, k.String())
	}
	s.Equal([]string{"matching_1.ext", "matching_2.ext", "matching_3.ext"}, matchingFilenames)
}

func (s *ClientSuite) TestBucketMetadata_Fail_BucketNotExists() {
	client := s.constructClient()
	s.s3cli.On("GetBucketAclWithContext", mock.Anything, mock.MatchedBy(func(input *s3.GetBucketAclInput) bool {
		return *input.Bucket == "bucket-not-exists"
	})).Return(nil, awserr.New(s3.ErrCodeNoSuchBucket, "", nil))

	metadata, err := client.BucketMetadata(context.Background(), "bucket-not-exists")
	s.Equal(blobstore.ErrBucketNotExists, err)
	s.Nil(metadata)
}

func (s *ClientSuite) TestBucketMetadata_Success() {
	client := s.constructClient()
	s.s3cli.On("GetBucketAclWithContext", mock.Anything, mock.Anything).Return(&s3.GetBucketAclOutput{
		Owner: &s3.Owner{
			DisplayName: aws.String("default-bucket-owner"),
		},
	}, nil)

	days := int64(defaultBucketRetentionDays)
	s.s3cli.On("GetBucketLifecycleConfigurationWithContext", mock.Anything, mock.Anything).
		Return(&s3.GetBucketLifecycleConfigurationOutput{Rules: []*s3.LifecycleRule{
			{
				Status: aws.String("Enabled"),
				Expiration: &s3.LifecycleExpiration{
					Days: &days,
				},
			},
		}}, nil)

	metadata, err := client.BucketMetadata(context.Background(), defaultBucketName)
	s.NoError(err)
	s.NotNil(metadata)
	s.Equal(defaultBucketRetentionDays, metadata.RetentionDays)
	s.Equal(defaultBucketOwner, metadata.Owner)
}

func (s *ClientSuite) TestBucketExists() {
	client := s.constructClient()
	s.s3cli.On("HeadBucketWithContext", mock.Anything, mock.MatchedBy(func(input *s3.HeadBucketInput) bool {
		return *input.Bucket == "bucket-not-exists"
	})).Return(nil, awserr.New(errNotFound, "", nil))
	s.s3cli.On("HeadBucketWithContext", mock.Anything, mock.Anything).Return(nil, nil)

	exists, err := client.BucketExists(context.Background(), "bucket-not-exists")
	s.NoError(err)
	s.False(exists)

	exists, err = client.BucketExists(context.Background(), defaultBucketName)
	s.NoError(err)
	s.True(exists)
}

func (s *ClientSuite) constructClient() blobstore.Client {
	return NewClient(s.s3cli)
}

func (s *ClientSuite) assertBlobEquals(expectedTags map[string]string, expectedBody string, actual *blob.Blob) {
	s.Equal(expectedTags, actual.Tags)
	s.Equal(expectedBody, string(actual.Body))
}

func (s *ClientSuite) setupFsEmulation() {
	fs := make(map[string]string)
	tags := make(map[string]string)

	putObjectFn := func(_ aws.Context, input *s3.PutObjectInput, _ ...request.Option) *s3.PutObjectOutput {
		buf := new(bytes.Buffer)
		buf.ReadFrom(input.Body)

		fs[*input.Bucket+*input.Key] = buf.String()
		if input.Tagging != nil {
			tags[*input.Bucket+*input.Key] = *input.Tagging
		}
		return &s3.PutObjectOutput{}
	}
	getObjectFn := func(_ aws.Context, input *s3.GetObjectInput, _ ...request.Option) *s3.GetObjectOutput {
		c := fs[*input.Bucket+*input.Key]
		return &s3.GetObjectOutput{
			Body: ioutil.NopCloser(bytes.NewReader([]byte(c))),
		}

	}
	getTagsFn := func(_ aws.Context, input *s3.GetObjectTaggingInput, _ ...request.Option) *s3.GetObjectTaggingOutput {
		t := tags[*input.Bucket+*input.Key]
		ql, _ := url.ParseQuery(t)

		tags := make([]*s3.Tag, 0, len(ql))
		for k, v := range ql {

			tag := s3.Tag{
				Key:   &k,
				Value: &v[0],
			}
			tags = append(tags, &tag)

		}
		return &s3.GetObjectTaggingOutput{
			TagSet: tags,
		}
	}

	s.s3cli.On("PutObjectWithContext", mock.Anything, mock.Anything).Return(putObjectFn, nil)
	s.s3cli.On("GetObjectWithContext", mock.Anything, mock.Anything).Return(getObjectFn, nil)
	s.s3cli.On("GetObjectTaggingWithContext", mock.Anything, mock.Anything).Return(getTagsFn, nil)
}
