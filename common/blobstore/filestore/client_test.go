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
	"bytes"
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber/cadence/common/blobstore"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"testing"
)

const (
	defaultBucketName          = "default-bucket-name"
	defaultBucketOwner         = "default-bucket-owner"
	defaultBucketRetentionDays = 10
	customBucketNamePrefix     = "custom-bucket-name"
	customBucketOwner          = "custom-bucket-owner"
	customBucketRetentionDays  = 100
	numberOfCustomBuckets      = 5
)

type ClientSuite struct {
	*require.Assertions
	suite.Suite
}

func TestClientSuite(t *testing.T) {
	suite.Run(t, new(ClientSuite))
}

func (s *ClientSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *ClientSuite) TestNewClientInvalidConfig() {
	invalidCfg := &Config{
		StoreDirectory: "/test/store/dir",
		DefaultBucket: BucketConfig{
			Name: "default-bucket-name",
		},
	}

	client, err := NewClient(invalidCfg)
	s.Error(err)
	s.Nil(client)
}

func (s *ClientSuite) TestSetupDirectoryFailure() {
	dir, err := ioutil.TempDir("", "test.setup.directory.failure")
	s.NoError(err)
	defer os.RemoveAll(dir)
	os.Chmod(dir, os.FileMode(0600))

	cfg := s.constructConfig(dir)
	client, err := NewClient(cfg)
	s.Error(err)
	s.Nil(client)
}

func (s *ClientSuite) TestWriteMetadataFilesFailure() {
	dir, err := ioutil.TempDir("", "test.write.metadata.files.failure")
	s.NoError(err)
	defer os.RemoveAll(dir)
	s.NoError(mkdirAll(filepath.Join(dir, defaultBucketName, metadataFilename, "foo")))

	cfg := s.constructConfig(dir)
	client, err := NewClient(cfg)
	s.Error(err)
	s.Nil(client)
}

func (s *ClientSuite) TestUploadBlobBucketNotExists() {
	dir, err := ioutil.TempDir("", "test.upload.blob.bucket.not.exists")
	s.NoError(err)
	defer os.RemoveAll(dir)
	client := s.constructClient(dir)

	blob := s.constructBlob("blob body", map[string]string{"tagKey": "tagValue"})
	blobFilename := "blob.blob"
	s.Equal(blobstore.ErrBucketNotExists, client.UploadBlob(context.Background(), "bucket-not-exists", blobFilename, blob))
}

func (s *ClientSuite) TestUploadBlobErrorOnWrite() {
	dir, err := ioutil.TempDir("", "test.upload.blob.error.on.write")
	s.NoError(err)
	defer os.RemoveAll(dir)

	blobFilename := "blob.blob"
	s.NoError(mkdirAll(path.Join(dir, defaultBucketName, blobFilename, "foo")))
	client := s.constructClient(dir)

	blob := s.constructBlob("blob body", map[string]string{"tagKey": "tagValue"})
	s.Error(client.UploadBlob(context.Background(), defaultBucketName, blobFilename, blob))
}

func (s *ClientSuite) TestDownloadBlobBucketNotExists() {
	dir, err := ioutil.TempDir("", "test.download.blob.bucket.not.exists")
	s.NoError(err)
	defer os.RemoveAll(dir)
	client := s.constructClient(dir)

	blob, err := client.DownloadBlob(context.Background(), "bucket-not-exists", "blobname")
	s.Equal(blobstore.ErrBucketNotExists, err)
	s.Nil(blob)
}

func (s *ClientSuite) TestDownloadBlobBlobNotExists() {
	dir, err := ioutil.TempDir("", "test.download.blob.blob.not.exists")
	s.NoError(err)
	defer os.RemoveAll(dir)
	client := s.constructClient(dir)

	blob, err := client.DownloadBlob(context.Background(), defaultBucketName, "blobname")
	s.Equal(blobstore.ErrBlobNotExists, err)
	s.Nil(blob)
}

func (s *ClientSuite) TestDownloadBlobNoPermissions() {
	dir, err := ioutil.TempDir("", "test.download.blob.no.permissions")
	s.NoError(err)
	defer os.RemoveAll(dir)
	client := s.constructClient(dir)

	blob := s.constructBlob("blob body", map[string]string{"tagKey": "tagValue"})
	blobFilename := "blob.blob"
	s.NoError(client.UploadBlob(context.Background(), defaultBucketName, blobFilename, blob))
	os.Chmod(bucketItemPath(dir, defaultBucketName, blobFilename), os.FileMode(0000))

	blob, err = client.DownloadBlob(context.Background(), defaultBucketName, blobFilename)
	s.NotEqual(blobstore.ErrBlobNotExists, err)
	s.Error(err)
	s.Nil(blob)
}

func (s *ClientSuite) TestDownloadBlobInvalidFormat() {
	dir, err := ioutil.TempDir("", "test.download.blob.invalid.format")
	s.NoError(err)
	defer os.RemoveAll(dir)

	client := s.constructClient(dir)
	blobFilename := "blob.blob"
	s.NoError(writeFile(filepath.Join(dir, defaultBucketName, blobFilename), []byte("invalid")))

	blob, err := client.DownloadBlob(context.Background(), defaultBucketName, blobFilename)
	s.NotEqual(blobstore.ErrBlobNotExists, err)
	s.Error(err)
	s.Nil(blob)
}

func (s *ClientSuite) TestUploadDownloadBlob() {
	dir, err := ioutil.TempDir("", "test.upload.download.blob")
	s.NoError(err)
	defer os.RemoveAll(dir)

	client := s.constructClient(dir)
	blob := s.constructBlob("body version 1", map[string]string{})
	blobFilename := "blob.blob"
	s.NoError(client.UploadBlob(context.Background(), defaultBucketName, blobFilename, blob))
	downloadBlob, err := client.DownloadBlob(context.Background(), defaultBucketName, blobFilename)
	s.NoError(err)
	s.NotNil(downloadBlob)
	s.assertBlobEquals(map[string]string{}, "body version 1", downloadBlob)

	blob = s.constructBlob("body version 2", map[string]string{"key": "value"})
	s.NoError(client.UploadBlob(context.Background(), defaultBucketName, blobFilename, blob))
	downloadBlob, err = client.DownloadBlob(context.Background(), defaultBucketName, blobFilename)
	s.NoError(err)
	s.NotNil(downloadBlob)
	s.assertBlobEquals(map[string]string{"key": "value"}, "body version 2", downloadBlob)
}

func (s *ClientSuite) TestUploadDownloadBlobCustomBucket() {
	dir, err := ioutil.TempDir("", "test.upload.download.blob.custom.bucket")
	s.NoError(err)
	defer os.RemoveAll(dir)

	client := s.constructClient(dir)
	blob := s.constructBlob("blob body", map[string]string{})
	blobFilename := "blob.blob"
	customBucketName := fmt.Sprintf("%v-%v", customBucketNamePrefix, 3)
	s.NoError(client.UploadBlob(context.Background(), customBucketName, blobFilename, blob))
	downloadBlob, err := client.DownloadBlob(context.Background(), customBucketName, blobFilename)
	s.NoError(err)
	s.NotNil(downloadBlob)
	s.assertBlobEquals(map[string]string{}, "blob body", downloadBlob)
}

func (s *ClientSuite) TestBucketMetadataBucketNotExists() {
	dir, err := ioutil.TempDir("", "test.bucket.metadata.bucket.not.exists")
	s.NoError(err)
	defer os.RemoveAll(dir)
	client := s.constructClient(dir)

	metadata, err := client.BucketMetadata(context.Background(), "bucket-not-exists")
	s.Equal(blobstore.ErrBucketNotExists, err)
	s.Nil(metadata)
}

func (s *ClientSuite) TestBucketMetadataCheckFileExistsError() {
	dir, err := ioutil.TempDir("", "test.bucket.metadata.check.file.exists.error")
	s.NoError(err)
	defer os.RemoveAll(dir)
	client := s.constructClient(dir)
	s.NoError(os.Chmod(bucketDirectory(dir, defaultBucketName), os.FileMode(0000)))

	metadata, err := client.BucketMetadata(context.Background(), defaultBucketName)
	s.Error(err)
	s.Nil(metadata)
}

func (s *ClientSuite) TestBucketMetadataFileNotExistsError() {
	dir, err := ioutil.TempDir("", "test.bucket.metadata.file.not.exists.error")
	s.NoError(err)
	defer os.RemoveAll(dir)
	client := s.constructClient(dir)
	s.NoError(os.Remove(bucketItemPath(dir, defaultBucketName, metadataFilename)))

	metadata, err := client.BucketMetadata(context.Background(), defaultBucketName)
	s.Error(err)
	s.Nil(metadata)
}

func (s *ClientSuite) TestBucketMetadataFileInvalidForm() {
	dir, err := ioutil.TempDir("", "test.bucket.metadata.file.invalid.form")
	s.NoError(err)
	defer os.RemoveAll(dir)
	client := s.constructClient(dir)
	s.NoError(writeFile(bucketItemPath(dir, defaultBucketName, metadataFilename), []byte("invalid")))

	metadata, err := client.BucketMetadata(context.Background(), defaultBucketName)
	s.Error(err)
	s.Nil(metadata)
}

func (s *ClientSuite) TestBucketMetadataSuccess() {
	dir, err := ioutil.TempDir("", "test.bucket.metadata.success")
	s.NoError(err)
	defer os.RemoveAll(dir)
	client := s.constructClient(dir)

	metadata, err := client.BucketMetadata(context.Background(), defaultBucketName)
	s.NoError(err)
	s.NotNil(metadata)
	s.Equal(defaultBucketRetentionDays, metadata.RetentionDays)
	s.Equal(defaultBucketOwner, metadata.Owner)
}

func (s *ClientSuite) constructBlob(body string, tags map[string]string) *blobstore.Blob {
	return &blobstore.Blob{
		Body:            bytes.NewReader([]byte(body)),
		Tags:            tags,
		CompressionType: blobstore.NoCompression,
	}
}

func (s *ClientSuite) constructClient(storeDir string) blobstore.Client {
	cfg := s.constructConfig(storeDir)
	client, err := NewClient(cfg)
	s.NoError(err)
	s.NotNil(client)
	return client
}

func (s *ClientSuite) constructConfig(storeDir string) *Config {
	cfg := &Config{
		StoreDirectory: storeDir,
	}
	cfg.DefaultBucket = BucketConfig{
		Name:          defaultBucketName,
		Owner:         defaultBucketOwner,
		RetentionDays: defaultBucketRetentionDays,
	}

	for i := 0; i < numberOfCustomBuckets; i++ {
		cfg.CustomBuckets = append(cfg.CustomBuckets, BucketConfig{
			Name:          fmt.Sprintf("%v-%v", customBucketNamePrefix, i),
			Owner:         customBucketOwner,
			RetentionDays: customBucketRetentionDays,
		})
	}
	return cfg
}

func (s *ClientSuite) assertBlobEquals(expectedTags map[string]string, expectedBody string, actual *blobstore.Blob) {
	s.Equal(blobstore.NoCompression, actual.CompressionType)
	s.Equal(expectedTags, actual.Tags)
	actualBody, err := ioutil.ReadAll(actual.Body)
	s.NoError(err)
	s.Equal(expectedBody, string(actualBody))
}
