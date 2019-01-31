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
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	"github.com/uber/cadence/common/blobstore"
	"github.com/uber/cadence/common/blobstore/blob"
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

func (s *ClientSuite) TestNewClient_Fail_InvalidConfig() {
	invalidCfg := &Config{
		StoreDirectory: "/test/store/dir",
		DefaultBucket: BucketConfig{
			Name: "default-bucket-name",
		},
	}

	client, err := NewClient(invalidCfg, bark.NewNopLogger())
	s.Error(err)
	s.Nil(client)
}

func (s *ClientSuite) TestNewClient_Fail_SetupDirectoryFailure() {
	dir, err := ioutil.TempDir("", "TestNewClient_Fail_SetupDirectoryFailure")
	s.NoError(err)
	defer os.RemoveAll(dir)
	os.Chmod(dir, os.FileMode(0600))

	cfg := s.constructConfig(dir)
	client, err := NewClient(cfg, bark.NewNopLogger())
	s.Error(err)
	s.Nil(client)
}

func (s *ClientSuite) TestNewClient_Fail_WriteMetadataFilesFailure() {
	dir, err := ioutil.TempDir("", "TestNewClient_Fail_WriteMetadataFilesFailure")
	s.NoError(err)
	defer os.RemoveAll(dir)
	s.NoError(mkdirAll(filepath.Join(dir, defaultBucketName, metadataFilename, "foo")))

	cfg := s.constructConfig(dir)
	client, err := NewClient(cfg, bark.NewNopLogger())
	s.Error(err)
	s.Nil(client)
}

func (s *ClientSuite) TestUpload_Fail_BucketNotExists() {
	dir, err := ioutil.TempDir("", "TestUpload_Fail_BucketNotExists")
	s.NoError(err)
	defer os.RemoveAll(dir)
	client := s.constructClient(dir)

	b := blob.NewBlob([]byte("blob body"), map[string]string{"tagKey": "tagValue"})
	key, err := blob.NewKeyFromString("blob.blob")
	s.NoError(err)
	s.Equal(blobstore.ErrBucketNotExists, client.Upload(context.Background(), "bucket-not-exists", key, b))
}

func (s *ClientSuite) TestUpload_Fail_ErrorOnWrite() {
	dir, err := ioutil.TempDir("", "TestUpload_Fail_ErrorOnWrite")
	s.NoError(err)
	defer os.RemoveAll(dir)

	key, err := blob.NewKeyFromString("blob.blob")
	s.NoError(err)
	s.NoError(mkdirAll(path.Join(dir, defaultBucketName, key.String(), "foo")))
	client := s.constructClient(dir)

	b := blob.NewBlob([]byte("blob body"), map[string]string{"tagKey": "tagValue"})
	s.Equal(ErrWriteFile, client.Upload(context.Background(), defaultBucketName, key, b))
}

func (s *ClientSuite) TestDownload_Fail_BucketNotExists() {
	dir, err := ioutil.TempDir("", "TestDownload_Fail_BucketNotExists")
	s.NoError(err)
	defer os.RemoveAll(dir)
	client := s.constructClient(dir)

	key, err := blob.NewKeyFromString("blobname.ext")
	s.NoError(err)
	b, err := client.Download(context.Background(), "bucket-not-exists", key)
	s.Equal(blobstore.ErrBucketNotExists, err)
	s.Nil(b)
}

func (s *ClientSuite) TestDownload_Fail_BlobNotExists() {
	dir, err := ioutil.TempDir("", "TestDownload_Fail_BlobNotExists")
	s.NoError(err)
	defer os.RemoveAll(dir)
	client := s.constructClient(dir)

	key, err := blob.NewKeyFromString("blobname.ext")
	s.NoError(err)
	b, err := client.Download(context.Background(), defaultBucketName, key)
	s.Equal(blobstore.ErrBlobNotExists, err)
	s.Nil(b)
}

func (s *ClientSuite) TestDownload_Fail_NoPermissions() {
	dir, err := ioutil.TempDir("", "TestDownload_Fail_NoPermissions")
	s.NoError(err)
	defer os.RemoveAll(dir)
	client := s.constructClient(dir)

	b := blob.NewBlob([]byte("blob body"), map[string]string{"tagKey": "tagValue"})
	key, err := blob.NewKeyFromString("blob.blob")
	s.NoError(err)
	s.NoError(client.Upload(context.Background(), defaultBucketName, key, b))
	os.Chmod(bucketItemPath(dir, defaultBucketName, key.String()), os.FileMode(0000))

	b, err = client.Download(context.Background(), defaultBucketName, key)
	s.Equal(ErrReadFile, err)
	s.Nil(b)
}

func (s *ClientSuite) TestDownload_Fail_BlobFormatInvalid() {
	dir, err := ioutil.TempDir("", "TestDownload_Fail_BlobFormatInvalid")
	s.NoError(err)
	defer os.RemoveAll(dir)

	client := s.constructClient(dir)
	key, err := blob.NewKeyFromString("blob.blob")
	s.NoError(err)
	s.NoError(writeFile(filepath.Join(dir, defaultBucketName, key.String()), []byte("invalid")))

	b, err := client.Download(context.Background(), defaultBucketName, key)
	s.Equal(blobstore.ErrBlobDeserialization, err)
	s.Nil(b)
}

func (s *ClientSuite) TestUploadDownload_Success() {
	dir, err := ioutil.TempDir("", "TestUploadDownload_Success")
	s.NoError(err)
	defer os.RemoveAll(dir)

	client := s.constructClient(dir)
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

func (s *ClientSuite) TestUploadDownload_Success_CustomBucket() {
	dir, err := ioutil.TempDir("", "TestUploadDownload_Success_CustomBucket")
	s.NoError(err)
	defer os.RemoveAll(dir)

	client := s.constructClient(dir)
	b := blob.NewBlob([]byte("blob body"), map[string]string{})
	key, err := blob.NewKeyFromString("blob.blob")
	s.NoError(err)
	customBucketName := fmt.Sprintf("%v-%v", customBucketNamePrefix, 3)
	s.NoError(client.Upload(context.Background(), customBucketName, key, b))
	downloadBlob, err := client.Download(context.Background(), customBucketName, key)
	s.NoError(err)
	s.NotNil(downloadBlob)
	s.assertBlobEquals(map[string]string{}, "blob body", downloadBlob)
}

func (s *ClientSuite) TestExists_Fail_BucketNotExists() {
	dir, err := ioutil.TempDir("", "TestExists_Fail_BucketNotExists")
	s.NoError(err)
	defer os.RemoveAll(dir)
	client := s.constructClient(dir)

	key, err := blob.NewKeyFromString("blob.blob")
	s.NoError(err)
	exists, err := client.Exists(context.Background(), "bucket-not-exists", key)
	s.Equal(blobstore.ErrBucketNotExists, err)
	s.False(exists)
}

func (s *ClientSuite) TestExists_Success() {
	dir, err := ioutil.TempDir("", "TestExists_Success")
	s.NoError(err)
	defer os.RemoveAll(dir)
	client := s.constructClient(dir)

	key, err := blob.NewKeyFromString("blob.blob")
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
	dir, err := ioutil.TempDir("", "TestDelete_Fail_BucketNotExists")
	s.NoError(err)
	defer os.RemoveAll(dir)
	client := s.constructClient(dir)

	key, err := blob.NewKeyFromString("blob.blob")
	s.NoError(err)
	deleted, err := client.Delete(context.Background(), "bucket-not-exists", key)
	s.Equal(blobstore.ErrBucketNotExists, err)
	s.False(deleted)
}

func (s *ClientSuite) TestDelete_Success() {
	dir, err := ioutil.TempDir("", "TestDelete_Success")
	s.NoError(err)
	defer os.RemoveAll(dir)
	client := s.constructClient(dir)

	key, err := blob.NewKeyFromString("blob.blob")
	s.NoError(err)
	deleted, err := client.Delete(context.Background(), defaultBucketName, key)
	s.NoError(err)
	s.False(deleted)

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
	dir, err := ioutil.TempDir("", "TestListByPrefix_Fail_BucketNotExists")
	s.NoError(err)
	defer os.RemoveAll(dir)
	client := s.constructClient(dir)

	files, err := client.ListByPrefix(context.Background(), "bucket-not-exists", "foo")
	s.Equal(blobstore.ErrBucketNotExists, err)
	s.Nil(files)
}

func (s *ClientSuite) TestListByPrefix_Success() {
	dir, err := ioutil.TempDir("", "TestListByPrefix_Success")
	s.NoError(err)
	defer os.RemoveAll(dir)
	client := s.constructClient(dir)

	allFiles := []string{"matching_1.ext", "matching_2.ext", "not_matching_1.ext", "not_matching_2.ext", "matching_3.ext"}
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
	dir, err := ioutil.TempDir("", "TestBucketMetadata_Fail_BucketNotExists")
	s.NoError(err)
	defer os.RemoveAll(dir)
	client := s.constructClient(dir)

	metadata, err := client.BucketMetadata(context.Background(), "bucket-not-exists")
	s.Equal(blobstore.ErrBucketNotExists, err)
	s.Nil(metadata)
}

func (s *ClientSuite) TestBucketMetadata_Fail_FileNotExistsError() {
	dir, err := ioutil.TempDir("", "TestBucketMetadata_Fail_FileNotExistsError")
	s.NoError(err)
	defer os.RemoveAll(dir)
	client := s.constructClient(dir)
	s.NoError(os.Remove(bucketItemPath(dir, defaultBucketName, metadataFilename)))

	metadata, err := client.BucketMetadata(context.Background(), defaultBucketName)
	s.Equal(ErrReadFile, err)
	s.Nil(metadata)
}

func (s *ClientSuite) TestBucketMetadata_Fail_InvalidFileFormat() {
	dir, err := ioutil.TempDir("", "TestBucketMetadata_Fail_InvalidFileFormat")
	s.NoError(err)
	defer os.RemoveAll(dir)
	client := s.constructClient(dir)
	s.NoError(writeFile(bucketItemPath(dir, defaultBucketName, metadataFilename), []byte("invalid")))

	metadata, err := client.BucketMetadata(context.Background(), defaultBucketName)
	s.Equal(ErrBucketConfigDeserialization, err)
	s.Nil(metadata)
}

func (s *ClientSuite) TestBucketMetadata_Success() {
	dir, err := ioutil.TempDir("", "TestBucketMetadata_Success")
	s.NoError(err)
	defer os.RemoveAll(dir)
	client := s.constructClient(dir)

	metadata, err := client.BucketMetadata(context.Background(), defaultBucketName)
	s.NoError(err)
	s.NotNil(metadata)
	s.Equal(defaultBucketRetentionDays, metadata.RetentionDays)
	s.Equal(defaultBucketOwner, metadata.Owner)
}

func (s *ClientSuite) constructClient(storeDir string) blobstore.Client {
	cfg := s.constructConfig(storeDir)
	client, err := NewClient(cfg, bark.NewNopLogger())
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

func (s *ClientSuite) assertBlobEquals(expectedTags map[string]string, expectedBody string, actual *blob.Blob) {
	s.Equal(expectedTags, actual.Tags)
	s.Equal(expectedBody, string(actual.Body))
}
