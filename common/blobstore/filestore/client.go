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
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/blobstore"
	"github.com/uber/cadence/common/blobstore/blob"
)

const (
	metadataFilename = "metadata"
)

var (
	// ErrCheckBucketExists could not verify that bucket directory exists
	ErrCheckBucketExists = &shared.BadRequestError{Message: "could not verify that bucket directory exists"}
	// ErrWriteFile could not write file
	ErrWriteFile = &shared.BadRequestError{Message: "could not write file"}
	// ErrReadFile could not read file
	ErrReadFile = &shared.BadRequestError{Message: "could not read file"}
	// ErrCheckFileExists could not check if file exists
	ErrCheckFileExists = &shared.BadRequestError{Message: "could not check if file exists"}
	// ErrDeleteFile could not delete file
	ErrDeleteFile = &shared.BadRequestError{Message: "could not delete file"}
	// ErrListFiles could not list files
	ErrListFiles = &shared.BadRequestError{Message: "could not list files"}
	// ErrConstructKey could not construct key
	ErrConstructKey = &shared.BadRequestError{Message: "could not construct key"}
	// ErrBucketConfigDeserialization bucket config could not be deserialized
	ErrBucketConfigDeserialization = &shared.BadRequestError{Message: "bucket config could not be deserialized"}
)

type client struct {
	sync.Mutex
	storeDirectory string
}

// NewClient returns a new Client backed by file system
func NewClient(cfg *Config) (blobstore.Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := setupDirectories(cfg); err != nil {
		return nil, err
	}
	if err := writeMetadataFiles(cfg); err != nil {
		return nil, err
	}
	return &client{
		storeDirectory: cfg.StoreDirectory,
	}, nil
}

func (c *client) Upload(_ context.Context, bucket string, key blob.Key, blob *blob.Blob) error {
	c.Lock()
	defer c.Unlock()

	bd := bucketDirectory(c.storeDirectory, bucket)
	if exists, err := directoryExists(bd); err != nil {
		return ErrCheckBucketExists
	} else if !exists {
		return blobstore.ErrBucketNotExists
	}

	fileBytes, err := serializeBlob(blob)
	if err != nil {
		return blobstore.ErrBlobSerialization
	}
	blobPath := bucketItemPath(c.storeDirectory, bucket, key.String())
	if err := writeFile(blobPath, fileBytes); err != nil {
		return ErrWriteFile
	}
	return nil
}

func (c *client) Download(_ context.Context, bucket string, key blob.Key) (*blob.Blob, error) {
	c.Lock()
	defer c.Unlock()

	bd := bucketDirectory(c.storeDirectory, bucket)
	if exists, err := directoryExists(bd); err != nil {
		return nil, ErrCheckBucketExists
	} else if !exists {
		return nil, blobstore.ErrBucketNotExists
	}

	blobPath := bucketItemPath(c.storeDirectory, bucket, key.String())
	fileBytes, err := readFile(blobPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, blobstore.ErrBlobNotExists
		}
		return nil, ErrReadFile
	}
	blob, err := deserializeBlob(fileBytes)
	if err != nil {
		return nil, blobstore.ErrBlobDeserialization
	}
	return blob, nil
}

func (c *client) GetTags(ctx context.Context, bucket string, key blob.Key) (map[string]string, error) {
	blob, err := c.Download(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	return blob.Tags, nil
}

func (c *client) Exists(_ context.Context, bucket string, key blob.Key) (bool, error) {
	c.Lock()
	defer c.Unlock()

	bd := bucketDirectory(c.storeDirectory, bucket)
	if exists, err := directoryExists(bd); err != nil {
		return false, ErrCheckBucketExists
	} else if !exists {
		return false, blobstore.ErrBucketNotExists
	}

	blobPath := bucketItemPath(c.storeDirectory, bucket, key.String())
	exists, err := fileExists(blobPath)
	if err != nil {
		return false, ErrCheckFileExists
	}
	return exists, nil
}

func (c *client) Delete(_ context.Context, bucket string, key blob.Key) (bool, error) {
	c.Lock()
	defer c.Unlock()

	bd := bucketDirectory(c.storeDirectory, bucket)
	if exists, err := directoryExists(bd); err != nil {
		return false, ErrCheckBucketExists
	} else if !exists {
		return false, blobstore.ErrBucketNotExists
	}

	blobPath := bucketItemPath(c.storeDirectory, bucket, key.String())
	deleted, err := deleteFile(blobPath)
	if err != nil {
		return false, ErrDeleteFile
	}
	return deleted, nil
}

func (c *client) ListByPrefix(_ context.Context, bucket string, prefix string) ([]blob.Key, error) {
	c.Lock()
	defer c.Unlock()

	bd := bucketDirectory(c.storeDirectory, bucket)
	if exists, err := directoryExists(bd); err != nil {
		return nil, ErrCheckBucketExists
	} else if !exists {
		return nil, blobstore.ErrBucketNotExists
	}

	files, err := listFiles(bd)
	if err != nil {
		return nil, ErrListFiles
	}
	var matchingKeys []blob.Key
	for _, f := range files {
		if strings.HasPrefix(f, prefix) {
			key, err := blob.NewKeyFromString(f)
			if err != nil {
				return nil, ErrConstructKey
			}
			matchingKeys = append(matchingKeys, key)
		}
	}
	return matchingKeys, nil
}

func (c *client) BucketMetadata(_ context.Context, bucket string) (*blobstore.BucketMetadataResponse, error) {
	c.Lock()
	defer c.Unlock()

	bd := bucketDirectory(c.storeDirectory, bucket)
	if exists, err := directoryExists(bd); err != nil {
		return nil, ErrCheckBucketExists
	} else if !exists {
		return nil, blobstore.ErrBucketNotExists
	}

	metadataFilepath := bucketItemPath(c.storeDirectory, bucket, metadataFilename)
	data, err := readFile(metadataFilepath)
	if err != nil {
		return nil, ErrReadFile
	}
	bucketCfg, err := deserializeBucketConfig(data)
	if err != nil {
		return nil, ErrBucketConfigDeserialization
	}

	return &blobstore.BucketMetadataResponse{
		Owner:         bucketCfg.Owner,
		RetentionDays: bucketCfg.RetentionDays,
	}, nil
}

func (c *client) BucketExists(_ context.Context, bucket string) (bool, error) {
	c.Lock()
	defer c.Unlock()

	bd := bucketDirectory(c.storeDirectory, bucket)
	exists, err := directoryExists(bd)
	if err != nil {
		return false, ErrCheckBucketExists
	}
	return exists, nil
}

func (c *client) IsRetryableError(err error) bool {
	return false
}

func (c *client) GetRetryPolicy() backoff.RetryPolicy {
	policy := backoff.NewExponentialRetryPolicy(0)
	policy.SetMaximumAttempts(1)
	return policy
}

func setupDirectories(cfg *Config) error {
	if err := mkdirAll(cfg.StoreDirectory); err != nil {
		return err
	}
	if err := mkdirAll(bucketDirectory(cfg.StoreDirectory, cfg.DefaultBucket.Name)); err != nil {
		return err
	}
	for _, b := range cfg.CustomBuckets {
		if err := mkdirAll(bucketDirectory(cfg.StoreDirectory, b.Name)); err != nil {
			return err
		}
	}
	return nil
}

func writeMetadataFiles(cfg *Config) error {
	writeMetadataFile := func(bucketConfig BucketConfig) error {
		path := bucketItemPath(cfg.StoreDirectory, bucketConfig.Name, metadataFilename)
		bytes, err := serializeBucketConfig(&bucketConfig)
		if err != nil {
			return fmt.Errorf("failed to write metadata file for bucket %v: %v", bucketConfig.Name, err)
		}
		if err := writeFile(path, bytes); err != nil {
			return fmt.Errorf("failed to write metadata file for bucket %v: %v", bucketConfig.Name, err)
		}
		return nil
	}

	if err := writeMetadataFile(cfg.DefaultBucket); err != nil {
		return err
	}
	for _, b := range cfg.CustomBuckets {
		if err := writeMetadataFile(b); err != nil {
			return err
		}
	}
	return nil
}

func bucketDirectory(storeDirectory string, bucketName string) string {
	return filepath.Join(storeDirectory, bucketName)
}

func bucketItemPath(storeDirectory string, bucketName string, filename string) string {
	return filepath.Join(bucketDirectory(storeDirectory, bucketName), filename)
}
