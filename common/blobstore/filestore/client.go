// The MIT License (MIT)
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package filestore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/uber/cadence/common/blobstore"
	"github.com/uber/cadence/common/service/config"
	"github.com/uber/cadence/common/util"
)

type (
	client struct {
		outputDirectory string
	}
)

// NewFilestoreClient constructs a blobstore backed by local file system
func NewFilestoreClient(cfg *config.FileBlobstore) (blobstore.Client, error) {
	if cfg == nil {
		return nil, errors.New("file blobstore config is nil")
	}
	if len(cfg.OutputDirectory) == 0 {
		return nil, errors.New("output directory not given for file blobstore")
	}
	outputDirectory := cfg.OutputDirectory
	exists, err := util.DirectoryExists(outputDirectory)
	if err != nil {
		return nil, err
	}
	if !exists {
		if err := util.MkdirAll(outputDirectory, os.FileMode(0766)); err != nil {
			return nil, err
		}
	}
	return &client{
		outputDirectory: outputDirectory,
	}, nil
}

// Put stores a blob
func (c *client) Put(_ context.Context, request *blobstore.PutRequest) (resp *blobstore.PutResponse, err error) {
	defer func() {
		if err != nil {
			os.Remove(c.bodyPath(request.Key))
			os.Remove(c.tagsPath(request.Key))
		}
	}()
	if err := util.WriteFile(c.bodyPath(request.Key), request.Blob.Body, os.FileMode(0666)); err != nil {
		return nil, err
	}
	tagsData, err := json.Marshal(request.Blob.Tags)
	if err != nil {
		return nil, err
	}
	if err := util.WriteFile(c.tagsPath(request.Key), tagsData, os.FileMode(0666)); err != nil {
		return nil, err
	}
	return &blobstore.PutResponse{}, nil
}

// Get fetches a blob
func (c *client) Get(_ context.Context, request *blobstore.GetRequest) (*blobstore.GetResponse, error) {
	data, err := util.ReadFile(c.bodyPath(request.Key))
	if err != nil {
		return nil, err
	}
	tagsData, err := util.ReadFile(c.tagsPath(request.Key))
	if err != nil {
		return nil, err
	}
	tags := make(map[string]string)
	if err := json.Unmarshal(tagsData, &tags); err != nil {
		return nil, err
	}
	return &blobstore.GetResponse{
		Blob: blobstore.Blob{
			Body: data,
			Tags: tags,
		},
	}, nil
}

// Exists determines if a blob exists
func (c *client) Exists(_ context.Context, request *blobstore.ExistsRequest) (*blobstore.ExistsResponse, error) {
	exists, err := util.FileExists(c.bodyPath(request.Key))
	if err != nil {
		return nil, err
	}
	return &blobstore.ExistsResponse{
		Exists: exists,
	}, nil
}

// Delete deletes a blob
func (c *client) Delete(_ context.Context, request *blobstore.DeleteRequest) (*blobstore.DeleteResponse, error) {
	if err := os.Remove(c.bodyPath(request.Key)); err != nil {
		return nil, err
	}
	if err := os.Remove(c.tagsPath(request.Key)); err != nil {
		return nil, err
	}
	return &blobstore.DeleteResponse{}, nil
}

// IsRetryableError returns true if the error is retryable false otherwise
func (c *client) IsRetryableError(err error) bool {
	return false
}

func (c *client) bodyPath(key string) string {
	return fmt.Sprintf("%v/%v", c.outputDirectory, key)
}

func (c *client) tagsPath(key string) string {
	return fmt.Sprintf("%v/.%v.tags", c.outputDirectory, key)
}
