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

package blobstore

import (
	"context"
	"errors"
	"io"
)

// CompressionType defines the type of compression used for a blob
type CompressionType int

const (
	// NoCompression indicates that blob is not compressed
	NoCompression CompressionType = iota
)

// Blob defines a blob
type Blob struct {
	Body            io.Reader
	CompressionType CompressionType
	Tags            map[string]string
}

// BucketMetadataResponse contains information relating to a bucket's configuration
type BucketMetadataResponse struct {
	Owner         string
	RetentionDays int
}

// Client is used to operate on blobs in a blobstore
type Client interface {
	UploadBlob(ctx context.Context, bucket string, path string, blob *Blob) error
	DownloadBlob(ctx context.Context, bucket string, path string) (*Blob, error)
	BucketMetadata(ctx context.Context, bucket string) (*BucketMetadataResponse, error)
}

type nopClient struct{}

func (c *nopClient) UploadBlob(ctx context.Context, bucket string, path string, blob *Blob) error {
	return errors.New("not implemented")
}

func (c *nopClient) DownloadBlob(ctx context.Context, bucket string, path string) (*Blob, error) {
	return nil, errors.New("not implemented")
}

func (c *nopClient) BucketMetadata(ctx context.Context, bucket string) (*BucketMetadataResponse, error) {
	return nil, errors.New("not implemented")
}

// NewNopClient creates a nop client
func NewNopClient() Client {
	return &nopClient{}
}
