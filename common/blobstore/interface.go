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
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/blobstore/blob"
)

var (
	// ErrBucketNotExists indicates that requested bucket does not exist
	ErrBucketNotExists = &shared.EntityNotExistsError{Message: "requested bucket does not exist"}
	// ErrBlobNotExists indicates that requested blob does not exist
	ErrBlobNotExists = &shared.EntityNotExistsError{Message: "requested blob does not exist"}
	// ErrBlobSerialization indicates that a failure occurred in serializing blob
	ErrBlobSerialization = &shared.BadRequestError{Message: "failed to serialize blob"}
	// ErrBlobDeserialization indicates that a failure occurred in deserializing blob
	ErrBlobDeserialization = &shared.BadRequestError{Message: "failed to deserialize blob"}
)

// BucketMetadataResponse contains information relating to a bucket's configuration
type BucketMetadataResponse struct {
	Owner         string
	RetentionDays int
}

// Client is used to operate on blobs in a blobstore
type Client interface {
	Upload(ctx context.Context, bucket string, key blob.Key, blob *blob.Blob) error
	Download(ctx context.Context, bucket string, key blob.Key) (*blob.Blob, error)
	Exists(ctx context.Context, bucket string, key blob.Key) (bool, error)
	Delete(ctx context.Context, bucket string, key blob.Key) (bool, error)
	ListByPrefix(ctx context.Context, bucket string, prefix string) ([]blob.Key, error)
	BucketMetadata(ctx context.Context, bucket string) (*BucketMetadataResponse, error)
}
