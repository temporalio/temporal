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
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/blobstore/blob"
)

var _ Client = (*retryableClient)(nil)

type retryableClient struct {
	client      Client
	policy      backoff.RetryPolicy
	isRetryable backoff.IsRetryable
}

// NewRetryableClient creates a new instance of Client with retry policy
func NewRetryableClient(client Client, policy backoff.RetryPolicy, isRetryable backoff.IsRetryable) Client {
	return &retryableClient{
		client:      client,
		policy:      policy,
		isRetryable: isRetryable,
	}
}

func (c *retryableClient) Upload(ctx context.Context, bucket string, key blob.Key, blob *blob.Blob) error {
	op := func() error {
		return c.client.Upload(ctx, bucket, key, blob)
	}
	return backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) Download(ctx context.Context, bucket string, key blob.Key) (*blob.Blob, error) {
	var resp *blob.Blob
	op := func() error {
		var err error
		resp, err = c.client.Download(ctx, bucket, key)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) Exists(ctx context.Context, bucket string, key blob.Key) (bool, error) {
	var resp bool
	op := func() error {
		var err error
		resp, err = c.client.Exists(ctx, bucket, key)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) Delete(ctx context.Context, bucket string, key blob.Key) (bool, error) {
	var resp bool
	op := func() error {
		var err error
		resp, err = c.client.Delete(ctx, bucket, key)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ListByPrefix(ctx context.Context, bucket string, prefix string) ([]blob.Key, error) {
	var resp []blob.Key
	op := func() error {
		var err error
		resp, err = c.client.ListByPrefix(ctx, bucket, prefix)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) BucketMetadata(ctx context.Context, bucket string) (*BucketMetadataResponse, error) {
	var resp *BucketMetadataResponse
	op := func() error {
		var err error
		resp, err = c.client.BucketMetadata(ctx, bucket)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}
