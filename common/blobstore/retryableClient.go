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

package blobstore

import (
	"context"

	"github.com/uber/cadence/common/backoff"
)

type (
	retryableClient struct {
		client Client
		policy backoff.RetryPolicy
	}
)

// NewRetryableClient constructs a blobstorre client which retries transient errors.
func NewRetryableClient(client Client, policy backoff.RetryPolicy) Client {
	return &retryableClient{
		client: client,
		policy: policy,
	}
}

func (c *retryableClient) Put(ctx context.Context, req *PutRequest) (*PutResponse, error) {
	var resp *PutResponse
	var err error
	op := func() error {
		resp, err = c.client.Put(ctx, req)
		return err
	}
	err = backoff.Retry(op, c.policy, c.client.IsRetryableError)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *retryableClient) Get(ctx context.Context, req *GetRequest) (*GetResponse, error) {
	var resp *GetResponse
	var err error
	op := func() error {
		resp, err = c.client.Get(ctx, req)
		return err
	}
	err = backoff.Retry(op, c.policy, c.client.IsRetryableError)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *retryableClient) Exists(ctx context.Context, req *ExistsRequest) (*ExistsResponse, error) {
	var resp *ExistsResponse
	var err error
	op := func() error {
		resp, err = c.client.Exists(ctx, req)
		return err
	}
	err = backoff.Retry(op, c.policy, c.client.IsRetryableError)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *retryableClient) Delete(ctx context.Context, req *DeleteRequest) (*DeleteResponse, error) {
	var resp *DeleteResponse
	var err error
	op := func() error {
		resp, err = c.client.Delete(ctx, req)
		return err
	}
	err = backoff.Retry(op, c.policy, c.client.IsRetryableError)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *retryableClient) IsRetryableError(err error) bool {
	return c.client.IsRetryableError(err)
}
