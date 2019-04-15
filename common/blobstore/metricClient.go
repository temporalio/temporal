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
	"github.com/uber/cadence/common/metrics"
)

var _ Client = (*metricClient)(nil)

type metricClient struct {
	client        Client
	metricsClient metrics.Client
}

// NewMetricClient creates a new instance of Client that emits metrics
func NewMetricClient(client Client, metricsClient metrics.Client) Client {
	return &metricClient{
		client:        client,
		metricsClient: metricsClient,
	}
}

func (c *metricClient) Upload(ctx context.Context, bucket string, key blob.Key, blob *blob.Blob) error {
	c.metricsClient.IncCounter(metrics.BlobstoreClientUploadScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.BlobstoreClientUploadScope, metrics.CadenceClientLatency)
	err := c.client.Upload(ctx, bucket, key, blob)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.BlobstoreClientUploadScope, metrics.CadenceClientFailures)
	}
	return err
}

func (c *metricClient) Download(ctx context.Context, bucket string, key blob.Key) (*blob.Blob, error) {
	c.metricsClient.IncCounter(metrics.BlobstoreClientDownloadScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.BlobstoreClientDownloadScope, metrics.CadenceClientLatency)
	resp, err := c.client.Download(ctx, bucket, key)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.BlobstoreClientDownloadScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) GetTags(ctx context.Context, bucket string, key blob.Key) (map[string]string, error) {
	c.metricsClient.IncCounter(metrics.BlobstoreClientGetTagsScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.BlobstoreClientGetTagsScope, metrics.CadenceClientLatency)
	resp, err := c.client.GetTags(ctx, bucket, key)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.BlobstoreClientGetTagsScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) Exists(ctx context.Context, bucket string, key blob.Key) (bool, error) {
	c.metricsClient.IncCounter(metrics.BlobstoreClientExistsScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.BlobstoreClientExistsScope, metrics.CadenceClientLatency)
	resp, err := c.client.Exists(ctx, bucket, key)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.BlobstoreClientExistsScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) Delete(ctx context.Context, bucket string, key blob.Key) (bool, error) {
	c.metricsClient.IncCounter(metrics.BlobstoreClientDeleteScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.BlobstoreClientDeleteScope, metrics.CadenceClientLatency)
	resp, err := c.client.Delete(ctx, bucket, key)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.BlobstoreClientDeleteScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) ListByPrefix(ctx context.Context, bucket string, prefix string) ([]blob.Key, error) {
	c.metricsClient.IncCounter(metrics.BlobstoreClientListByPrefixScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.BlobstoreClientListByPrefixScope, metrics.CadenceClientLatency)
	resp, err := c.client.ListByPrefix(ctx, bucket, prefix)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.BlobstoreClientListByPrefixScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) BucketMetadata(ctx context.Context, bucket string) (*BucketMetadataResponse, error) {
	c.metricsClient.IncCounter(metrics.BlobstoreClientBucketMetadataScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.BlobstoreClientBucketMetadataScope, metrics.CadenceClientLatency)
	resp, err := c.client.BucketMetadata(ctx, bucket)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.BlobstoreClientBucketMetadataScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) BucketExists(ctx context.Context, bucket string) (bool, error) {
	c.metricsClient.IncCounter(metrics.BlobstoreClientBucketExistsScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.BlobstoreClientBucketExistsScope, metrics.CadenceClientLatency)
	resp, err := c.client.BucketExists(ctx, bucket)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.BlobstoreClientBucketExistsScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) IsRetryableError(err error) bool {
	return c.client.IsRetryableError(err)
}

func (c *metricClient) GetRetryPolicy() backoff.RetryPolicy {
	return c.client.GetRetryPolicy()
}
