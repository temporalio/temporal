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
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/blobstore"
	"github.com/uber/cadence/common/blobstore/blob"
	"go.uber.org/multierr"
)

const (
	defaultBlobstoreTimeout = 10 * time.Second
)

type client struct {
	s3cli s3iface.S3API
}

// NewClient returns a new Client backed by S3
func NewClient(s3cli s3iface.S3API) blobstore.Client {
	return &client{
		s3cli: s3cli,
	}
}
func ClientFromConfig(cfg *Config) (s3iface.S3API, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	s3Config := &aws.Config{
		Endpoint:         cfg.Endpoint,
		Region:           aws.String(cfg.Region),
		S3ForcePathStyle: aws.Bool(cfg.S3ForcePathStyle),
		MaxRetries:       aws.Int(0),
	}
	sess, err := session.NewSession(s3Config)
	if err != nil {
		return nil, err
	}
	return s3.New(sess), nil
}

func (c *client) Upload(ctx context.Context, bucket string, key blob.Key, blob *blob.Blob) error {
	ctx, cancel := c.ensureContextTimeout(ctx)
	defer cancel()
	params := url.Values{}
	for k, v := range blob.Tags {
		params.Add(k, v)
	}

	_, err := c.s3cli.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket:  aws.String(bucket),
		Key:     aws.String(key.String()),
		Body:    bytes.NewReader(blob.Body),
		Tagging: aws.String(params.Encode()),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == s3.ErrCodeNoSuchBucket {
				return blobstore.ErrBucketNotExists
			}
		}
		return err
	}
	return nil
}

func (c *client) Download(ctx context.Context, bucket string, key blob.Key) (*blob.Blob, error) {
	ctx, cancel := c.ensureContextTimeout(ctx)
	defer cancel()
	result, err := c.s3cli.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key.String()),
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == s3.ErrCodeNoSuchBucket {
				return nil, blobstore.ErrBucketNotExists
			}

			if aerr.Code() == s3.ErrCodeNoSuchKey {
				return nil, blobstore.ErrBlobNotExists
			}
		}
		return nil, err
	}

	defer func() {
		if ierr := result.Body.Close(); ierr != nil {
			err = multierr.Append(err, ierr)
		}
	}()

	body, err := ioutil.ReadAll(result.Body)
	if err != nil {
		return nil, err
	}

	tags, err := getTags(ctx, c.s3cli, bucket, key.String())
	if err != nil {
		return nil, err
	}
	return blob.NewBlob(body, tags), nil
}

func (c *client) GetTags(ctx context.Context, bucket string, key blob.Key) (map[string]string, error) {
	ctx, cancel := c.ensureContextTimeout(ctx)
	defer cancel()
	tags, err := getTags(ctx, c.s3cli, bucket, key.String())
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == s3.ErrCodeNoSuchBucket {
				return nil, blobstore.ErrBucketNotExists
			}

			if aerr.Code() == s3.ErrCodeNoSuchKey {
				return nil, blobstore.ErrBlobNotExists
			}
		}
		return nil, err
	}
	return tags, nil
}

// Exists returns false when the bucket does not exist or the bucket and key does not exist
func (c *client) Exists(ctx context.Context, bucket string, key blob.Key) (bool, error) {
	ctx, cancel := c.ensureContextTimeout(ctx)
	defer cancel()
	_, err := c.s3cli.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key.String()),
	})
	if err != nil {
		if isNotFoundError(err) {
			_, err = c.s3cli.HeadBucketWithContext(ctx, &s3.HeadBucketInput{
				Bucket: aws.String(bucket),
			})
			if err != nil {
				if isNotFoundError(err) {
					return false, blobstore.ErrBucketNotExists
				}
				return false, err
			}
			return false, nil
		}

		return false, err
	}
	return true, nil
}

// Delete will always return true whether or not the specific key exists
// S3 DeleteObject gives no indication if the key exists
func (c *client) Delete(ctx context.Context, bucket string, key blob.Key) (bool, error) {
	ctx, cancel := c.ensureContextTimeout(ctx)
	defer cancel()
	_, err := c.s3cli.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key.String()),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == s3.ErrCodeNoSuchBucket {
				return false, blobstore.ErrBucketNotExists
			}
		}
		return false, err

	}
	return true, nil
}

func (c *client) ListByPrefix(ctx context.Context, bucket string, prefix string) ([]blob.Key, error) {
	ctx, cancel := c.ensureContextTimeout(ctx)
	defer cancel()
	results, err := c.s3cli.ListObjectsV2WithContext(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == s3.ErrCodeNoSuchBucket {
			return nil, blobstore.ErrBucketNotExists
		}
		return nil, err
	}
	var keys = make([]blob.Key, len(results.Contents), len(results.Contents))
	for i, v := range results.Contents {
		key, err := blob.NewKeyFromString(*v.Key)
		if err != nil {
			return nil, blobstore.ErrConstructKey
		}
		keys[i] = key
	}
	return keys, nil
}

func (c *client) BucketMetadata(ctx context.Context, bucket string) (*blobstore.BucketMetadataResponse, error) {
	ctx, cancel := c.ensureContextTimeout(ctx)
	defer cancel()
	results, err := c.s3cli.GetBucketAclWithContext(ctx, &s3.GetBucketAclInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == s3.ErrCodeNoSuchBucket {
			return nil, blobstore.ErrBucketNotExists
		}
		return nil, err
	}

	lifecycleResults, err := c.s3cli.GetBucketLifecycleConfigurationWithContext(ctx, &s3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == s3.ErrCodeNoSuchBucket {
			return nil, blobstore.ErrBucketNotExists
		}
		return nil, err
	}
	retentionDays := 0
	for _, v := range lifecycleResults.Rules {
		if *v.Status == s3.ExpirationStatusEnabled && retentionDays < int(*v.Expiration.Days) {
			retentionDays = int(*v.Expiration.Days)
		}
	}
	owner := ""
	if results.Owner != nil {
		if results.Owner.DisplayName != nil {
			owner = *results.Owner.DisplayName
		} else if results.Owner.ID != nil {
			owner = *results.Owner.ID
		}
	}
	return &blobstore.BucketMetadataResponse{
		Owner:         owner,
		RetentionDays: retentionDays,
	}, nil
}

func (c *client) BucketExists(ctx context.Context, bucket string) (bool, error) {
	ctx, cancel := c.ensureContextTimeout(ctx)
	defer cancel()
	_, err := c.s3cli.HeadBucketWithContext(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})

	if err != nil {
		if isNotFoundError(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (c *client) IsRetryableError(err error) bool {
	return c.isStatusCodeRetryable(err) || request.IsErrorRetryable(err) || request.IsErrorThrottle(err)
}

func (c *client) GetRetryPolicy() backoff.RetryPolicy {
	policy := backoff.NewExponentialRetryPolicy(30)
	policy.SetMaximumAttempts(3)
	return policy
}

func (c *client) ensureContextTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, defaultBlobstoreTimeout)
}

func (c *client) isStatusCodeRetryable(err error) bool {
	if err == nil {
		return false
	}
	if aerr, ok := err.(awserr.Error); ok {
		if rerr, ok := err.(awserr.RequestFailure); ok {
			if rerr.StatusCode() == 429 {
				return true
			}
			if rerr.StatusCode() >= 500 && rerr.StatusCode() != 501 {
				return true
			}
		}
		return c.isStatusCodeRetryable(aerr.OrigErr())
	}
	return false
}

func getTags(ctx context.Context, s3api s3iface.S3API, bucket string, key string) (map[string]string, error) {
	result, err := s3api.GetObjectTaggingWithContext(ctx, &s3.GetObjectTaggingInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}

	tags := make(map[string]string)
	for _, e := range result.TagSet {
		if len(*e.Key) > 0 {
			tags[*e.Key] = *e.Value
		}
	}
	return tags, err
}

func isNotFoundError(err error) bool {
	aerr, ok := err.(awserr.Error)
	return ok && (aerr.Code() == "NotFound")
}
