// Copyright (c) 2020 Uber Technologies, Inc.
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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"go.temporal.io/temporal-proto/serviceerror"
	"go.uber.org/multierr"

	"github.com/temporalio/temporal/common"

	"github.com/temporalio/temporal/.gen/go/shared"
	"github.com/temporalio/temporal/common/archiver"
)

// encoding & decoding util

func encode(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func decodeHistoryBatches(data []byte) ([]*shared.History, error) {
	historyBatches := []*shared.History{}
	err := json.Unmarshal(data, &historyBatches)
	if err != nil {
		return nil, err
	}
	return historyBatches, nil
}
func decodeVisibilityRecord(data []byte) (*visibilityRecord, error) {
	record := &visibilityRecord{}
	err := json.Unmarshal(data, record)
	if err != nil {
		return nil, err
	}
	return record, nil
}

func serializeToken(token interface{}) ([]byte, error) {
	if token == nil {
		return nil, nil
	}
	return json.Marshal(token)
}

func deserializeGetHistoryToken(bytes []byte) (*getHistoryToken, error) {
	token := &getHistoryToken{}
	err := json.Unmarshal(bytes, token)
	return token, err
}

func deserializeQueryVisibilityToken(bytes []byte) *string {
	var ret = string(bytes)
	return &ret
}
func serializeQueryVisibilityToken(token string) []byte {
	return []byte(token)
}

// Only validates the scheme and buckets are passed
func softValidateURI(URI archiver.URI) error {
	if URI.Scheme() != URIScheme {
		return archiver.ErrURISchemeMismatch
	}
	if len(URI.Hostname()) == 0 {
		return errNoBucketSpecified
	}
	return nil
}

func bucketExists(ctx context.Context, s3cli s3iface.S3API, URI archiver.URI) error {
	ctx, cancel := ensureContextTimeout(ctx)
	defer cancel()
	_, err := s3cli.HeadBucketWithContext(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(URI.Hostname()),
	})
	if err == nil {
		return nil
	}
	if aerr, ok := err.(awserr.Error); ok {
		if aerr.Code() == "NotFound" {
			return errBucketNotExists
		}
	}
	return err
}

// Key construction
func constructHistoryKey(path, domainID, workflowID, runID string, version int64) string {
	prefix := constructHistoryKeyPrefix(path, domainID, workflowID, runID)
	return fmt.Sprintf("%s/%v", prefix, version)
}

func constructHistoryKeyPrefix(path, domainID, workflowID, runID string) string {
	return fmt.Sprintf("%s/%s", constructCommonKeyPrefix(path, domainID, workflowID, "history"), runID)
}

func constructVisibilitySearchPrefix(path, domainID, workflowID, indexType string) string {
	return constructCommonKeyPrefix(path, domainID, workflowID, fmt.Sprintf("visibility/%s", indexType))
}

// Make sure there are no trailing slashes
func constructCommonKeyPrefix(path, domainID, workflowID, entryType string) string {
	return strings.TrimLeft(strings.Join([]string{path, domainID, workflowID, entryType}, "/"), "/")
}

func ensureContextTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, defaultBlobstoreTimeout)
}
func upload(ctx context.Context, s3cli s3iface.S3API, URI archiver.URI, key string, data []byte) error {
	ctx, cancel := ensureContextTimeout(ctx)
	defer cancel()

	_, err := s3cli.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String(URI.Hostname()),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == s3.ErrCodeNoSuchBucket {
				return serviceerror.NewInvalidArgument(errBucketNotExists.Error())
			}
		}
		return err
	}
	return nil
}

func download(ctx context.Context, s3cli s3iface.S3API, URI archiver.URI, key string) ([]byte, error) {
	ctx, cancel := ensureContextTimeout(ctx)
	defer cancel()
	result, err := s3cli.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(URI.Hostname()),
		Key:    aws.String(key),
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == s3.ErrCodeNoSuchBucket {
				return nil, serviceerror.NewInvalidArgument(errBucketNotExists.Error())
			}

			if aerr.Code() == s3.ErrCodeNoSuchKey {
				return nil, serviceerror.NewInvalidArgument(archiver.ErrHistoryNotExist.Error())
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
	return body, nil
}

func historyMutated(request *archiver.ArchiveHistoryRequest, historyBatches []*shared.History, isLast bool) bool {
	lastBatch := historyBatches[len(historyBatches)-1].Events
	lastEvent := lastBatch[len(lastBatch)-1]
	lastFailoverVersion := lastEvent.GetVersion()
	if lastFailoverVersion > request.CloseFailoverVersion {
		return true
	}

	if !isLast {
		return false
	}
	lastEventID := lastEvent.GetEventId()
	return lastFailoverVersion != request.CloseFailoverVersion || lastEventID+1 != request.NextEventID
}

func contextExpired(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func convertToExecutionInfo(record *visibilityRecord) *shared.WorkflowExecutionInfo {
	return &shared.WorkflowExecutionInfo{
		Execution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(record.WorkflowID),
			RunId:      common.StringPtr(record.RunID),
		},
		Type: &shared.WorkflowType{
			Name: common.StringPtr(record.WorkflowTypeName),
		},
		StartTime:     common.Int64Ptr(record.StartTimestamp),
		ExecutionTime: common.Int64Ptr(record.ExecutionTimestamp),
		CloseTime:     common.Int64Ptr(record.CloseTimestamp),
		CloseStatus:   record.CloseStatus.Ptr(),
		HistoryLength: common.Int64Ptr(record.HistoryLength),
		Memo:          record.Memo,
		SearchAttributes: &shared.SearchAttributes{
			IndexedFields: archiver.ConvertSearchAttrToBytes(record.SearchAttributes),
		},
	}
}
