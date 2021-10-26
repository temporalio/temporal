// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
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
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/gogo/protobuf/proto"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.uber.org/multierr"

	archiverspb "go.temporal.io/server/api/archiver/v1"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/codec"
	"go.temporal.io/server/common/searchattribute"
)

// encoding & decoding util

func encode(message proto.Message) ([]byte, error) {
	encoder := codec.NewJSONPBEncoder()
	return encoder.Encode(message)
}

func decodeVisibilityRecord(data []byte) (*archiverspb.VisibilityRecord, error) {
	record := &archiverspb.VisibilityRecord{}
	encoder := codec.NewJSONPBEncoder()
	err := encoder.Decode(data, record)
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
	if isNotFoundError(err) {
		return errBucketNotExists
	}
	return err
}

func keyExists(ctx context.Context, s3cli s3iface.S3API, URI archiver.URI, key string) (bool, error) {
	ctx, cancel := ensureContextTimeout(ctx)
	defer cancel()
	_, err := s3cli.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(URI.Hostname()),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFoundError(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func isNotFoundError(err error) bool {
	aerr, ok := err.(awserr.Error)
	return ok && (aerr.Code() == "NotFound")
}

// Key construction
func constructHistoryKey(path, namespaceID, workflowID, runID string, version int64, batchIdx int) string {
	prefix := constructHistoryKeyPrefixWithVersion(path, namespaceID, workflowID, runID, version)
	return fmt.Sprintf("%s%d", prefix, batchIdx)
}

func constructHistoryKeyPrefixWithVersion(path, namespaceID, workflowID, runID string, version int64) string {
	prefix := constructHistoryKeyPrefix(path, namespaceID, workflowID, runID)
	return fmt.Sprintf("%s/%v/", prefix, version)
}

func constructHistoryKeyPrefix(path, namespaceID, workflowID, runID string) string {
	return strings.TrimLeft(strings.Join([]string{path, namespaceID, "history", workflowID, runID}, "/"), "/")
}

func constructTimeBasedSearchKey(path, namespaceID, primaryIndexKey, primaryIndexValue, secondaryIndexKey string, t time.Time, precision string) string {
	var timeFormat = ""
	switch precision {
	case PrecisionSecond:
		timeFormat = ":05"
		fallthrough
	case PrecisionMinute:
		timeFormat = ":04" + timeFormat
		fallthrough
	case PrecisionHour:
		timeFormat = "15" + timeFormat
		fallthrough
	case PrecisionDay:
		timeFormat = "2006-01-02T" + timeFormat
	}

	return fmt.Sprintf("%s/%s", constructVisibilitySearchPrefix(path, namespaceID, primaryIndexKey, primaryIndexValue, secondaryIndexKey), t.Format(timeFormat))
}

func constructTimestampIndex(path, namespaceID, primaryIndexKey, primaryIndexValue, secondaryIndexKey string, secondaryIndexValue time.Time, runID string) string {
	return fmt.Sprintf("%s/%s/%s", constructVisibilitySearchPrefix(path, namespaceID, primaryIndexKey, primaryIndexValue, secondaryIndexKey), secondaryIndexValue.Format(time.RFC3339), runID)
}

func constructVisibilitySearchPrefix(path, namespaceID, primaryIndexKey, primaryIndexValue, secondaryIndexType string) string {
	return strings.TrimLeft(strings.Join([]string{path, namespaceID, "visibility", primaryIndexKey, primaryIndexValue, secondaryIndexType}, "/"), "/")
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
				return nil, serviceerror.NewNotFound(archiver.ErrHistoryNotExist.Error())
			}
		}
		return nil, err
	}

	defer func() {
		if ierr := result.Body.Close(); ierr != nil {
			err = multierr.Append(err, ierr)
		}
	}()

	body, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

func historyMutated(request *archiver.ArchiveHistoryRequest, historyBatches []*historypb.History, isLast bool) bool {
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

func convertToExecutionInfo(record *archiverspb.VisibilityRecord, saTypeMap searchattribute.NameTypeMap) (*workflowpb.WorkflowExecutionInfo, error) {
	searchAttributes, err := searchattribute.Parse(record.SearchAttributes, &saTypeMap)
	if err != nil {
		return nil, err
	}

	return &workflowpb.WorkflowExecutionInfo{
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: record.GetWorkflowId(),
			RunId:      record.GetRunId(),
		},
		Type: &commonpb.WorkflowType{
			Name: record.WorkflowTypeName,
		},
		StartTime:        record.StartTime,
		ExecutionTime:    record.ExecutionTime,
		CloseTime:        record.CloseTime,
		Status:           record.Status,
		HistoryLength:    record.HistoryLength,
		Memo:             record.Memo,
		SearchAttributes: searchAttributes,
	}, nil
}
