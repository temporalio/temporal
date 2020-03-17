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
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/gogo/protobuf/types"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/serviceerror"
	"go.uber.org/multierr"

	archiverproto "github.com/temporalio/temporal/.gen/proto/archiver"
	"github.com/temporalio/temporal/common/archiver"
	"github.com/temporalio/temporal/common/codec"
)

// encoding & decoding util

func encode(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func decodeVisibilityRecord(data []byte) (*archiverproto.ArchiveVisibilityRequest, error) {
	record := &archiverproto.ArchiveVisibilityRequest{}
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
func constructHistoryKey(path, domainID, workflowID, runID string, version int64, batchIdx int) string {
	prefix := constructHistoryKeyPrefixWithVersion(path, domainID, workflowID, runID, version)
	return fmt.Sprintf("%s%d", prefix, batchIdx)
}

func constructHistoryKeyPrefixWithVersion(path, domainID, workflowID, runID string, version int64) string {
	prefix := constructHistoryKeyPrefix(path, domainID, workflowID, runID)
	return fmt.Sprintf("%s/%v/", prefix, version)
}

func constructHistoryKeyPrefix(path, domainID, workflowID, runID string) string {
	return strings.TrimLeft(strings.Join([]string{path, domainID, "history", workflowID, runID}, "/"), "/")
}

func constructTimeBasedSearchKey(path, domainID, primaryIndexKey, primaryIndexValue, secondaryIndexKey string, timestamp int64, precision string) string {
	t := time.Unix(0, timestamp).In(time.UTC)
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

	return fmt.Sprintf("%s/%s", constructVisibilitySearchPrefix(path, domainID, primaryIndexKey, primaryIndexValue, secondaryIndexKey), t.Format(timeFormat))
}

func constructTimestampIndex(path, domainID, primaryIndexKey, primaryIndexValue, secondaryIndexKey string, timestamp int64, runID string) string {
	t := time.Unix(0, timestamp).In(time.UTC)
	return fmt.Sprintf("%s/%s/%s", constructVisibilitySearchPrefix(path, domainID, primaryIndexKey, primaryIndexValue, secondaryIndexKey), t.Format(time.RFC3339), runID)
}

func constructVisibilitySearchPrefix(path, domainID, primaryIndexKey, primaryIndexValue, secondaryIndexType string) string {
	return strings.TrimLeft(strings.Join([]string{path, domainID, "visibility", primaryIndexKey, primaryIndexValue, secondaryIndexType}, "/"), "/")
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

	body, err := ioutil.ReadAll(result.Body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

func historyMutated(request *archiver.ArchiveHistoryRequest, historyBatches []*commonproto.History, isLast bool) bool {
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

func convertToExecutionInfo(record *archiverproto.ArchiveVisibilityRequest) *commonproto.WorkflowExecutionInfo {
	return &commonproto.WorkflowExecutionInfo{
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: record.WorkflowID,
			RunId:      record.RunID,
		},
		Type: &commonproto.WorkflowType{
			Name: record.WorkflowTypeName,
		},
		StartTime: &types.Int64Value{
			Value: record.StartTimestamp},
		ExecutionTime: record.ExecutionTimestamp,
		CloseTime: &types.Int64Value{
			Value: record.CloseTimestamp},
		CloseStatus:   record.CloseStatus,
		HistoryLength: record.HistoryLength,
		Memo:          record.Memo,
		SearchAttributes: &commonproto.SearchAttributes{
			IndexedFields: archiver.ConvertSearchAttrToBytes(record.SearchAttributes),
		},
	}
}
