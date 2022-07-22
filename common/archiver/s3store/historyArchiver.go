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

// S3 History Archiver will archive workflow histories to amazon s3

package s3store

import (
	"context"
	"encoding/binary"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"go.temporal.io/api/serviceerror"

	archiverspb "go.temporal.io/server/api/archiver/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/codec"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
)

const (
	// URIScheme is the scheme for the s3 implementation
	URIScheme               = "s3"
	errEncodeHistory        = "failed to encode history batches"
	errWriteKey             = "failed to write history to s3"
	defaultBlobstoreTimeout = time.Minute
	targetHistoryBlobSize   = 2 * 1024 * 1024 // 2MB
)

var (
	errNoBucketSpecified = errors.New("no bucket specified")
	errBucketNotExists   = errors.New("requested bucket does not exist")
	errEmptyAwsRegion    = errors.New("empty aws region")
)

type (
	historyArchiver struct {
		container *archiver.HistoryBootstrapContainer
		s3cli     s3iface.S3API
		// only set in test code
		historyIterator archiver.HistoryIterator
	}

	getHistoryToken struct {
		CloseFailoverVersion int64
		BatchIdx             int
	}

	uploadProgress struct {
		BatchIdx      int
		IteratorState []byte
		uploadedSize  int64
		historySize   int64
	}
)

// NewHistoryArchiver creates a new archiver.HistoryArchiver based on s3
func NewHistoryArchiver(
	container *archiver.HistoryBootstrapContainer,
	config *config.S3Archiver,
) (archiver.HistoryArchiver, error) {
	return newHistoryArchiver(container, config, nil)
}

func newHistoryArchiver(
	container *archiver.HistoryBootstrapContainer,
	config *config.S3Archiver,
	historyIterator archiver.HistoryIterator,
) (*historyArchiver, error) {
	if len(config.Region) == 0 {
		return nil, errEmptyAwsRegion
	}
	s3Config := &aws.Config{
		Endpoint:         config.Endpoint,
		Region:           aws.String(config.Region),
		S3ForcePathStyle: aws.Bool(config.S3ForcePathStyle),
	}
	sess, err := session.NewSession(s3Config)
	if err != nil {
		return nil, err
	}

	return &historyArchiver{
		container:       container,
		s3cli:           s3.New(sess),
		historyIterator: historyIterator,
	}, nil
}
func (h *historyArchiver) Archive(
	ctx context.Context,
	URI archiver.URI,
	request *archiver.ArchiveHistoryRequest,
	opts ...archiver.ArchiveOption,
) (err error) {
	scope := h.container.MetricsClient.Scope(metrics.HistoryArchiverScope, metrics.NamespaceTag(request.Namespace))
	featureCatalog := archiver.GetFeatureCatalog(opts...)
	sw := scope.StartTimer(metrics.ServiceLatency)
	defer func() {
		sw.Stop()
		if err != nil {
			if common.IsPersistenceTransientError(err) || isRetryableError(err) {
				scope.IncCounter(metrics.HistoryArchiverArchiveTransientErrorCount)
			} else {
				scope.IncCounter(metrics.HistoryArchiverArchiveNonRetryableErrorCount)
				if featureCatalog.NonRetryableError != nil {
					err = featureCatalog.NonRetryableError()
				}
			}
		}
	}()

	logger := archiver.TagLoggerWithArchiveHistoryRequestAndURI(h.container.Logger, request, URI.String())

	if err := softValidateURI(URI); err != nil {
		logger.Error(archiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason(archiver.ErrReasonInvalidURI), tag.Error(err))
		return err
	}

	if err := archiver.ValidateHistoryArchiveRequest(request); err != nil {
		logger.Error(archiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason(archiver.ErrReasonInvalidArchiveRequest), tag.Error(err))
		return err
	}

	var progress uploadProgress
	historyIterator := h.historyIterator
	if historyIterator == nil { // will only be set by testing code
		historyIterator = loadHistoryIterator(ctx, request, h.container.ExecutionManager, featureCatalog, &progress)
	}
	for historyIterator.HasNext() {
		historyBlob, err := getNextHistoryBlob(ctx, historyIterator)
		if err != nil {
			if _, isNotFound := err.(*serviceerror.NotFound); isNotFound {
				// workflow history no longer exists, may due to duplicated archival signal
				// this may happen even in the middle of iterating history as two archival signals
				// can be processed concurrently.
				logger.Info(archiver.ArchiveSkippedInfoMsg)
				scope.IncCounter(metrics.HistoryArchiverDuplicateArchivalsCount)
				return nil
			}

			logger := log.With(logger, tag.ArchivalArchiveFailReason(archiver.ErrReasonReadHistory), tag.Error(err))
			if common.IsPersistenceTransientError(err) {
				logger.Error(archiver.ArchiveTransientErrorMsg)
			} else {
				logger.Error(archiver.ArchiveNonRetryableErrorMsg)
			}
			return err
		}

		if historyMutated(request, historyBlob.Body, historyBlob.Header.IsLast) {
			logger.Error(archiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason(archiver.ErrReasonHistoryMutated))
			return archiver.ErrHistoryMutated
		}

		encoder := codec.NewJSONPBEncoder()
		encodedHistoryBlob, err := encoder.Encode(historyBlob)
		if err != nil {
			logger.Error(archiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason(errEncodeHistory), tag.Error(err))
			return err
		}
		key := constructHistoryKey(URI.Path(), request.NamespaceID, request.WorkflowID, request.RunID, request.CloseFailoverVersion, progress.BatchIdx)

		exists, err := keyExists(ctx, h.s3cli, URI, key)
		if err != nil {
			if isRetryableError(err) {
				logger.Error(archiver.ArchiveTransientErrorMsg, tag.ArchivalArchiveFailReason(errWriteKey), tag.Error(err))
			} else {
				logger.Error(archiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason(errWriteKey), tag.Error(err))
			}
			return err
		}
		blobSize := int64(binary.Size(encodedHistoryBlob))
		if exists {
			scope.IncCounter(metrics.HistoryArchiverBlobExistsCount)
		} else {
			if err := upload(ctx, h.s3cli, URI, key, encodedHistoryBlob); err != nil {
				if isRetryableError(err) {
					logger.Error(archiver.ArchiveTransientErrorMsg, tag.ArchivalArchiveFailReason(errWriteKey), tag.Error(err))
				} else {
					logger.Error(archiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason(errWriteKey), tag.Error(err))
				}
				return err
			}
			progress.uploadedSize += blobSize
			scope.RecordDistribution(metrics.HistoryArchiverBlobSize, int(blobSize))
		}

		progress.historySize += blobSize
		progress.BatchIdx = progress.BatchIdx + 1
		saveHistoryIteratorState(ctx, featureCatalog, historyIterator, &progress)
	}

	scope.RecordDistribution(metrics.HistoryArchiverTotalUploadSize, int(progress.uploadedSize))
	scope.RecordDistribution(metrics.HistoryArchiverHistorySize, int(progress.historySize))
	scope.IncCounter(metrics.HistoryArchiverArchiveSuccessCount)
	return nil
}

func loadHistoryIterator(ctx context.Context, request *archiver.ArchiveHistoryRequest, executionManager persistence.ExecutionManager, featureCatalog *archiver.ArchiveFeatureCatalog, progress *uploadProgress) (historyIterator archiver.HistoryIterator) {
	if featureCatalog.ProgressManager != nil {
		if featureCatalog.ProgressManager.HasProgress(ctx) {
			err := featureCatalog.ProgressManager.LoadProgress(ctx, progress)
			if err == nil {
				historyIterator, err := archiver.NewHistoryIteratorFromState(request, executionManager, targetHistoryBlobSize, progress.IteratorState)
				if err == nil {
					return historyIterator
				}
			}
			progress.IteratorState = nil
			progress.BatchIdx = 0
			progress.historySize = 0
			progress.uploadedSize = 0
		}
	}
	return archiver.NewHistoryIterator(request, executionManager, targetHistoryBlobSize)
}

func saveHistoryIteratorState(ctx context.Context, featureCatalog *archiver.ArchiveFeatureCatalog, historyIterator archiver.HistoryIterator, progress *uploadProgress) {
	// Saving history state is a best effort operation. Ignore errors and continue
	if featureCatalog.ProgressManager != nil {
		state, err := historyIterator.GetState()
		if err != nil {
			return
		}
		progress.IteratorState = state
		err = featureCatalog.ProgressManager.RecordProgress(ctx, progress)
		if err != nil {
			return
		}
	}
}

func (h *historyArchiver) Get(
	ctx context.Context,
	URI archiver.URI,
	request *archiver.GetHistoryRequest,
) (*archiver.GetHistoryResponse, error) {
	if err := softValidateURI(URI); err != nil {
		return nil, serviceerror.NewInvalidArgument(archiver.ErrInvalidURI.Error())
	}

	if err := archiver.ValidateGetRequest(request); err != nil {
		return nil, serviceerror.NewInvalidArgument(archiver.ErrInvalidGetHistoryRequest.Error())
	}

	var err error
	var token *getHistoryToken
	if request.NextPageToken != nil {
		token, err = deserializeGetHistoryToken(request.NextPageToken)
		if err != nil {
			return nil, serviceerror.NewInvalidArgument(archiver.ErrNextPageTokenCorrupted.Error())
		}
	} else if request.CloseFailoverVersion != nil {
		token = &getHistoryToken{
			CloseFailoverVersion: *request.CloseFailoverVersion,
		}
	} else {
		highestVersion, err := h.getHighestVersion(ctx, URI, request)
		if err != nil {
			return nil, serviceerror.NewInvalidArgument(err.Error())
		}
		token = &getHistoryToken{
			CloseFailoverVersion: *highestVersion,
		}
	}
	encoder := codec.NewJSONPBEncoder()
	response := &archiver.GetHistoryResponse{}
	numOfEvents := 0
	isTruncated := false
	for {
		if numOfEvents >= request.PageSize {
			isTruncated = true
			break
		}
		key := constructHistoryKey(URI.Path(), request.NamespaceID, request.WorkflowID, request.RunID, token.CloseFailoverVersion, token.BatchIdx)

		encodedRecord, err := download(ctx, h.s3cli, URI, key)
		if err != nil {
			if isRetryableError(err) {
				return nil, serviceerror.NewUnavailable(err.Error())
			}
			switch err.(type) {
			case *serviceerror.InvalidArgument, *serviceerror.Unavailable, *serviceerror.NotFound:
				return nil, err
			default:
				return nil, serviceerror.NewInternal(err.Error())
			}
		}

		historyBlob := archiverspb.HistoryBlob{}
		err = encoder.Decode(encodedRecord, &historyBlob)
		if err != nil {
			return nil, serviceerror.NewInternal(err.Error())
		}

		for _, batch := range historyBlob.Body {
			response.HistoryBatches = append(response.HistoryBatches, batch)
			numOfEvents += len(batch.Events)
		}

		if historyBlob.Header.IsLast {
			break
		}
		token.BatchIdx++
	}

	if isTruncated {
		nextToken, err := serializeToken(token)
		if err != nil {
			return nil, serviceerror.NewInternal(err.Error())
		}
		response.NextPageToken = nextToken
	}

	return response, nil
}

func (h *historyArchiver) ValidateURI(URI archiver.URI) error {
	err := softValidateURI(URI)
	if err != nil {
		return err
	}
	return bucketExists(context.TODO(), h.s3cli, URI)
}

func getNextHistoryBlob(ctx context.Context, historyIterator archiver.HistoryIterator) (*archiverspb.HistoryBlob, error) {
	historyBlob, err := historyIterator.Next()
	op := func(_ context.Context) error {
		historyBlob, err = historyIterator.Next()
		return err
	}
	for err != nil {
		if !common.IsPersistenceTransientError(err) {
			return nil, err
		}
		if contextExpired(ctx) {
			return nil, archiver.ErrContextTimeout
		}
		err = backoff.ThrottleRetryContext(ctx, op, common.CreatePersistenceRetryPolicy(), common.IsPersistenceTransientError)
	}
	return historyBlob, nil
}

func (h *historyArchiver) getHighestVersion(ctx context.Context, URI archiver.URI, request *archiver.GetHistoryRequest) (*int64, error) {
	ctx, cancel := ensureContextTimeout(ctx)
	defer cancel()
	var prefix = constructHistoryKeyPrefix(URI.Path(), request.NamespaceID, request.WorkflowID, request.RunID) + "/"
	results, err := h.s3cli.ListObjectsV2WithContext(ctx, &s3.ListObjectsV2Input{
		Bucket:    aws.String(URI.Hostname()),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == s3.ErrCodeNoSuchBucket {
			return nil, serviceerror.NewInvalidArgument(errBucketNotExists.Error())
		}
		return nil, err
	}
	var highestVersion *int64

	for _, v := range results.CommonPrefixes {
		var version int64
		version, err = strconv.ParseInt(strings.Replace(strings.Replace(*v.Prefix, prefix, "", 1), "/", "", 1), 10, 64)
		if err != nil {
			continue
		}
		if highestVersion == nil || version > *highestVersion {
			highestVersion = &version
		}
	}
	if highestVersion == nil {
		return nil, archiver.ErrHistoryNotExist
	}
	return highestVersion, nil
}

func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	if aerr, ok := err.(awserr.Error); ok {
		return isStatusCodeRetryable(aerr) || request.IsErrorRetryable(aerr) || request.IsErrorThrottle(aerr)
	}
	return false
}

func isStatusCodeRetryable(err error) bool {
	if aerr, ok := err.(awserr.Error); ok {
		if rerr, ok := err.(awserr.RequestFailure); ok {
			if rerr.StatusCode() == 429 {
				return true
			}
			if rerr.StatusCode() >= 500 && rerr.StatusCode() != 501 {
				return true
			}
		}
		return isStatusCodeRetryable(aerr.OrigErr())
	}
	return false
}
