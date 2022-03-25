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

package archiver

import (
	"context"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	archiverspb "go.temporal.io/server/api/archiver/v1"
	"go.temporal.io/server/common"
	carchiver "go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
)

const (
	uploadHistoryActivityFnName     = "uploadHistoryActivity"
	deleteHistoryActivityFnName     = "deleteHistoryActivity"
	archiveVisibilityActivityFnName = "archiveVisibilityActivity"
)

var (
	errUploadNonRetryable            = temporal.NewNonRetryableApplicationError("upload non-retryable error", "", nil)
	errDeleteNonRetryable            = temporal.NewNonRetryableApplicationError("delete non-retryable error", "", nil)
	errArchiveVisibilityNonRetryable = temporal.NewNonRetryableApplicationError("archive visibility non-retryable error", "", nil)
)

func uploadHistoryActivity(ctx context.Context, request ArchiveRequest) (err error) {
	container := ctx.Value(bootstrapContainerKey).(*BootstrapContainer)
	scope := container.MetricsClient.Scope(metrics.ArchiverUploadHistoryActivityScope, metrics.NamespaceTag(request.Namespace))
	sw := scope.StartTimer(metrics.ServiceLatency)
	defer func() {
		sw.Stop()
		if err != nil {
			if err.Error() == errUploadNonRetryable.Error() {
				scope.IncCounter(metrics.ArchiverNonRetryableErrorCount)
			}
			err = temporal.NewNonRetryableApplicationError(err.Error(), "", nil)
		}
	}()
	logger := tagLoggerWithHistoryRequest(tagLoggerWithActivityInfo(container.Logger, activity.GetInfo(ctx)), &request)
	URI, err := carchiver.NewURI(request.HistoryURI)
	if err != nil {
		logger.Error(carchiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason("failed to get history archival uri"), tag.ArchivalURI(request.HistoryURI), tag.Error(err))
		return errUploadNonRetryable
	}
	historyArchiver, err := container.ArchiverProvider.GetHistoryArchiver(URI.Scheme(), common.WorkerServiceName)
	if err != nil {
		logger.Error(carchiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason("failed to get history archiver"), tag.Error(err))
		return errUploadNonRetryable
	}
	err = historyArchiver.Archive(ctx, URI, &carchiver.ArchiveHistoryRequest{
		ShardID:              request.ShardID,
		NamespaceID:          request.NamespaceID,
		Namespace:            request.Namespace,
		WorkflowID:           request.WorkflowID,
		RunID:                request.RunID,
		BranchToken:          request.BranchToken,
		NextEventID:          request.NextEventID,
		CloseFailoverVersion: request.CloseFailoverVersion,
	}, carchiver.GetHeartbeatArchiveOption(), carchiver.GetNonRetryableErrorOption(errUploadNonRetryable))
	if err == nil {
		return nil
	}
	if err.Error() == errUploadNonRetryable.Error() {
		logger.Error(carchiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason("got non-retryable error from history archiver"))
		return errUploadNonRetryable
	}
	logger.Error(carchiver.ArchiveTransientErrorMsg, tag.ArchivalArchiveFailReason("got retryable error from history archiver"), tag.Error(err))
	return err
}

func deleteHistoryActivity(ctx context.Context, request ArchiveRequest) (err error) {
	container := ctx.Value(bootstrapContainerKey).(*BootstrapContainer)
	scope := container.MetricsClient.Scope(metrics.ArchiverDeleteHistoryActivityScope, metrics.NamespaceTag(request.Namespace))
	sw := scope.StartTimer(metrics.ServiceLatency)
	defer func() {
		sw.Stop()
		if err != nil {
			if err.Error() == errDeleteNonRetryable.Error() {
				scope.IncCounter(metrics.ArchiverNonRetryableErrorCount)
			}
			err = temporal.NewNonRetryableApplicationError(err.Error(), "", nil)
		}
	}()
	err = container.HistoryV2Manager.DeleteHistoryBranch(ctx, &persistence.DeleteHistoryBranchRequest{
		BranchToken: request.BranchToken,
		ShardID:     request.ShardID,
	})
	if err == nil {
		return nil
	}
	logger := tagLoggerWithHistoryRequest(tagLoggerWithActivityInfo(container.Logger, activity.GetInfo(ctx)), &request)
	logger.Error("failed to delete history events", tag.Error(err))
	if !common.IsPersistenceTransientError(err) {
		return errDeleteNonRetryable
	}
	return err
}

func archiveVisibilityActivity(ctx context.Context, request ArchiveRequest) (err error) {
	container := ctx.Value(bootstrapContainerKey).(*BootstrapContainer)
	scope := container.MetricsClient.Scope(metrics.ArchiverArchiveVisibilityActivityScope, metrics.NamespaceTag(request.Namespace))
	sw := scope.StartTimer(metrics.ServiceLatency)
	defer func() {
		sw.Stop()
		if err != nil {
			if err.Error() == errArchiveVisibilityNonRetryable.Error() {
				scope.IncCounter(metrics.ArchiverNonRetryableErrorCount)
			}
			err = temporal.NewNonRetryableApplicationError(err.Error(), "", nil)
		}
	}()
	logger := tagLoggerWithVisibilityRequest(tagLoggerWithActivityInfo(container.Logger, activity.GetInfo(ctx)), &request)
	URI, err := carchiver.NewURI(request.VisibilityURI)
	if err != nil {
		logger.Error(carchiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason("failed to get visibility archival uri"), tag.ArchivalURI(request.VisibilityURI), tag.Error(err))
		return errArchiveVisibilityNonRetryable
	}
	visibilityArchiver, err := container.ArchiverProvider.GetVisibilityArchiver(URI.Scheme(), common.WorkerServiceName)
	if err != nil {
		logger.Error(carchiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason("failed to get visibility archiver"), tag.Error(err))
		return errArchiveVisibilityNonRetryable
	}

	// It is safe to pass nil to typeMap here because search attributes type must be embedded by caller.
	searchAttributes, err := searchattribute.Stringify(request.SearchAttributes, nil)
	if err != nil {
		logger.Error("Unable to stringify search attributes.", tag.Error(err))
		return err
	}

	err = visibilityArchiver.Archive(ctx, URI, &archiverspb.VisibilityRecord{
		NamespaceId:        request.NamespaceID,
		Namespace:          request.Namespace,
		WorkflowId:         request.WorkflowID,
		RunId:              request.RunID,
		WorkflowTypeName:   request.WorkflowTypeName,
		StartTime:          timestamp.TimePtr(request.StartTime),
		ExecutionTime:      timestamp.TimePtr(request.ExecutionTime),
		CloseTime:          timestamp.TimePtr(request.CloseTime),
		Status:             request.Status,
		HistoryLength:      request.HistoryLength,
		Memo:               request.Memo,
		SearchAttributes:   searchAttributes,
		HistoryArchivalUri: request.HistoryURI,
	}, carchiver.GetNonRetryableErrorOption(errArchiveVisibilityNonRetryable))
	if err == nil {
		return nil
	}
	if err.Error() == errArchiveVisibilityNonRetryable.Error() {
		logger.Error(carchiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason("got non-retryable error from visibility archiver"))
		return errArchiveVisibilityNonRetryable
	}
	logger.Error(carchiver.ArchiveTransientErrorMsg, tag.ArchivalArchiveFailReason("got retryable error from visibility archiver"), tag.Error(err))
	return err
}
