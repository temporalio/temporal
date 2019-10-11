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

package archiver

import (
	"context"
	"errors"

	"github.com/uber/cadence/common"
	carchiver "github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"go.uber.org/cadence"
	"go.uber.org/cadence/activity"
)

const (
	uploadHistoryActivityFnName     = "uploadHistoryActivity"
	deleteHistoryActivityFnName     = "deleteHistoryActivity"
	archiveVisibilityActivityFnName = "archiveVisibilityActivity"
)

var (
	errUploadNonRetriable            = errors.New("upload non-retriable error")
	errDeleteNonRetriable            = errors.New("delete non-retriable error")
	errArchiveVisibilityNonRetriable = errors.New("archive visibility non-retriable error")

	uploadHistoryActivityNonRetryableErrors     = []string{"cadenceInternal:Panic", errUploadNonRetriable.Error()}
	deleteHistoryActivityNonRetryableErrors     = []string{"cadenceInternal:Panic", errDeleteNonRetriable.Error()}
	archiveVisibitliyActivityNonRetryableErrors = []string{"cadenceInternal:Panic", errArchiveVisibilityNonRetriable.Error()}
)

func uploadHistoryActivity(ctx context.Context, request ArchiveRequest) (err error) {
	container := ctx.Value(bootstrapContainerKey).(*BootstrapContainer)
	scope := container.MetricsClient.Scope(metrics.ArchiverUploadHistoryActivityScope, metrics.DomainTag(request.DomainName))
	sw := scope.StartTimer(metrics.CadenceLatency)
	defer func() {
		sw.Stop()
		if err != nil {
			if err.Error() == errUploadNonRetriable.Error() {
				scope.IncCounter(metrics.ArchiverNonRetryableErrorCount)
			}
			err = cadence.NewCustomError(err.Error())
		}
	}()
	logger := tagLoggerWithHistoryRequest(tagLoggerWithActivityInfo(container.Logger, activity.GetInfo(ctx)), &request)
	URI, err := carchiver.NewURI(request.URI)
	if err != nil {
		logger.Error(carchiver.ArchiveNonRetriableErrorMsg, tag.ArchivalArchiveFailReason("failed to get history archival uri"), tag.ArchivalURI(request.URI), tag.Error(err))
		return errUploadNonRetriable
	}
	historyArchiver, err := container.ArchiverProvider.GetHistoryArchiver(URI.Scheme(), common.WorkerServiceName)
	if err != nil {
		logger.Error(carchiver.ArchiveNonRetriableErrorMsg, tag.ArchivalArchiveFailReason("failed to get history archiver"), tag.Error(err))
		return errUploadNonRetriable
	}
	err = historyArchiver.Archive(ctx, URI, &carchiver.ArchiveHistoryRequest{
		ShardID:              request.ShardID,
		DomainID:             request.DomainID,
		DomainName:           request.DomainName,
		WorkflowID:           request.WorkflowID,
		RunID:                request.RunID,
		BranchToken:          request.BranchToken,
		NextEventID:          request.NextEventID,
		CloseFailoverVersion: request.CloseFailoverVersion,
	}, carchiver.GetHeartbeatArchiveOption(), carchiver.GetNonRetriableErrorOption(errUploadNonRetriable))
	if err == nil {
		return nil
	}
	if err.Error() == errUploadNonRetriable.Error() {
		logger.Error(carchiver.ArchiveNonRetriableErrorMsg, tag.ArchivalArchiveFailReason("got non-retryable error from history archiver"))
		return errUploadNonRetriable
	}
	logger.Error(carchiver.ArchiveTransientErrorMsg, tag.ArchivalArchiveFailReason("got retryable error from history archiver"), tag.Error(err))
	return err
}

func deleteHistoryActivity(ctx context.Context, request ArchiveRequest) (err error) {
	container := ctx.Value(bootstrapContainerKey).(*BootstrapContainer)
	scope := container.MetricsClient.Scope(metrics.ArchiverDeleteHistoryActivityScope, metrics.DomainTag(request.DomainName))
	sw := scope.StartTimer(metrics.CadenceLatency)
	defer func() {
		sw.Stop()
		if err != nil {
			if err.Error() == errDeleteNonRetriable.Error() {
				scope.IncCounter(metrics.ArchiverNonRetryableErrorCount)
			}
			err = cadence.NewCustomError(err.Error())
		}
	}()
	logger := tagLoggerWithHistoryRequest(tagLoggerWithActivityInfo(container.Logger, activity.GetInfo(ctx)), &request)
	err = persistence.DeleteWorkflowExecutionHistoryV2(container.HistoryV2Manager, request.BranchToken, common.IntPtr(request.ShardID), container.Logger)
	if err == nil {
		return nil
	}
	logger.Error("failed to delete history from events v2", tag.Error(err))
	if !common.IsPersistenceTransientError(err) {
		return errDeleteNonRetriable
	}
	return err
}

func archiveVisibilityActivity(ctx context.Context, request ArchiveRequest) (err error) {
	container := ctx.Value(bootstrapContainerKey).(*BootstrapContainer)
	scope := container.MetricsClient.Scope(metrics.ArchiverArchiveVisibilityActivityScope, metrics.DomainTag(request.DomainName))
	sw := scope.StartTimer(metrics.CadenceLatency)
	defer func() {
		sw.Stop()
		if err != nil {
			if err.Error() == errArchiveVisibilityNonRetriable.Error() {
				scope.IncCounter(metrics.ArchiverNonRetryableErrorCount)
			}
			err = cadence.NewCustomError(err.Error())
		}
	}()
	logger := tagLoggerWithVisibilityRequest(tagLoggerWithActivityInfo(container.Logger, activity.GetInfo(ctx)), &request)
	URI, err := carchiver.NewURI(request.VisibilityURI)
	if err != nil {
		logger.Error(carchiver.ArchiveNonRetriableErrorMsg, tag.ArchivalArchiveFailReason("failed to get visibility archival uri"), tag.ArchivalURI(request.VisibilityURI), tag.Error(err))
		return errArchiveVisibilityNonRetriable
	}
	visibilityArchiver, err := container.ArchiverProvider.GetVisibilityArchiver(URI.Scheme(), common.WorkerServiceName)
	if err != nil {
		logger.Error(carchiver.ArchiveNonRetriableErrorMsg, tag.ArchivalArchiveFailReason("failed to get visibility archiver"), tag.Error(err))
		return errArchiveVisibilityNonRetriable
	}
	err = visibilityArchiver.Archive(ctx, URI, &carchiver.ArchiveVisibilityRequest{
		DomainID:           request.DomainID,
		DomainName:         request.DomainName,
		WorkflowID:         request.WorkflowID,
		RunID:              request.RunID,
		WorkflowTypeName:   request.WorkflowTypeName,
		StartTimestamp:     request.StartTimestamp,
		ExecutionTimestamp: request.ExecutionTimestamp,
		CloseTimestamp:     request.CloseTimestamp,
		CloseStatus:        request.CloseStatus,
		HistoryLength:      request.HistoryLength,
		Memo:               request.Memo,
		SearchAttributes:   convertSearchAttributesToString(request.SearchAttributes),
		HistoryArchivalURI: request.URI,
	}, carchiver.GetNonRetriableErrorOption(errArchiveVisibilityNonRetriable))
	if err == nil {
		return nil
	}
	if err.Error() == errArchiveVisibilityNonRetriable.Error() {
		logger.Error(carchiver.ArchiveNonRetriableErrorMsg, tag.ArchivalArchiveFailReason("got non-retryable error from visibility archiver"))
		return errArchiveVisibilityNonRetriable
	}
	logger.Error(carchiver.ArchiveTransientErrorMsg, tag.ArchivalArchiveFailReason("got retryable error from visibility archiver"), tag.Error(err))
	return err
}
