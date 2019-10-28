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
	uploadHistoryActivityFnName = "uploadHistoryActivity"
	deleteHistoryActivityFnName = "deleteHistoryActivity"
)

var (
	errUploadNonRetriable = errors.New("upload non-retriable error")
	errDeleteNonRetriable = errors.New("delete non-retriable error")

	uploadHistoryActivityNonRetryableErrors = []string{"cadenceInternal:Panic", errUploadNonRetriable.Error()}
	deleteHistoryActivityNonRetryableErrors = []string{"cadenceInternal:Panic", errDeleteNonRetriable.Error()}
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
	logger := tagLoggerWithRequest(tagLoggerWithActivityInfo(container.Logger, activity.GetInfo(ctx)), request)
	URI, err := carchiver.NewURI(request.URI)
	if err != nil {
		logger.Error(carchiver.ArchiveNonRetriableErrorMsg, tag.ArchivalArchiveFailReason("failed to get archival uri"), tag.ArchivalURI(request.URI), tag.Error(err))
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
		EventStoreVersion:    request.EventStoreVersion,
		BranchToken:          request.BranchToken,
		NextEventID:          request.NextEventID,
		CloseFailoverVersion: request.CloseFailoverVersion,
	}, carchiver.GetHeartbeatArchiveOption(), carchiver.GetNonRetriableErrorOption(errUploadNonRetriable))
	if err == nil {
		return nil
	}
	if err.Error() == errUploadNonRetriable.Error() {
		logger.Error(carchiver.ArchiveNonRetriableErrorMsg, tag.ArchivalArchiveFailReason("got non-retryable error from archiver"))
		return errUploadNonRetriable
	}
	logger.Error(carchiver.ArchiveTransientErrorMsg, tag.ArchivalArchiveFailReason("got retryable error from archiver"), tag.Error(err))
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
	err = container.HistoryV2Manager.DeleteHistoryBranch(&persistence.DeleteHistoryBranchRequest{
		BranchToken: request.BranchToken,
		ShardID:     common.IntPtr(request.ShardID),
	})
	if err == nil {
		return nil
	}
	logger := tagLoggerWithRequest(tagLoggerWithActivityInfo(container.Logger, activity.GetInfo(ctx)), request)
	logger.Error("failed to delete history events", tag.Error(err))
	if !common.IsPersistenceTransientError(err) {
		return errDeleteNonRetriable
	}
	return err
}
