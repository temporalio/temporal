package archiver

import (
	"context"
	"errors"

	"go.temporal.io/temporal"
	"go.temporal.io/temporal/activity"

	archiverproto "github.com/temporalio/temporal/.gen/proto/archiver"
	"github.com/temporalio/temporal/common"
	carchiver "github.com/temporalio/temporal/common/archiver"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
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

	uploadHistoryActivityNonRetryableErrors = []string{"temporalInternal:Panic", errUploadNonRetriable.Error()}
	deleteHistoryActivityNonRetryableErrors = []string{"temporalInternal:Panic", errDeleteNonRetriable.Error()}
)

func uploadHistoryActivity(ctx context.Context, request ArchiveRequest) (err error) {
	container := ctx.Value(bootstrapContainerKey).(*BootstrapContainer)
	scope := container.MetricsClient.Scope(metrics.ArchiverUploadHistoryActivityScope, metrics.NamespaceTag(request.Namespace))
	sw := scope.StartTimer(metrics.ServiceLatency)
	defer func() {
		sw.Stop()
		if err != nil {
			if err.Error() == errUploadNonRetriable.Error() {
				scope.IncCounter(metrics.ArchiverNonRetryableErrorCount)
			}
			err = temporal.NewCustomError(err.Error())
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
		NamespaceID:          request.NamespaceID,
		Namespace:            request.Namespace,
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
	scope := container.MetricsClient.Scope(metrics.ArchiverDeleteHistoryActivityScope, metrics.NamespaceTag(request.Namespace))
	sw := scope.StartTimer(metrics.ServiceLatency)
	defer func() {
		sw.Stop()
		if err != nil {
			if err.Error() == errDeleteNonRetriable.Error() {
				scope.IncCounter(metrics.ArchiverNonRetryableErrorCount)
			}
			err = temporal.NewCustomError(err.Error())
		}
	}()
	err = container.HistoryV2Manager.DeleteHistoryBranch(&persistence.DeleteHistoryBranchRequest{
		BranchToken: request.BranchToken,
		ShardID:     common.IntPtr(request.ShardID),
	})
	if err == nil {
		return nil
	}
	logger := tagLoggerWithHistoryRequest(tagLoggerWithActivityInfo(container.Logger, activity.GetInfo(ctx)), &request)
	logger.Error("failed to delete history events", tag.Error(err))
	if !common.IsPersistenceTransientError(err) {
		return errDeleteNonRetriable
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
			if err.Error() == errArchiveVisibilityNonRetriable.Error() {
				scope.IncCounter(metrics.ArchiverNonRetryableErrorCount)
			}
			err = temporal.NewCustomError(err.Error())
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
	err = visibilityArchiver.Archive(ctx, URI, &archiverproto.ArchiveVisibilityRequest{
		NamespaceId:        request.NamespaceID,
		Namespace:          request.Namespace,
		WorkflowId:         request.WorkflowID,
		RunId:              request.RunID,
		WorkflowTypeName:   request.WorkflowTypeName,
		StartTimestamp:     request.StartTimestamp,
		ExecutionTimestamp: request.ExecutionTimestamp,
		CloseTimestamp:     request.CloseTimestamp,
		Status:             request.Status,
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
