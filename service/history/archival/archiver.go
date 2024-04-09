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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination archiver_mock.go

package archival

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.uber.org/multierr"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	archiverspb "go.temporal.io/server/api/archiver/v1"
	carchiver "go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/searchattribute"
)

type (
	Request struct {
		ShardID     int32
		NamespaceID string
		Namespace   string
		WorkflowID  string
		RunID       string

		// history archival
		BranchToken          []byte
		NextEventID          int64
		CloseFailoverVersion int64
		// HistoryURI is the URI of the history archival backend.
		HistoryURI carchiver.URI

		// visibility archival
		WorkflowTypeName  string
		StartTime         *timestamppb.Timestamp
		ExecutionTime     *timestamppb.Timestamp
		CloseTime         *timestamppb.Timestamp
		ExecutionDuration *durationpb.Duration
		Status            enumspb.WorkflowExecutionStatus
		HistoryLength     int64
		Memo              *commonpb.Memo
		SearchAttributes  *commonpb.SearchAttributes
		// VisibilityURI is the URI of the visibility archival backend.
		VisibilityURI carchiver.URI

		// archival targets: history and/or visibility
		Targets       []Target
		CallerService string
	}

	Target string

	Response struct {
	}

	// Archiver is used to archive workflow history and visibility data. If a target fails, it returns an error, unlike
	// archiver.Client, which will try to signal an archival workflow whenever an error occurs.
	Archiver interface {
		Archive(context.Context, *Request) (*Response, error)
	}

	archiver struct {
		archiverProvider        provider.ArchiverProvider
		metricsHandler          metrics.Handler
		logger                  log.Logger
		rateLimiter             quotas.RateLimiter
		searchAttributeProvider searchattribute.Provider
		visibilityManager       manager.VisibilityManager
	}
)

const (
	TargetHistory    Target = "history"
	TargetVisibility Target = "visibility"
)

// NewArchiver creates a new Archiver
func NewArchiver(
	archiverProvider provider.ArchiverProvider,
	logger log.Logger,
	metricsHandler metrics.Handler,
	rateLimiter quotas.RateLimiter,
	searchAttributeProvider searchattribute.Provider,
	visibilityManger manager.VisibilityManager,
) Archiver {
	return &archiver{
		archiverProvider:        archiverProvider,
		metricsHandler:          metricsHandler.WithTags(metrics.OperationTag(metrics.ArchiverClientScope)),
		logger:                  logger,
		rateLimiter:             rateLimiter,
		searchAttributeProvider: searchAttributeProvider,
		visibilityManager:       visibilityManger,
	}
}

func (a *archiver) Archive(ctx context.Context, request *Request) (res *Response, err error) {
	logger := log.With(
		a.logger,
		tag.ShardID(request.ShardID),
		tag.ArchivalCallerServiceName(request.CallerService),
		tag.ArchivalRequestNamespaceID(request.NamespaceID),
		tag.ArchivalRequestNamespace(request.Namespace),
		tag.ArchivalRequestWorkflowID(request.WorkflowID),
		tag.ArchivalRequestRunID(request.RunID),
	)

	defer func(start time.Time) {
		metricsScope := a.metricsHandler

		status := "ok"
		if err != nil {
			status = "err"

			var rateLimitExceededErr *serviceerror.ResourceExhausted

			if errors.As(err, &rateLimitExceededErr) {
				status = "rate_limit_exceeded"
			}

			logger.Warn("failed to archive workflow", tag.Error(err))
		}

		metrics.ArchiverArchiveLatency.With(metricsScope).
			Record(time.Since(start), metrics.StringTag("status", status))
	}(time.Now())

	numTargets := len(request.Targets)
	if err := a.rateLimiter.WaitN(ctx, numTargets); err != nil {
		return nil, &serviceerror.ResourceExhausted{
			Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT,
			Message: fmt.Sprintf("archival rate limited: %s", err.Error()),
		}
	}

	var wg sync.WaitGroup

	errs := make([]error, numTargets)

	for i, target := range request.Targets {
		wg.Add(1)

		i := i

		switch target {
		case TargetHistory:
			go func() {
				defer wg.Done()

				errs[i] = a.archiveHistory(ctx, request, logger)
			}()
		case TargetVisibility:
			go func() {
				defer wg.Done()

				errs[i] = a.archiveVisibility(ctx, request, logger)
			}()
		default:
			return nil, fmt.Errorf("unknown archival target: %s", target)
		}
	}

	wg.Wait()

	return &Response{}, multierr.Combine(errs...)
}

func (a *archiver) archiveHistory(ctx context.Context, request *Request, logger log.Logger) (err error) {
	logger = log.With(
		logger,
		tag.ArchivalRequestBranchToken(request.BranchToken),
		tag.ArchivalRequestCloseFailoverVersion(request.CloseFailoverVersion),
		tag.ArchivalURI(request.HistoryURI.String()),
	)
	defer a.recordArchiveTargetResult(logger, time.Now(), TargetHistory, &err)

	historyArchiver, err := a.archiverProvider.GetHistoryArchiver(request.HistoryURI.Scheme(), request.CallerService)
	if err != nil {
		return err
	}

	return historyArchiver.Archive(ctx, request.HistoryURI, &carchiver.ArchiveHistoryRequest{
		ShardID:              request.ShardID,
		NamespaceID:          request.NamespaceID,
		Namespace:            request.Namespace,
		WorkflowID:           request.WorkflowID,
		RunID:                request.RunID,
		BranchToken:          request.BranchToken,
		NextEventID:          request.NextEventID,
		CloseFailoverVersion: request.CloseFailoverVersion,
	})
}

func (a *archiver) archiveVisibility(ctx context.Context, request *Request, logger log.Logger) (err error) {
	logger = log.With(
		logger,
		tag.ArchivalURI(request.VisibilityURI.String()),
	)
	defer a.recordArchiveTargetResult(logger, time.Now(), TargetVisibility, &err)

	visibilityArchiver, err := a.archiverProvider.GetVisibilityArchiver(request.VisibilityURI.Scheme(), request.CallerService)
	if err != nil {
		return err
	}

	// The types of the search attributes may not be embedded in the request,
	// so we fetch them from the search attributes provider here.
	saTypeMap, err := a.searchAttributeProvider.GetSearchAttributes(a.visibilityManager.GetIndexName(), false)
	if err != nil {
		return err
	}

	searchAttributes, err := searchattribute.Stringify(request.SearchAttributes, &saTypeMap)
	if err != nil {
		return err
	}

	var historyArchivalUri string
	if request.HistoryURI != nil {
		historyArchivalUri = request.HistoryURI.String()
	}

	return visibilityArchiver.Archive(ctx, request.VisibilityURI, &archiverspb.VisibilityRecord{
		NamespaceId:        request.NamespaceID,
		Namespace:          request.Namespace,
		WorkflowId:         request.WorkflowID,
		RunId:              request.RunID,
		WorkflowTypeName:   request.WorkflowTypeName,
		StartTime:          request.StartTime,
		ExecutionTime:      request.ExecutionTime,
		CloseTime:          request.CloseTime,
		ExecutionDuration:  request.ExecutionDuration,
		Status:             request.Status,
		HistoryLength:      request.HistoryLength,
		Memo:               request.Memo,
		SearchAttributes:   searchAttributes,
		HistoryArchivalUri: historyArchivalUri,
	})
}

// recordArchiveTargetResult takes an error pointer as an argument so that it isn't passed-by-value when used in a defer
// statement (this would make the err always nil).
func (a *archiver) recordArchiveTargetResult(logger log.Logger, startTime time.Time, target Target, err *error) {
	duration := time.Since(startTime)

	status := "ok"
	if *err != nil {
		status = "err"

		logger.Error("failed to archive target", tag.NewStringTag("target", string(target)), tag.Error(*err))
	}

	tags := []metrics.Tag{
		metrics.StringTag("target", string(target)),
		metrics.StringTag("status", status),
	}

	metrics.ArchiverArchiveTargetLatency.With(a.metricsHandler).Record(duration, tags...)
}
