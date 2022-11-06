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
	"go.uber.org/multierr"

	archiverspb "go.temporal.io/server/api/archiver/v1"
	carchiver "go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives/timestamp"
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
		HistoryURI           string

		// visibility archival
		WorkflowTypeName string
		StartTime        time.Time
		ExecutionTime    time.Time
		CloseTime        time.Time
		Status           enumspb.WorkflowExecutionStatus
		HistoryLength    int64
		Memo             *commonpb.Memo
		SearchAttributes *commonpb.SearchAttributes
		VisibilityURI    string

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
		archiverProvider provider.ArchiverProvider
		metricsHandler   metrics.MetricsHandler
		logger           log.Logger
		rateLimiter      quotas.RateLimiter
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
	metricsHandler metrics.MetricsHandler,
	rateLimiter quotas.RateLimiter,
) Archiver {
	return &archiver{
		archiverProvider: archiverProvider,
		metricsHandler:   metricsHandler.WithTags(metrics.OperationTag(metrics.ArchiverClientScope)),
		logger:           logger,
		rateLimiter:      rateLimiter,
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
		metricsScope.Timer(metrics.ArchiverArchiveLatency.GetMetricName()).
			Record(time.Since(start), metrics.StringTag("status", status))
	}(time.Now())
	if err := a.rateLimiter.WaitN(ctx, 2); err != nil {
		return nil, &serviceerror.ResourceExhausted{
			Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT,
			Message: fmt.Sprintf("archival rate limited: %s", err.Error()),
		}
	}

	var wg sync.WaitGroup
	errs := make([]error, len(request.Targets))
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
		tag.ArchivalURI(request.HistoryURI),
	)
	defer a.recordArchiveTargetResult(logger, time.Now(), TargetHistory, &err)

	URI, err := carchiver.NewURI(request.HistoryURI)
	if err != nil {
		return err
	}

	historyArchiver, err := a.archiverProvider.GetHistoryArchiver(URI.Scheme(), request.CallerService)
	if err != nil {
		return err
	}

	return historyArchiver.Archive(ctx, URI, &carchiver.ArchiveHistoryRequest{
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
		tag.ArchivalURI(request.VisibilityURI),
	)
	defer a.recordArchiveTargetResult(logger, time.Now(), TargetVisibility, &err)

	uri, err := carchiver.NewURI(request.VisibilityURI)
	if err != nil {
		return err
	}

	visibilityArchiver, err := a.archiverProvider.GetVisibilityArchiver(uri.Scheme(), request.CallerService)
	if err != nil {
		return err
	}

	// It is safe to pass nil to typeMap here because search attributes type must be embedded by caller.
	searchAttributes, err := searchattribute.Stringify(request.SearchAttributes, nil)
	if err != nil {
		return err
	}
	return visibilityArchiver.Archive(ctx, uri, &archiverspb.VisibilityRecord{
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
	latency := metrics.ArchiverArchiveTargetLatency.GetMetricName()
	a.metricsHandler.Timer(latency).Record(duration, tags...)
}
