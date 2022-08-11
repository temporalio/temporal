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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination client_mock.go

package archiver

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	sdkclient "go.temporal.io/sdk/client"

	archiverspb "go.temporal.io/server/api/archiver/v1"
	carchiver "go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute"
)

type (
	// ClientRequest is the archive request sent to the archiver client
	ClientRequest struct {
		ArchiveRequest       *ArchiveRequest
		CallerService        string
		AttemptArchiveInline bool
	}

	// ClientResponse is the archive response returned from the archiver client
	ClientResponse struct {
		HistoryArchivedInline bool
	}

	// ArchiveRequest is the request signal sent to the archival workflow
	ArchiveRequest struct {
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
		Targets []ArchivalTarget
	}

	// Client is used to archive workflow histories
	Client interface {
		Archive(context.Context, *ClientRequest) (*ClientResponse, error)
	}

	client struct {
		metricsScope     metrics.Scope
		logger           log.Logger
		sdkClientFactory sdk.ClientFactory
		numWorkflows     dynamicconfig.IntPropertyFn
		rateLimiter      quotas.RateLimiter
		archiverProvider provider.ArchiverProvider
	}

	// ArchivalTarget is either history or visibility
	ArchivalTarget int
)

const (
	signalTimeout = 300 * time.Millisecond

	tooManyRequestsErrMsg = "too many requests to archival workflow"
)

const (
	// ArchiveTargetHistory is the archive target for workflow history
	ArchiveTargetHistory ArchivalTarget = iota
	// ArchiveTargetVisibility is the archive target for workflow visibility record
	ArchiveTargetVisibility
)

// NewClient creates a new Client
func NewClient(
	metricsClient metrics.Client,
	logger log.Logger,
	sdkClientFactory sdk.ClientFactory,
	numWorkflows dynamicconfig.IntPropertyFn,
	requestRPS dynamicconfig.IntPropertyFn,
	archiverProvider provider.ArchiverProvider,
) Client {
	return &client{
		metricsScope:     metricsClient.Scope(metrics.ArchiverClientScope),
		logger:           logger,
		sdkClientFactory: sdkClientFactory,
		numWorkflows:     numWorkflows,
		rateLimiter: quotas.NewDefaultOutgoingRateLimiter(
			func() float64 { return float64(requestRPS()) },
		),
		archiverProvider: archiverProvider,
	}
}

// Archive starts an archival task
func (c *client) Archive(ctx context.Context, request *ClientRequest) (*ClientResponse, error) {
	for _, target := range request.ArchiveRequest.Targets {
		switch target {
		case ArchiveTargetHistory:
			c.metricsScope.IncCounter(metrics.ArchiverClientHistoryRequestCount)
		case ArchiveTargetVisibility:
			c.metricsScope.IncCounter(metrics.ArchiverClientVisibilityRequestCount)
		}
	}
	logger := log.With(
		c.logger,
		tag.ShardID(request.ArchiveRequest.ShardID),
		tag.ArchivalCallerServiceName(request.CallerService),
		tag.ArchivalArchiveAttemptedInline(request.AttemptArchiveInline),
	)
	resp := &ClientResponse{
		HistoryArchivedInline: false,
	}
	if request.AttemptArchiveInline {
		results := []chan error{}
		for _, target := range request.ArchiveRequest.Targets {
			ch := make(chan error)
			results = append(results, ch)
			switch target {
			case ArchiveTargetHistory:
				go c.archiveHistoryInline(ctx, request, logger, ch)
			case ArchiveTargetVisibility:
				go c.archiveVisibilityInline(ctx, request, logger, ch)
			default:
				close(ch)
			}
		}

		targets := []ArchivalTarget{}
		for i, target := range request.ArchiveRequest.Targets {
			if <-results[i] != nil {
				targets = append(targets, target)
			} else if target == ArchiveTargetHistory {
				resp.HistoryArchivedInline = true
			}
		}
		request.ArchiveRequest.Targets = targets
	}
	if len(request.ArchiveRequest.Targets) != 0 {
		if err := c.sendArchiveSignal(ctx, request.ArchiveRequest, logger); err != nil {
			return nil, err
		}
	}
	return resp, nil
}

func (c *client) archiveHistoryInline(ctx context.Context, request *ClientRequest, logger log.Logger, errCh chan error) {
	logger = tagLoggerWithHistoryRequest(logger, request.ArchiveRequest)
	var err error
	defer func() {
		if err != nil {
			c.metricsScope.IncCounter(metrics.ArchiverClientHistoryInlineArchiveFailureCount)
			logger.Info("failed to perform workflow history archival inline", tag.Error(err))
		}
		errCh <- err
	}()
	c.metricsScope.IncCounter(metrics.ArchiverClientHistoryInlineArchiveAttemptCount)
	URI, err := carchiver.NewURI(request.ArchiveRequest.HistoryURI)
	if err != nil {
		return
	}

	historyArchiver, err := c.archiverProvider.GetHistoryArchiver(URI.Scheme(), request.CallerService)
	if err != nil {
		return
	}

	err = historyArchiver.Archive(ctx, URI, &carchiver.ArchiveHistoryRequest{
		ShardID:              request.ArchiveRequest.ShardID,
		NamespaceID:          request.ArchiveRequest.NamespaceID,
		Namespace:            request.ArchiveRequest.Namespace,
		WorkflowID:           request.ArchiveRequest.WorkflowID,
		RunID:                request.ArchiveRequest.RunID,
		BranchToken:          request.ArchiveRequest.BranchToken,
		NextEventID:          request.ArchiveRequest.NextEventID,
		CloseFailoverVersion: request.ArchiveRequest.CloseFailoverVersion,
	})
}

func (c *client) archiveVisibilityInline(ctx context.Context, request *ClientRequest, logger log.Logger, errCh chan error) {
	logger = tagLoggerWithVisibilityRequest(logger, request.ArchiveRequest)

	var err error
	defer func() {
		if err != nil {
			c.metricsScope.IncCounter(metrics.ArchiverClientVisibilityInlineArchiveFailureCount)
			logger.Info("failed to perform visibility archival inline", tag.Error(err))
		}
		errCh <- err
	}()
	c.metricsScope.IncCounter(metrics.ArchiverClientVisibilityInlineArchiveAttemptCount)
	var uri carchiver.URI
	uri, err = carchiver.NewURI(request.ArchiveRequest.VisibilityURI)
	if err != nil {
		return
	}

	var visibilityArchiver carchiver.VisibilityArchiver
	visibilityArchiver, err = c.archiverProvider.GetVisibilityArchiver(uri.Scheme(), request.CallerService)
	if err != nil {
		return
	}

	// It is safe to pass nil to typeMap here because search attributes type must be embedded by caller.
	searchAttributes, err := searchattribute.Stringify(request.ArchiveRequest.SearchAttributes, nil)
	if err != nil {
		logger.Error("Unable to stringify search attributes.", tag.Error(err))
		return
	}
	err = visibilityArchiver.Archive(ctx, uri, &archiverspb.VisibilityRecord{
		NamespaceId:        request.ArchiveRequest.NamespaceID,
		Namespace:          request.ArchiveRequest.Namespace,
		WorkflowId:         request.ArchiveRequest.WorkflowID,
		RunId:              request.ArchiveRequest.RunID,
		WorkflowTypeName:   request.ArchiveRequest.WorkflowTypeName,
		StartTime:          timestamp.TimePtr(request.ArchiveRequest.StartTime),
		ExecutionTime:      timestamp.TimePtr(request.ArchiveRequest.ExecutionTime),
		CloseTime:          timestamp.TimePtr(request.ArchiveRequest.CloseTime),
		Status:             request.ArchiveRequest.Status,
		HistoryLength:      request.ArchiveRequest.HistoryLength,
		Memo:               request.ArchiveRequest.Memo,
		SearchAttributes:   searchAttributes,
		HistoryArchivalUri: request.ArchiveRequest.HistoryURI,
	})
}

func (c *client) sendArchiveSignal(ctx context.Context, request *ArchiveRequest, taggedLogger log.Logger) error {
	c.metricsScope.IncCounter(metrics.ArchiverClientSendSignalCount)
	if ok := c.rateLimiter.Allow(); !ok {
		c.logger.Error(tooManyRequestsErrMsg)
		c.metricsScope.Tagged(metrics.ResourceExhaustedCauseTag(enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT)).
			IncCounter(metrics.ServiceErrResourceExhaustedCounter)
		return errors.New(tooManyRequestsErrMsg)
	}

	workflowID := fmt.Sprintf("%v-%v", workflowIDPrefix, rand.Intn(c.numWorkflows()))
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                       workflowID,
		TaskQueue:                workflowTaskQueue,
		WorkflowExecutionTimeout: workflowRunTimeout,
		WorkflowTaskTimeout:      workflowTaskTimeout,
		WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}
	signalCtx, cancel := context.WithTimeout(context.Background(), signalTimeout)
	defer cancel()

	sdkClient := c.sdkClientFactory.GetSystemClient(c.logger)
	_, err := sdkClient.SignalWithStartWorkflow(signalCtx, workflowID, signalName, *request, workflowOptions, archivalWorkflowFnName, nil)
	if err != nil {
		taggedLogger.Error("failed to send signal to archival system workflow",
			tag.ArchivalRequestNamespaceID(request.NamespaceID),
			tag.ArchivalRequestNamespace(request.Namespace),
			tag.ArchivalRequestWorkflowID(request.WorkflowID),
			tag.ArchivalRequestRunID(request.RunID),
			tag.WorkflowID(workflowID),
			tag.Error(err))

		c.metricsScope.IncCounter(metrics.ArchiverClientSendSignalFailureCount)
		return err
	}
	return nil
}
