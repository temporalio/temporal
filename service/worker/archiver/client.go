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
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/service/dynamicconfig"
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
		NamespaceID string
		Namespace   string
		WorkflowID  string
		RunID       string

		// history archival
		ShardID              int
		BranchToken          []byte
		NextEventID          int64
		CloseFailoverVersion int64
		HistoryURI           string

		// visibility archival
		WorkflowTypeName   string
		StartTimestamp     int64
		ExecutionTimestamp int64
		CloseTimestamp     int64
		Status             enumspb.WorkflowExecutionStatus
		HistoryLength      int64
		Memo               *commonpb.Memo
		SearchAttributes   map[string]*commonpb.Payload
		VisibilityURI      string

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
		temporalClient   sdkclient.Client
		numWorkflows     dynamicconfig.IntPropertyFn
		rateLimiter      quotas.Limiter
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
	publicClient sdkclient.Client,
	numWorkflows dynamicconfig.IntPropertyFn,
	requestRPS dynamicconfig.IntPropertyFn,
	archiverProvider provider.ArchiverProvider,
) Client {
	return &client{
		metricsScope:   metricsClient.Scope(metrics.ArchiverClientScope),
		logger:         logger,
		temporalClient: publicClient,
		numWorkflows:   numWorkflows,
		rateLimiter: quotas.NewDynamicRateLimiter(
			func() float64 {
				return float64(requestRPS())
			},
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
	logger := c.logger.WithTags(
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
	URI, err := carchiver.NewURI(request.ArchiveRequest.VisibilityURI)
	if err != nil {
		return
	}

	visibilityArchiver, err := c.archiverProvider.GetVisibilityArchiver(URI.Scheme(), request.CallerService)
	if err != nil {
		return
	}

	err = visibilityArchiver.Archive(ctx, URI, &archiverspb.ArchiveVisibilityRequest{
		NamespaceId:        request.ArchiveRequest.NamespaceID,
		Namespace:          request.ArchiveRequest.Namespace,
		WorkflowId:         request.ArchiveRequest.WorkflowID,
		RunId:              request.ArchiveRequest.RunID,
		WorkflowTypeName:   request.ArchiveRequest.WorkflowTypeName,
		StartTime:          timestamp.UnixOrZeroTimePtr(request.ArchiveRequest.StartTimestamp),
		ExecutionTime:      timestamp.UnixOrZeroTimePtr(request.ArchiveRequest.ExecutionTimestamp),
		CloseTime:          timestamp.UnixOrZeroTimePtr(request.ArchiveRequest.CloseTimestamp),
		Status:             request.ArchiveRequest.Status,
		HistoryLength:      request.ArchiveRequest.HistoryLength,
		Memo:               request.ArchiveRequest.Memo,
		SearchAttributes:   convertSearchAttributesToString(request.ArchiveRequest.SearchAttributes),
		HistoryArchivalUri: request.ArchiveRequest.HistoryURI,
	})
}

func (c *client) sendArchiveSignal(ctx context.Context, request *ArchiveRequest, taggedLogger log.Logger) error {
	c.metricsScope.IncCounter(metrics.ArchiverClientSendSignalCount)
	if ok := c.rateLimiter.Allow(); !ok {
		c.logger.Error(tooManyRequestsErrMsg)
		c.metricsScope.IncCounter(metrics.ServiceErrResourceExhaustedCounter)
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
	_, err := c.temporalClient.SignalWithStartWorkflow(signalCtx, workflowID, signalName, *request, workflowOptions, archivalWorkflowFnName, nil)
	if err != nil {
		taggedLogger = taggedLogger.WithTags(
			tag.ArchivalRequestNamespaceID(request.NamespaceID),
			tag.ArchivalRequestNamespace(request.Namespace),
			tag.ArchivalRequestWorkflowID(request.WorkflowID),
			tag.ArchivalRequestRunID(request.RunID),
			tag.WorkflowID(workflowID),
			tag.Error(err),
		)
		taggedLogger.Error("failed to send signal to archival system workflow")
		c.metricsScope.IncCounter(metrics.ArchiverClientSendSignalFailureCount)
		return err
	}
	return nil
}
