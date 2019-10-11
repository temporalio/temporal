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
	"fmt"
	"math/rand"
	"time"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	carchiver "github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/archiver/provider"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/quotas"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	cclient "go.uber.org/cadence/client"
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
		DomainID   string
		DomainName string
		WorkflowID string
		RunID      string

		// history archival
		ShardID              int
		BranchToken          []byte
		NextEventID          int64
		CloseFailoverVersion int64
		URI                  string // should be historyURI, but keep the existing name for backward compatibility

		// visibility archival
		WorkflowTypeName   string
		StartTimestamp     int64
		ExecutionTimestamp int64
		CloseTimestamp     int64
		CloseStatus        shared.WorkflowExecutionCloseStatus
		HistoryLength      int64
		Memo               *shared.Memo
		SearchAttributes   map[string][]byte
		VisibilityURI      string

		// archival targets: history and/or visibility
		Targets []archivalTarget
	}

	// Client is used to archive workflow histories
	Client interface {
		Archive(context.Context, *ClientRequest) (*ClientResponse, error)
	}

	client struct {
		metricsScope     metrics.Scope
		logger           log.Logger
		cadenceClient    cclient.Client
		numWorkflows     dynamicconfig.IntPropertyFn
		rateLimiter      quotas.Limiter
		archiverProvider provider.ArchiverProvider
	}

	archivalTarget int
)

const (
	signalTimeout = 300 * time.Millisecond

	tooManyRequestsErrMsg = "too many requests to archival workflow"
)

const (
	// ArchiveTargetHistory is the archive target for workflow history
	ArchiveTargetHistory archivalTarget = iota
	// ArchiveTargetVisibility is the archive target for workflow visibility record
	ArchiveTargetVisibility
)

// NewClient creates a new Client
func NewClient(
	metricsClient metrics.Client,
	logger log.Logger,
	publicClient workflowserviceclient.Interface,
	numWorkflows dynamicconfig.IntPropertyFn,
	requestRPS dynamicconfig.IntPropertyFn,
	archiverProvider provider.ArchiverProvider,
) Client {
	return &client{
		metricsScope:  metricsClient.Scope(metrics.ArchiverClientScope),
		logger:        logger,
		cadenceClient: cclient.NewClient(publicClient, common.SystemLocalDomainName, &cclient.Options{}),
		numWorkflows:  numWorkflows,
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

		targets := []archivalTarget{}
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
	URI, err := carchiver.NewURI(request.ArchiveRequest.URI)
	if err != nil {
		return
	}

	historyArchiver, err := c.archiverProvider.GetHistoryArchiver(URI.Scheme(), request.CallerService)
	if err != nil {
		return
	}

	err = historyArchiver.Archive(ctx, URI, &carchiver.ArchiveHistoryRequest{
		ShardID:              request.ArchiveRequest.ShardID,
		DomainID:             request.ArchiveRequest.DomainID,
		DomainName:           request.ArchiveRequest.DomainName,
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

	err = visibilityArchiver.Archive(ctx, URI, &carchiver.ArchiveVisibilityRequest{
		DomainID:           request.ArchiveRequest.DomainID,
		DomainName:         request.ArchiveRequest.DomainName,
		WorkflowID:         request.ArchiveRequest.WorkflowID,
		RunID:              request.ArchiveRequest.RunID,
		WorkflowTypeName:   request.ArchiveRequest.WorkflowTypeName,
		StartTimestamp:     request.ArchiveRequest.StartTimestamp,
		ExecutionTimestamp: request.ArchiveRequest.ExecutionTimestamp,
		CloseTimestamp:     request.ArchiveRequest.CloseTimestamp,
		CloseStatus:        request.ArchiveRequest.CloseStatus,
		HistoryLength:      request.ArchiveRequest.HistoryLength,
		Memo:               request.ArchiveRequest.Memo,
		SearchAttributes:   convertSearchAttributesToString(request.ArchiveRequest.SearchAttributes),
		HistoryArchivalURI: request.ArchiveRequest.URI,
	})
}

func (c *client) sendArchiveSignal(ctx context.Context, request *ArchiveRequest, taggedLogger log.Logger) error {
	fmt.Println(request.Targets)
	if ok := c.rateLimiter.Allow(); !ok {
		c.logger.Error(tooManyRequestsErrMsg)
		c.metricsScope.IncCounter(metrics.CadenceErrServiceBusyCounter)
		return errors.New(tooManyRequestsErrMsg)
	}

	workflowID := fmt.Sprintf("%v-%v", workflowIDPrefix, rand.Intn(c.numWorkflows()))
	workflowOptions := cclient.StartWorkflowOptions{
		ID:                              workflowID,
		TaskList:                        decisionTaskList,
		ExecutionStartToCloseTimeout:    workflowStartToCloseTimeout,
		DecisionTaskStartToCloseTimeout: workflowTaskStartToCloseTimeout,
		WorkflowIDReusePolicy:           cclient.WorkflowIDReusePolicyAllowDuplicate,
	}
	signalCtx, cancel := context.WithTimeout(context.Background(), signalTimeout)
	defer cancel()
	_, err := c.cadenceClient.SignalWithStartWorkflow(signalCtx, workflowID, signalName, *request, workflowOptions, archivalWorkflowFnName, nil)
	if err != nil {
		taggedLogger = taggedLogger.WithTags(
			tag.ArchivalRequestDomainID(request.DomainID),
			tag.ArchivalRequestDomainName(request.DomainName),
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
