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
	// ClientHistoryRequest is the archive request sent to the archiver client
	ClientHistoryRequest struct {
		ArchiveHistoryRequest *ArchiveHistoryRequest
		CallerService         string
		AttemptArchiveInline  bool
	}

	// ClientResponse is the archive response returned from the archiver client
	ClientResponse struct {
		ArchivedInline bool
	}

	// ArchiveHistoryRequest is the archival history request signal sent to the archiver workflow
	ArchiveHistoryRequest struct {
		ShardID              int
		DomainID             string
		DomainName           string
		WorkflowID           string
		RunID                string
		EventStoreVersion    int32
		BranchToken          []byte
		NextEventID          int64
		CloseFailoverVersion int64
		URI                  string
	}

	// Client is used to archive workflow histories
	Client interface {
		ArchiveHistory(context.Context, *ClientHistoryRequest) (*ClientResponse, error)
	}

	client struct {
		metricsClient    metrics.Client
		logger           log.Logger
		cadenceClient    cclient.Client
		numWorkflows     dynamicconfig.IntPropertyFn
		rateLimiter      quotas.Limiter
		archiverProvider provider.ArchiverProvider
	}
)

const (
	signalTimeout = 300 * time.Millisecond

	tooManyRequestsErrMsg = "too many requests to archival workflow"
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
		metricsClient: metricsClient,
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

// ArchiveHistory starts an archival task
func (c *client) ArchiveHistory(ctx context.Context, request *ClientHistoryRequest) (resp *ClientResponse, err error) {
	c.metricsClient.IncCounter(metrics.ArchiverClientScope, metrics.CadenceRequests)
	taggedLogger := tagLoggerWithArchiveHistoryRequest(c.logger, *request.ArchiveHistoryRequest).WithTags(
		tag.ArchivalCallerServiceName(request.CallerService),
		tag.ArchivalArchiveAttemptedInline(request.AttemptArchiveInline),
	)
	archivedInline := false
	defer func() {
		if err != nil {
			resp = nil
			return
		}
		resp = &ClientResponse{
			ArchivedInline: archivedInline,
		}
	}()
	if request.AttemptArchiveInline {
		err = c.archiveHistoryInline(ctx, request, taggedLogger)
		if err != nil {
			err = c.sendArchiveHistorySignal(ctx, request.ArchiveHistoryRequest, taggedLogger)
			return
		}
		archivedInline = true
		return
	}
	err = c.sendArchiveHistorySignal(ctx, request.ArchiveHistoryRequest, taggedLogger)
	return
}

func (c *client) archiveHistoryInline(ctx context.Context, request *ClientHistoryRequest, taggedLogger log.Logger) (err error) {
	defer func() {
		if err != nil {
			c.metricsClient.IncCounter(metrics.ArchiverClientScope, metrics.ArchiverClientInlineArchiveFailureCount)
			taggedLogger.Info("failed to perform workflow history archival inline", tag.Error(err))
		}
	}()
	c.metricsClient.IncCounter(metrics.ArchiverClientScope, metrics.ArchiverClientInlineArchiveAttemptCount)
	URI, err := carchiver.NewURI(request.ArchiveHistoryRequest.URI)
	if err != nil {
		return err
	}

	historyArchiver, err := c.archiverProvider.GetHistoryArchiver(URI.Scheme(), request.CallerService)
	if err != nil {
		return err
	}

	return historyArchiver.Archive(ctx, URI, &carchiver.ArchiveHistoryRequest{
		ShardID:              request.ArchiveHistoryRequest.ShardID,
		DomainID:             request.ArchiveHistoryRequest.DomainID,
		DomainName:           request.ArchiveHistoryRequest.DomainName,
		WorkflowID:           request.ArchiveHistoryRequest.WorkflowID,
		RunID:                request.ArchiveHistoryRequest.RunID,
		EventStoreVersion:    request.ArchiveHistoryRequest.EventStoreVersion,
		BranchToken:          request.ArchiveHistoryRequest.BranchToken,
		NextEventID:          request.ArchiveHistoryRequest.NextEventID,
		CloseFailoverVersion: request.ArchiveHistoryRequest.CloseFailoverVersion,
	})
}

func (c *client) sendArchiveHistorySignal(ctx context.Context, request *ArchiveHistoryRequest, taggedLogger log.Logger) error {
	if ok := c.rateLimiter.Allow(); !ok {
		c.logger.Error(tooManyRequestsErrMsg)
		c.metricsClient.IncCounter(metrics.ArchiverClientScope, metrics.CadenceErrServiceBusyCounter)
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
	_, err := c.cadenceClient.SignalWithStartWorkflow(signalCtx, workflowID, archiveHistorySignalName, *request, workflowOptions, archiveHistoryWorkflowFnName, nil)
	if err != nil {
		taggedLogger = taggedLogger.WithTags(tag.WorkflowID(workflowID), tag.Error(err))
		taggedLogger.Error("failed to send signal to archival system workflow")
		c.metricsClient.IncCounter(metrics.ArchiverClientScope, metrics.ArchiverClientSendSignalFailureCount)
		return err
	}
	return nil
}
