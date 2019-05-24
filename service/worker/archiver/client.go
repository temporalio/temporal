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

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"github.com/uber/cadence/common/tokenbucket"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	cclient "go.uber.org/cadence/client"
)

type (
	// ArchiveRequest is request to Archive
	ArchiveRequest struct {
		ShardID              int
		DomainID             string
		DomainName           string
		WorkflowID           string
		RunID                string
		EventStoreVersion    int32
		BranchToken          []byte
		NextEventID          int64
		CloseFailoverVersion int64
	}

	// Client is used to archive workflow histories
	Client interface {
		Archive(*ArchiveRequest) error
	}

	client struct {
		metricsClient metrics.Client
		logger        log.Logger
		cadenceClient cclient.Client
		numWorkflows  dynamicconfig.IntPropertyFn
		rateLimiter   tokenbucket.TokenBucket
	}
)

const tooManyRequestsErrMsg = "Too many requests to archival workflow"

// NewClient creates a new Client
func NewClient(
	metricsClient metrics.Client,
	logger log.Logger,
	publicClient workflowserviceclient.Interface,
	numWorkflows dynamicconfig.IntPropertyFn,
	requestRPS dynamicconfig.IntPropertyFn,
) Client {
	return &client{
		metricsClient: metricsClient,
		logger:        logger,
		cadenceClient: cclient.NewClient(publicClient, common.SystemDomainName, &cclient.Options{}),
		numWorkflows:  numWorkflows,
		rateLimiter:   tokenbucket.NewDynamicTokenBucket(requestRPS, clock.NewRealTimeSource()),
	}
}

// Archive starts an archival task
func (c *client) Archive(request *ArchiveRequest) error {
	c.metricsClient.IncCounter(metrics.ArchiverClientScope, metrics.CadenceRequests)

	if ok, _ := c.rateLimiter.TryConsume(1); !ok {
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
	_, err := c.cadenceClient.SignalWithStartWorkflow(context.Background(), workflowID, signalName, *request, workflowOptions, archivalWorkflowFnName, nil)
	if err != nil {
		taggedLogger := tagLoggerWithRequest(c.logger, *request).WithTags(tag.WorkflowID(workflowID), tag.Error(err))
		taggedLogger.Error("failed to send signal to archival system workflow")
		c.metricsClient.IncCounter(metrics.ArchiverClientScope, metrics.CadenceFailures)
	}
	return err
}
