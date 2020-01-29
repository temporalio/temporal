// Copyright (c) 2019 Uber Technologies, Inc.
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

package replicator

import (
	"fmt"
	"sync/atomic"
	"time"

	commonproto "go.temporal.io/temporal-proto/common"

	"github.com/temporalio/temporal/.gen/proto/adminservice"
	"github.com/temporalio/temporal/client/admin"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/backoff"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/membership"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/service/frontend/adapter"
)

const (
	fetchTaskRequestTimeout                   = 10 * time.Second
	pollTimerJitterCoefficient                = 0.2
	pollIntervalSecs                          = 1
	taskProcessorErrorRetryWait               = time.Second
	taskProcessorErrorRetryBackoffCoefficient = 1
	taskProcessorErrorRetryMaxAttampts        = 5
)

func newDomainReplicationMessageProcessor(
	sourceCluster string,
	logger log.Logger,
	remotePeer admin.Client,
	metricsClient metrics.Client,
	domainReplicator DomainReplicator,
	hostInfo *membership.HostInfo,
	serviceResolver membership.ServiceResolver,
	domainReplicationQueue persistence.DomainReplicationQueue,
) *domainReplicationMessageProcessor {
	retryPolicy := backoff.NewExponentialRetryPolicy(taskProcessorErrorRetryWait)
	retryPolicy.SetBackoffCoefficient(taskProcessorErrorRetryBackoffCoefficient)
	retryPolicy.SetMaximumAttempts(taskProcessorErrorRetryMaxAttampts)

	return &domainReplicationMessageProcessor{
		hostInfo:               hostInfo,
		serviceResolver:        serviceResolver,
		status:                 common.DaemonStatusInitialized,
		sourceCluster:          sourceCluster,
		logger:                 logger,
		remotePeer:             remotePeer,
		domainReplicator:       domainReplicator,
		metricsClient:          metricsClient,
		retryPolicy:            retryPolicy,
		lastProcessedMessageID: -1,
		lastRetrievedMessageID: -1,
		done:                   make(chan struct{}),
		domainReplicationQueue: domainReplicationQueue,
	}
}

type (
	domainReplicationMessageProcessor struct {
		hostInfo               *membership.HostInfo
		serviceResolver        membership.ServiceResolver
		status                 int32
		sourceCluster          string
		logger                 log.Logger
		remotePeer             admin.Client
		domainReplicator       DomainReplicator
		metricsClient          metrics.Client
		retryPolicy            backoff.RetryPolicy
		lastProcessedMessageID int64
		lastRetrievedMessageID int64
		done                   chan struct{}
		domainReplicationQueue persistence.DomainReplicationQueue
	}
)

func (p *domainReplicationMessageProcessor) Start() {
	if !atomic.CompareAndSwapInt32(&p.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	go p.processorLoop()
}

func (p *domainReplicationMessageProcessor) processorLoop() {
	timer := time.NewTimer(getWaitDuration())

	for {
		select {
		case <-timer.C:
			p.getAndHandleDomainReplicationTasks()
			timer.Reset(getWaitDuration())
		case <-p.done:
			timer.Stop()
			return
		}
	}
}

func (p *domainReplicationMessageProcessor) getAndHandleDomainReplicationTasks() {
	// The following is a best effort to make sure only one worker is processing tasks for a
	// particular source cluster. When the ring is under reconfiguration, it is possible that
	// for a small period of time two or more workers think they are the owner and try to execute
	// the processing logic. This will not result in correctness issue as domain replication task
	// processing will be protected by version check.
	info, err := p.serviceResolver.Lookup(p.sourceCluster)
	if err != nil {
		p.logger.Info("Failed to lookup host info. Skip current run.")
		return
	}

	if info.Identity() != p.hostInfo.Identity() {
		p.logger.Info(fmt.Sprintf("Worker not responsible for source cluster %v.", p.sourceCluster))
		return
	}

	ctx, cancel := createContextWithCancel(fetchTaskRequestTimeout)
	request := &adminservice.GetDomainReplicationMessagesRequest{
		LastRetrievedMessageId: p.lastRetrievedMessageID,
		LastProcessedMessageId: p.lastProcessedMessageID,
	}
	response, err := p.remotePeer.GetDomainReplicationMessages(ctx, request)
	defer cancel()

	if err != nil {
		p.logger.Error("Failed to get replication tasks", tag.Error(err))
		return
	}

	p.logger.Debug("Successfully fetched domain replication tasks.", tag.Counter(len(response.Messages.ReplicationTasks)))

	for taskIndex := range response.Messages.ReplicationTasks {
		task := response.Messages.ReplicationTasks[taskIndex]
		err := backoff.Retry(func() error {
			return p.handleDomainReplicationTask(task)
		}, p.retryPolicy, isTransientRetryableError)

		if err != nil {
			p.metricsClient.IncCounter(metrics.DomainReplicationTaskScope, metrics.ReplicatorFailures)
			p.logger.Error("Failed to apply domain replication tasks", tag.Error(err))

			dlqErr := backoff.Retry(func() error {
				return p.putDomainReplicationTaskToDLQ(task)
			}, p.retryPolicy, isTransientRetryableError)
			if dlqErr != nil {
				p.logger.Error("Failed to put replication tasks to DLQ", tag.Error(dlqErr))
				p.metricsClient.IncCounter(metrics.DomainReplicationTaskScope, metrics.ReplicatorFailures)
				return
			}
		}
	}

	p.lastProcessedMessageID = response.Messages.GetLastRetrievedMessageId()
	p.lastRetrievedMessageID = response.Messages.GetLastRetrievedMessageId()
}

func (p *domainReplicationMessageProcessor) putDomainReplicationTaskToDLQ(
	task *commonproto.ReplicationTask,
) error {

	return p.domainReplicationQueue.PublishToDLQ(adapter.ToThriftReplicationTask(task))
}

func (p *domainReplicationMessageProcessor) handleDomainReplicationTask(
	task *commonproto.ReplicationTask,
) error {
	p.metricsClient.IncCounter(metrics.DomainReplicationTaskScope, metrics.ReplicatorMessages)
	sw := p.metricsClient.StartTimer(metrics.DomainReplicationTaskScope, metrics.ReplicatorLatency)
	defer sw.Stop()

	return p.domainReplicator.HandleReceivingTask(task.GetDomainTaskAttributes())
}

func (p *domainReplicationMessageProcessor) Stop() {
	close(p.done)
}

func getWaitDuration() time.Duration {
	return backoff.JitDuration(time.Duration(pollIntervalSecs)*time.Second, pollTimerJitterCoefficient)
}
