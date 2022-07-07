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

package replicator

import (
	"context"
	"sync/atomic"
	"time"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/adminservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/rpc"
)

const (
	fetchTaskRequestTimeout                   = 10 * time.Second
	pollTimerJitterCoefficient                = 0.2
	pollIntervalSecs                          = 1
	taskProcessorErrorRetryWait               = time.Second
	taskProcessorErrorRetryBackoffCoefficient = 1
	taskProcessorErrorRetryMaxAttampts        = 5
)

func newNamespaceReplicationMessageProcessor(
	currentCluster string,
	sourceCluster string,
	logger log.Logger,
	remotePeer adminservice.AdminServiceClient,
	metricsClient metrics.Client,
	taskExecutor namespace.ReplicationTaskExecutor,
	hostInfo *membership.HostInfo,
	serviceResolver membership.ServiceResolver,
	namespaceReplicationQueue persistence.NamespaceReplicationQueue,
) *namespaceReplicationMessageProcessor {
	retryPolicy := backoff.NewExponentialRetryPolicy(taskProcessorErrorRetryWait)
	retryPolicy.SetBackoffCoefficient(taskProcessorErrorRetryBackoffCoefficient)
	retryPolicy.SetMaximumAttempts(taskProcessorErrorRetryMaxAttampts)

	return &namespaceReplicationMessageProcessor{
		hostInfo:                  hostInfo,
		serviceResolver:           serviceResolver,
		status:                    common.DaemonStatusInitialized,
		currentCluster:            currentCluster,
		sourceCluster:             sourceCluster,
		logger:                    logger,
		remotePeer:                remotePeer,
		taskExecutor:              taskExecutor,
		metricsClient:             metricsClient,
		retryPolicy:               retryPolicy,
		lastProcessedMessageID:    -1,
		lastRetrievedMessageID:    -1,
		done:                      make(chan struct{}),
		namespaceReplicationQueue: namespaceReplicationQueue,
	}
}

type (
	namespaceReplicationMessageProcessor struct {
		hostInfo                  *membership.HostInfo
		serviceResolver           membership.ServiceResolver
		status                    int32
		currentCluster            string
		sourceCluster             string
		logger                    log.Logger
		remotePeer                adminservice.AdminServiceClient
		taskExecutor              namespace.ReplicationTaskExecutor
		metricsClient             metrics.Client
		retryPolicy               backoff.RetryPolicy
		lastProcessedMessageID    int64
		lastRetrievedMessageID    int64
		done                      chan struct{}
		namespaceReplicationQueue persistence.NamespaceReplicationQueue
	}
)

func (p *namespaceReplicationMessageProcessor) Start() {
	if !atomic.CompareAndSwapInt32(&p.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	go p.processorLoop()
}

func (p *namespaceReplicationMessageProcessor) processorLoop() {
	timer := time.NewTimer(getWaitDuration())

	for {
		select {
		case <-timer.C:
			p.getAndHandleNamespaceReplicationTasks()
			timer.Reset(getWaitDuration())
		case <-p.done:
			timer.Stop()
			return
		}
	}
}

func (p *namespaceReplicationMessageProcessor) getAndHandleNamespaceReplicationTasks() {
	// The following is a best effort to make sure only one worker is processing tasks for a
	// particular source cluster. When the ring is under reconfiguration, it is possible that
	// for a small period of time two or more workers think they are the owner and try to execute
	// the processing logic. This will not result in correctness issue as namespace replication task
	// processing will be protected by version check.
	info, err := p.serviceResolver.Lookup(p.sourceCluster)
	if err != nil {
		p.logger.Info("Failed to lookup host info. Skip current run")
		return
	}

	if info.Identity() != p.hostInfo.Identity() {
		p.logger.Debug("Worker not responsible for source cluster", tag.ClusterName(p.sourceCluster))
		return
	}

	ctx, cancel := rpc.NewContextWithTimeoutAndHeaders(fetchTaskRequestTimeout)
	request := &adminservice.GetNamespaceReplicationMessagesRequest{
		ClusterName:            p.currentCluster,
		LastRetrievedMessageId: p.lastRetrievedMessageID,
		LastProcessedMessageId: p.lastProcessedMessageID,
	}
	response, err := p.remotePeer.GetNamespaceReplicationMessages(ctx, request)
	defer cancel()

	if err != nil {
		p.logger.Error("Failed to get replication tasks", tag.Error(err))
		return
	}

	p.logger.Debug("Successfully fetched namespace replication tasks", tag.Counter(len(response.Messages.ReplicationTasks)))

	for taskIndex := range response.Messages.ReplicationTasks {
		task := response.Messages.ReplicationTasks[taskIndex]
		err := backoff.ThrottleRetry(func() error {
			return p.handleNamespaceReplicationTask(task)
		}, p.retryPolicy, isTransientRetryableError)

		if err != nil {
			p.metricsClient.IncCounter(metrics.NamespaceReplicationTaskScope, metrics.ReplicatorFailures)
			p.logger.Error("Failed to apply namespace replication tasks", tag.Error(err))

			dlqErr := backoff.ThrottleRetry(func() error {
				return p.putNamespaceReplicationTaskToDLQ(task)
			}, p.retryPolicy, isTransientRetryableError)
			if dlqErr != nil {
				p.logger.Error("Failed to put replication tasks to DLQ", tag.Error(dlqErr))
				p.metricsClient.IncCounter(metrics.NamespaceReplicationTaskScope, metrics.ReplicatorDLQFailures)
				return
			}
		}
	}

	p.lastProcessedMessageID = response.Messages.GetLastRetrievedMessageId()
	p.lastRetrievedMessageID = response.Messages.GetLastRetrievedMessageId()
}

func (p *namespaceReplicationMessageProcessor) putNamespaceReplicationTaskToDLQ(
	task *replicationspb.ReplicationTask,
) error {

	namespaceAttribute := task.GetNamespaceTaskAttributes()
	if namespaceAttribute == nil {
		return serviceerror.NewUnavailable(
			"Namespace replication task does not set namespace task attribute",
		)
	}
	p.metricsClient.Scope(
		metrics.NamespaceReplicationTaskScope,
		metrics.NamespaceTag(namespaceAttribute.GetInfo().GetName()),
	).IncCounter(metrics.NamespaceReplicationEnqueueDLQCount)
	return p.namespaceReplicationQueue.PublishToDLQ(context.TODO(), task)
}

func (p *namespaceReplicationMessageProcessor) handleNamespaceReplicationTask(
	task *replicationspb.ReplicationTask,
) error {
	p.metricsClient.IncCounter(metrics.NamespaceReplicationTaskScope, metrics.ReplicatorMessages)
	sw := p.metricsClient.StartTimer(metrics.NamespaceReplicationTaskScope, metrics.ReplicatorLatency)
	defer sw.Stop()

	return p.taskExecutor.Execute(context.TODO(), task.GetNamespaceTaskAttributes())
}

func (p *namespaceReplicationMessageProcessor) Stop() {
	close(p.done)
}

func getWaitDuration() time.Duration {
	return backoff.JitDuration(time.Duration(pollIntervalSecs)*time.Second, pollTimerJitterCoefficient)
}

func isTransientRetryableError(err error) bool {
	switch err.(type) {
	case *serviceerror.InvalidArgument:
		return false
	default:
		return true
	}
}
