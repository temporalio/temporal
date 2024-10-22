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

package respondworkflowtaskcompleted

import (
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/history/workflow"
)

type (
	workflowSizeLimits struct {
		blobSizeLimitWarn              int
		blobSizeLimitError             int
		memoSizeLimitWarn              int
		memoSizeLimitError             int
		numPendingChildExecutionsLimit int
		numPendingActivitiesLimit      int
		numPendingSignalsLimit         int
		numPendingCancelsRequestLimit  int
	}

	workflowSizeChecker struct {
		workflowSizeLimits

		mutableState              workflow.MutableState
		searchAttributesValidator *searchattribute.Validator
		metricsHandler            metrics.Handler
		logger                    log.Logger
	}
)

func newWorkflowSizeChecker(
	limits workflowSizeLimits,
	mutableState workflow.MutableState,
	searchAttributesValidator *searchattribute.Validator,
	metricsHandler metrics.Handler,
	logger log.Logger,
) *workflowSizeChecker {
	return &workflowSizeChecker{
		workflowSizeLimits:        limits,
		mutableState:              mutableState,
		searchAttributesValidator: searchAttributesValidator,
		metricsHandler:            metricsHandler,
		logger:                    logger,
	}
}

func (c *workflowSizeChecker) checkIfPayloadSizeExceedsLimit(
	commandTypeTag metrics.Tag,
	payloadSize int,
	message string,
) error {

	executionInfo := c.mutableState.GetExecutionInfo()
	executionState := c.mutableState.GetExecutionState()
	err := common.CheckEventBlobSizeLimit(
		payloadSize,
		c.blobSizeLimitWarn,
		c.blobSizeLimitError,
		executionInfo.NamespaceId,
		executionInfo.WorkflowId,
		executionState.RunId,
		c.metricsHandler.WithTags(commandTypeTag),
		c.logger,
		tag.BlobSizeViolationOperation(commandTypeTag.Value()),
	)
	if err != nil {
		return fmt.Errorf("%s", message) // nolint:err113
	}
	return nil
}

func (c *workflowSizeChecker) checkIfMemoSizeExceedsLimit(
	memo *commonpb.Memo,
	commandTypeTag metrics.Tag,
	message string,
) error {
	metrics.MemoSize.With(c.metricsHandler).Record(
		int64(memo.Size()),
		commandTypeTag)

	executionInfo := c.mutableState.GetExecutionInfo()
	executionState := c.mutableState.GetExecutionState()
	err := common.CheckEventBlobSizeLimit(
		memo.Size(),
		c.memoSizeLimitWarn,
		c.memoSizeLimitError,
		executionInfo.NamespaceId,
		executionInfo.WorkflowId,
		executionState.RunId,
		c.metricsHandler.WithTags(commandTypeTag),
		c.logger,
		tag.BlobSizeViolationOperation(commandTypeTag.Value()),
	)
	if err != nil {
		return fmt.Errorf("%s", message) // nolint:err113
	}
	return nil
}

func withinLimit(value int, limit int) bool {
	if limit <= 0 {
		// limit not defined
		return true
	}
	return value < limit
}

func (c *workflowSizeChecker) checkCountConstraint(
	numPending int,
	errLimit int,
	metricName string,
	resourceName string,
) error {
	key := c.mutableState.GetWorkflowKey()
	logger := log.With(
		c.logger,
		tag.WorkflowNamespaceID(key.NamespaceID),
		tag.WorkflowID(key.WorkflowID),
		tag.WorkflowRunID(key.RunID),
	)

	if withinLimit(numPending, errLimit) {
		return nil
	}
	c.metricsHandler.Counter(metricName).Record(1)
	// nolint:err113
	err := fmt.Errorf(
		"the number of %s, %d, has reached the per-workflow limit of %d",
		resourceName,
		numPending,
		errLimit,
	)
	logger.Error(err.Error(), tag.Error(err))
	return err
}

const (
	PendingChildWorkflowExecutionsDescription = "pending child workflow executions"
	PendingActivitiesDescription              = "pending activities"
	PendingCancelRequestsDescription          = "pending requests to cancel external workflows"
	PendingSignalsDescription                 = "pending signals to external workflows"
)

func (c *workflowSizeChecker) checkIfNumChildWorkflowsExceedsLimit() error {
	return c.checkCountConstraint(
		len(c.mutableState.GetPendingChildExecutionInfos()),
		c.numPendingChildExecutionsLimit,
		metrics.TooManyPendingChildWorkflows.Name(),
		PendingChildWorkflowExecutionsDescription,
	)
}

func (c *workflowSizeChecker) checkIfNumPendingActivitiesExceedsLimit() error {
	return c.checkCountConstraint(
		len(c.mutableState.GetPendingActivityInfos()),
		c.numPendingActivitiesLimit,
		metrics.TooManyPendingActivities.Name(),
		PendingActivitiesDescription,
	)
}

func (c *workflowSizeChecker) checkIfNumPendingCancelRequestsExceedsLimit() error {
	return c.checkCountConstraint(
		len(c.mutableState.GetPendingRequestCancelExternalInfos()),
		c.numPendingCancelsRequestLimit,
		metrics.TooManyPendingCancelRequests.Name(),
		PendingCancelRequestsDescription,
	)
}

func (c *workflowSizeChecker) checkIfNumPendingSignalsExceedsLimit() error {
	return c.checkCountConstraint(
		len(c.mutableState.GetPendingSignalExternalInfos()),
		c.numPendingSignalsLimit,
		metrics.TooManyPendingSignalsToExternalWorkflows.Name(),
		PendingSignalsDescription,
	)
}

func (c *workflowSizeChecker) checkIfSearchAttributesSizeExceedsLimit(
	searchAttributes *commonpb.SearchAttributes,
	namespaceName namespace.Name,
	commandTypeTag metrics.Tag,
) error {
	metrics.SearchAttributesSize.With(c.metricsHandler).Record(
		int64(searchAttributes.Size()),
		commandTypeTag)
	err := c.searchAttributesValidator.ValidateSize(searchAttributes, namespaceName.String())
	if err != nil {
		c.logger.Warn(
			"Search attributes size exceeds limits. Fail workflow.",
			tag.Error(err),
			tag.WorkflowNamespace(namespaceName.String()),
		)
	}
	return err
}
