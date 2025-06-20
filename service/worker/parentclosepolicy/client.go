//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination client_mock.go

package parentclosepolicy

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/sdk"
)

type (

	// Client is used to send request to processor workflow
	Client interface {
		SendParentClosePolicyRequest(context.Context, Request) error
	}

	clientImpl struct {
		metricsHandler   metrics.Handler
		logger           log.Logger
		sdkClientFactory sdk.ClientFactory
		numWorkflows     int
	}
)

var _ Client = (*clientImpl)(nil)

const (
	signalTimeout         = 400 * time.Millisecond
	workflowIDPrefix      = "parent-close-policy-workflow"
	workflowTaskTimeout   = time.Minute
	workflowIDReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
)

// NewClient creates a new Client
func NewClient(
	metricsHandler metrics.Handler,
	logger log.Logger,
	sdkClientFactory sdk.ClientFactory,
	numWorkflows int,
) Client {
	return &clientImpl{
		metricsHandler:   metricsHandler,
		logger:           logger,
		sdkClientFactory: sdkClientFactory,
		numWorkflows:     numWorkflows,
	}
}

func (c *clientImpl) SendParentClosePolicyRequest(ctx context.Context, request Request) error {
	workflowID := getWorkflowID(c.numWorkflows)
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                    workflowID,
		TaskQueue:             processorTaskQueueName,
		WorkflowTaskTimeout:   workflowTaskTimeout,
		WorkflowIDReusePolicy: workflowIDReusePolicy,
	}
	signalCtx, cancel := context.WithTimeout(ctx, signalTimeout)
	defer cancel()

	sdkClient := c.sdkClientFactory.GetSystemClient()
	_, err := sdkClient.SignalWithStartWorkflow(signalCtx, workflowID, processorChannelName, request, workflowOptions, processorWFTypeName, nil)
	return err
}

func getWorkflowID(numWorkflows int) string {
	return fmt.Sprintf("%v-%v", workflowIDPrefix, rand.Intn(numWorkflows))
}
