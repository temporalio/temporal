package parentclosepolicy

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	sdkclient "go.temporal.io/temporal/client"

	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/metrics"
)

type (

	// Client is used to send request to processor workflow
	Client interface {
		SendParentClosePolicyRequest(Request) error
	}

	clientImpl struct {
		metricsClient  metrics.Client
		logger         log.Logger
		temporalClient sdkclient.Client
		numWorkflows   int
	}
)

var _ Client = (*clientImpl)(nil)

const (
	signalTimeout    = 400 * time.Millisecond
	workflowIDPrefix = "parent-close-policy-workflow"
)

// NewClient creates a new Client
func NewClient(
	metricsClient metrics.Client,
	logger log.Logger,
	publicClient sdkclient.Client,
	numWorkflows int,
) Client {
	return &clientImpl{
		metricsClient:  metricsClient,
		logger:         logger,
		temporalClient: publicClient,
		numWorkflows:   numWorkflows,
	}
}

func (c *clientImpl) SendParentClosePolicyRequest(request Request) error {
	randomID := rand.Intn(c.numWorkflows)
	workflowID := fmt.Sprintf("%v-%v", workflowIDPrefix, randomID)
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                              workflowID,
		TaskList:                        processorTaskListName,
		ExecutionStartToCloseTimeout:    infiniteDuration,
		DecisionTaskStartToCloseTimeout: time.Minute,
		WorkflowIDReusePolicy:           sdkclient.WorkflowIDReusePolicyAllowDuplicate,
	}
	signalCtx, cancel := context.WithTimeout(context.Background(), signalTimeout)
	defer cancel()
	_, err := c.temporalClient.SignalWithStartWorkflow(signalCtx, workflowID, processorChannelName, request, workflowOptions, processorWFTypeName, nil)
	return err
}
