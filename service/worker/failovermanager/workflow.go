// Copyright (c) 2017-2020 Uber Technologies Inc.
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

package failovermanager

import (
	"context"
	"errors"
	"strings"
	"time"

	"go.uber.org/cadence"
	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/workflow"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client/frontend"
	"github.com/uber/cadence/common"
)

const (
	failoverManagerContextKey = "failoverManagerContext"
	// TaskListName tasklist
	TaskListName = "cadence-sys-failoverManager-tasklist"
	// WorkflowTypeName workflow type name
	WorkflowTypeName = "cadence-sys-failoverManager-workflow"
	// WorkflowID will be reused to ensure only one workflow running
	WorkflowID             = "cadence-failover-manager"
	failoverActivityName   = "cadence-sys-failover-activity"
	getDomainsActivityName = "cadence-sys-getDomains-activity"

	defaultBatchFailoverSize              = 10
	defaultBatchFailoverWaitTimeInSeconds = 10

	errMsgParamsIsNil                 = "params is nil"
	errMsgTargetClusterIsEmpty        = "targetCluster is empty"
	errMsgSourceClusterIsEmpty        = "sourceCluster is empty"
	errMsgTargetClusterIsSameAsSource = "targetCluster is same as sourceCluster"

	// QueryType for failover workflow
	QueryType = "state"
	// PauseSignal signal name for pause
	PauseSignal = "pause"
	// ResumeSignal signal name for resume
	ResumeSignal = "resume"

	// workflow states for query

	// WorkflowRunning state
	WorkflowRunning = "running"
	// WorkflowPaused state
	WorkflowPaused = "paused"
	// WorkflowCompleted state
	WorkflowCompleted = "complete"
	// WorkflowAborted state
	WorkflowAborted = "aborted"
)

type (
	// FailoverParams is the arg for failoverWorkflow
	FailoverParams struct {
		// TargetCluster is the destination of failover
		TargetCluster string
		// SourceCluster is from which cluster the domains are active before failover
		SourceCluster string
		// BatchFailoverSize is number of domains to failover in one batch
		BatchFailoverSize int
		// BatchFailoverWaitTimeInSeconds is the waiting time between batch failover
		BatchFailoverWaitTimeInSeconds int
		// Domains candidates to be failover
		Domains []string
	}

	// FailoverResult is workflow result
	FailoverResult struct {
		SuccessDomains []string
		FailedDomains  []string
	}

	// GetDomainsActivityParams params for activity
	GetDomainsActivityParams struct {
		TargetCluster string
		SourceCluster string
		Domains       []string
	}

	// FailoverActivityParams params for activity
	FailoverActivityParams struct {
		Domains       []string
		TargetCluster string
	}

	// FailoverActivityResult result for failover activity
	FailoverActivityResult struct {
		SuccessDomains []string
		FailedDomains  []string
	}

	// QueryResult for failover progress
	QueryResult struct {
		TotalDomains   int
		Success        int
		Failed         int
		State          string
		TargetCluster  string
		SourceCluster  string
		SuccessDomains []string // SuccessDomains are guaranteed succeed processed
		FailedDomains  []string // FailedDomains contains false positive
	}
)

func init() {
	workflow.RegisterWithOptions(FailoverWorkflow, workflow.RegisterOptions{Name: WorkflowTypeName})
	activity.RegisterWithOptions(FailoverActivity, activity.RegisterOptions{Name: failoverActivityName})
	activity.RegisterWithOptions(GetDomainsActivity, activity.RegisterOptions{Name: getDomainsActivityName})
}

// FailoverWorkflow is the workflow that managed failover all domains with IsManagedByCadence=true
func FailoverWorkflow(ctx workflow.Context, params *FailoverParams) (*FailoverResult, error) {
	err := validateParams(params)
	if err != nil {
		return nil, err
	}

	// define query properties
	var failedDomains []string
	var successDomains []string
	var totalNumOfDomains int
	wfState := WorkflowRunning
	err = workflow.SetQueryHandler(ctx, QueryType, func(input []byte) (*QueryResult, error) {
		return &QueryResult{
			TotalDomains:   totalNumOfDomains,
			Success:        len(successDomains),
			Failed:         len(failedDomains),
			State:          wfState,
			TargetCluster:  params.TargetCluster,
			SourceCluster:  params.SourceCluster,
			SuccessDomains: successDomains,
			FailedDomains:  failedDomains,
		}, nil
	})
	if err != nil {
		return nil, err
	}

	// get target domains
	ao := workflow.WithActivityOptions(ctx, getGetDomainsActivityOptions())
	getDomainsParams := &GetDomainsActivityParams{
		TargetCluster: params.TargetCluster,
		SourceCluster: params.SourceCluster,
		Domains:       params.Domains,
	}
	var domains []string
	err = workflow.ExecuteActivity(ao, GetDomainsActivity, getDomainsParams).Get(ctx, &domains)
	if err != nil {
		return nil, err
	}
	totalNumOfDomains = len(domains)

	// failover in batch
	ao = workflow.WithActivityOptions(ctx, getFailoverActivityOptions())
	batchSize := params.BatchFailoverSize
	times := len(domains)/batchSize + 1

	pauseCh := workflow.GetSignalChannel(ctx, PauseSignal)
	resumeCh := workflow.GetSignalChannel(ctx, ResumeSignal)
	var shouldPause bool

	for i := 0; i < times; i++ {
		// check if need to pause
		shouldPause = pauseCh.ReceiveAsync(nil)
		if shouldPause {
			wfState = WorkflowPaused
			resumeCh.Receive(ctx, nil)
		}
		wfState = WorkflowRunning

		failoverActivityParams := &FailoverActivityParams{
			Domains:       domains[i*batchSize : common.MinInt((i+1)*batchSize, totalNumOfDomains)],
			TargetCluster: params.TargetCluster,
		}
		var actResult FailoverActivityResult
		err = workflow.ExecuteActivity(ao, FailoverActivity, failoverActivityParams).Get(ctx, &actResult)
		if err != nil {
			// Domains in failed activity can be either failovered or not, but we treated them as failed.
			// This makes the query result for FailedDomains contains false positive results.
			failedDomains = append(failedDomains, failoverActivityParams.Domains...)
		} else {
			successDomains = append(successDomains, actResult.SuccessDomains...)
			failedDomains = append(failedDomains, actResult.FailedDomains...)
		}

		workflow.Sleep(ctx, time.Duration(params.BatchFailoverWaitTimeInSeconds)*time.Second)
	}

	wfState = WorkflowCompleted
	return &FailoverResult{
		SuccessDomains: successDomains,
		FailedDomains:  failedDomains,
	}, nil
}

func getGetDomainsActivityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		ScheduleToStartTimeout: 10 * time.Second,
		StartToCloseTimeout:    20 * time.Second,
		RetryPolicy: &cadence.RetryPolicy{
			InitialInterval:    2 * time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    1 * time.Minute,
			ExpirationInterval: 10 * time.Minute,
			NonRetriableErrorReasons: []string{
				errMsgParamsIsNil,
				errMsgTargetClusterIsEmpty,
				errMsgSourceClusterIsEmpty,
				errMsgTargetClusterIsSameAsSource},
		},
	}
}

func getFailoverActivityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		ScheduleToStartTimeout: 10 * time.Second,
		StartToCloseTimeout:    10 * time.Second,
	}
}

func validateParams(params *FailoverParams) error {
	if params == nil {
		return errors.New(errMsgParamsIsNil)
	}
	if params.BatchFailoverSize <= 0 {
		params.BatchFailoverSize = defaultBatchFailoverSize
	}
	if params.BatchFailoverWaitTimeInSeconds <= 0 {
		params.BatchFailoverWaitTimeInSeconds = defaultBatchFailoverWaitTimeInSeconds
	}
	return validateTargetAndSourceCluster(params.TargetCluster, params.SourceCluster)
}

// GetDomainsActivity activity def
func GetDomainsActivity(ctx context.Context, params *GetDomainsActivityParams) ([]string, error) {
	err := validateGetDomainsActivityParams(params)
	if err != nil {
		return nil, err
	}
	domains, err := getAllDomains(ctx, params.Domains)
	if err != nil {
		return nil, err
	}
	var res []string
	for _, domain := range domains {
		if shouldFailover(domain, params.SourceCluster) {
			domainName := domain.GetDomainInfo().GetName()
			res = append(res, domainName)
		}
	}
	return res, nil
}

func validateGetDomainsActivityParams(params *GetDomainsActivityParams) error {
	if params == nil {
		return errors.New(errMsgParamsIsNil)
	}
	return validateTargetAndSourceCluster(params.TargetCluster, params.SourceCluster)
}

func validateTargetAndSourceCluster(targetCluster, sourceCluster string) error {
	if len(targetCluster) == 0 {
		return errors.New(errMsgTargetClusterIsEmpty)
	}
	if len(sourceCluster) == 0 {
		return errors.New(errMsgSourceClusterIsEmpty)
	}
	if sourceCluster == targetCluster {
		return errors.New(errMsgTargetClusterIsSameAsSource)
	}
	return nil
}

func shouldFailover(domain *shared.DescribeDomainResponse, sourceCluster string) bool {
	if !domain.GetIsGlobalDomain() {
		return false
	}
	currentActiveCluster := domain.ReplicationConfiguration.GetActiveClusterName()
	isDomainTarget := currentActiveCluster == sourceCluster
	return isDomainTarget && isDomainFailoverManagedByCadence(domain)
}

func isDomainFailoverManagedByCadence(domain *shared.DescribeDomainResponse) bool {
	domainData := domain.DomainInfo.GetData()
	return strings.ToLower(strings.TrimSpace(domainData[common.DomainDataKeyForManagedFailover])) == "true"
}

func getClient(ctx context.Context) frontend.Client {
	manager := ctx.Value(failoverManagerContextKey).(*FailoverManager)
	feClient := manager.clientBean.GetFrontendClient()
	return feClient
}

func getAllDomains(ctx context.Context, targetDomains []string) ([]*shared.DescribeDomainResponse, error) {
	feClient := getClient(ctx)
	var res []*shared.DescribeDomainResponse

	isTargetDomainsProvided := len(targetDomains) > 0
	targetDomainsSet := make(map[string]struct{})
	if isTargetDomainsProvided {
		for _, domain := range targetDomains {
			targetDomainsSet[domain] = struct{}{}
		}
	}

	pagesize := int32(200)
	var token []byte
	for more := true; more; more = len(token) > 0 {
		listRequest := &shared.ListDomainsRequest{
			PageSize:      common.Int32Ptr(pagesize),
			NextPageToken: token,
		}
		listResp, err := feClient.ListDomains(ctx, listRequest)
		if err != nil {
			return nil, err
		}
		token = listResp.GetNextPageToken()

		if isTargetDomainsProvided {
			for _, domain := range listResp.GetDomains() {
				if _, ok := targetDomainsSet[domain.DomainInfo.GetName()]; ok {
					res = append(res, domain)
				}
			}
		} else {
			res = append(res, listResp.GetDomains()...)
		}

		activity.RecordHeartbeat(ctx, len(res))
	}
	return res, nil
}

// FailoverActivity activity def
func FailoverActivity(ctx context.Context, params *FailoverActivityParams) (*FailoverActivityResult, error) {
	feClient := getClient(ctx)
	domains := params.Domains
	var successDomains []string
	var failedDomains []string
	for _, domain := range domains {
		replicationConfig := &shared.DomainReplicationConfiguration{
			ActiveClusterName: common.StringPtr(params.TargetCluster),
		}
		updateRequest := &shared.UpdateDomainRequest{
			Name:                     common.StringPtr(domain),
			ReplicationConfiguration: replicationConfig,
		}
		_, err := feClient.UpdateDomain(ctx, updateRequest)
		if err != nil {
			failedDomains = append(failedDomains, domain)
		} else {
			successDomains = append(successDomains, domain)
		}
	}
	return &FailoverActivityResult{
		SuccessDomains: successDomains,
		FailedDomains:  failedDomains,
	}, nil
}
