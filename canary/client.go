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

package canary

import (
	"context"
	"time"

	"github.com/opentracing/opentracing-go"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/serviceerror"
	"go.temporal.io/temporal-proto/workflowservice"
	"go.temporal.io/temporal/activity"
	"go.temporal.io/temporal/client"
	"go.temporal.io/temporal/workflow"
)

// cadenceClient is an abstraction on top of
// the cadence library client that serves as
// a union of all the client interfaces that
// the library exposes
type cadenceClient struct {
	client.Client
	// domainClient only exposes domain API
	client.DomainClient
	// this is the service needed to start the workers
	Service workflowservice.WorkflowServiceClient
}

// createDomain creates a cadence domain with the given name and description
// if the domain already exist, this method silently returns success
func (client *cadenceClient) createDomain(name string, desc string, owner string, archivalStatus enums.ArchivalStatus) error {
	emitMetric := true
	retention := int32(workflowRetentionDays)
	if archivalStatus == enums.ArchivalStatusEnabled {
		retention = int32(0)
	}
	req := &workflowservice.RegisterDomainRequest{
		Name:                                   name,
		Description:                            desc,
		OwnerEmail:                             owner,
		WorkflowExecutionRetentionPeriodInDays: retention,
		EmitMetric:                             emitMetric,
		HistoryArchivalStatus:                  archivalStatus,
	}
	err := client.Register(context.Background(), req)
	if err != nil {
		if _, ok := err.(*serviceerror.DomainAlreadyExists); !ok {
			return err
		}
	}
	return nil
}

// newCadenceClient builds a cadenceClient from the runtimeContext
func newCadenceClient(domain string, runtime *RuntimeContext) (cadenceClient, error) {
	tracer := opentracing.GlobalTracer()
	cclient, err := client.NewClient(
		client.Options{
			HostPort:     runtime.hostPort,
			DomainName:   domain,
			MetricsScope: runtime.metrics,
			Tracer:       tracer,
		},
	)

	if err != nil {
		return cadenceClient{}, err
	}

	domainClient, err := client.NewDomainClient(
		client.Options{
			HostPort:     runtime.hostPort,
			MetricsScope: runtime.metrics,
			Tracer:       tracer,
		},
	)
	if err != nil {
		return cadenceClient{}, err
	}

	return cadenceClient{
		Client:       cclient,
		DomainClient: domainClient,
		Service:      runtime.service,
	}, nil
}

// newWorkflowOptions builds workflowOptions with defaults for everything except startToCloseTimeout
func newWorkflowOptions(id string, executionTimeout time.Duration) client.StartWorkflowOptions {
	return client.StartWorkflowOptions{
		ID:                              id,
		TaskList:                        taskListName,
		ExecutionStartToCloseTimeout:    executionTimeout,
		DecisionTaskStartToCloseTimeout: decisionTaskTimeout,
		WorkflowIDReusePolicy:           client.WorkflowIDReusePolicyAllowDuplicate,
	}
}

// newActivityOptions builds and returns activityOptions with reasonable defaults
func newActivityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		TaskList:               taskListName,
		StartToCloseTimeout:    activityTaskTimeout,
		ScheduleToStartTimeout: scheduleToStartTimeout,
		ScheduleToCloseTimeout: scheduleToStartTimeout + activityTaskTimeout,
	}
}

// newChildWorkflowOptions builds and returns childWorkflowOptions for given domain
func newChildWorkflowOptions(domain string, wfID string) workflow.ChildWorkflowOptions {
	return workflow.ChildWorkflowOptions{
		Domain:                       domain,
		WorkflowID:                   wfID,
		TaskList:                     taskListName,
		ExecutionStartToCloseTimeout: childWorkflowTimeout,
		TaskStartToCloseTimeout:      decisionTaskTimeout,
		WorkflowIDReusePolicy:        client.WorkflowIDReusePolicyAllowDuplicate,
	}
}

// registerWorkflow registers a workflow function with a given friendly name
func registerWorkflow(r registrar, workflowFunc interface{}, name string) {
	r.RegisterWorkflowWithOptions(workflowFunc, workflow.RegisterOptions{Name: name})
}

// registerActivity registers an activity function with a given friendly name
func registerActivity(r registrar, activityFunc interface{}, name string) {
	if name == "" {
		r.RegisterActivity(activityFunc)
	} else {
		r.RegisterActivityWithOptions(activityFunc, activity.RegisterOptions{Name: name})
	}
}
