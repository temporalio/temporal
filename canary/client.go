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

package canary

import (
	"context"
	"time"

	"github.com/opentracing/opentracing-go"
	namespacepb "go.temporal.io/temporal-proto/namespace"
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
	// namespaceClient only exposes namespace API
	client.NamespaceClient
	// this is the service needed to start the workers
	Service workflowservice.WorkflowServiceClient
}

// createNamespace creates a cadence namespace with the given name and description
// if the namespace already exist, this method silently returns success
func (client *cadenceClient) createNamespace(name string, desc string, owner string, archivalStatus namespacepb.ArchivalStatus) error {
	emitMetric := true
	isGlobalNamespace := false
	retention := int32(workflowRetentionDays)
	if archivalStatus == namespacepb.ArchivalStatus_Enabled {
		retention = int32(0)
	}
	req := &workflowservice.RegisterNamespaceRequest{
		Name:                                   name,
		Description:                            desc,
		OwnerEmail:                             owner,
		WorkflowExecutionRetentionPeriodInDays: retention,
		EmitMetric:                             emitMetric,
		HistoryArchivalStatus:                  archivalStatus,
		VisibilityArchivalStatus:               archivalStatus,
		IsGlobalNamespace:                      isGlobalNamespace,
	}
	err := client.Register(context.Background(), req)
	if err != nil {
		if _, ok := err.(*serviceerror.NamespaceAlreadyExists); !ok {
			return err
		}
	}
	return nil
}

// newCadenceClient builds a cadenceClient from the runtimeContext
func newCadenceClient(namespace string, runtime *RuntimeContext) (cadenceClient, error) {
	tracer := opentracing.GlobalTracer()
	cclient, err := client.NewClient(
		client.Options{
			HostPort:     runtime.hostPort,
			Namespace:    namespace,
			MetricsScope: runtime.metrics,
			Tracer:       tracer,
			Logger:       runtime.logger,
		},
	)

	if err != nil {
		return cadenceClient{}, err
	}

	namespaceClient, err := client.NewNamespaceClient(
		client.Options{
			HostPort:     runtime.hostPort,
			MetricsScope: runtime.metrics,
			Tracer:       tracer,
			Logger:       runtime.logger,
		},
	)
	if err != nil {
		return cadenceClient{}, err
	}

	return cadenceClient{
		Client:          cclient,
		NamespaceClient: namespaceClient,
		Service:         runtime.service,
	}, nil
}

// newWorkflowOptions builds workflowOptions with defaults for everything except startToCloseTimeout
func newWorkflowOptions(id string, executionTimeout time.Duration) client.StartWorkflowOptions {
	return client.StartWorkflowOptions{
		ID:                    id,
		TaskList:              taskListName,
		WorkflowRunTimeout:    executionTimeout,
		WorkflowTaskTimeout:   decisionTaskTimeout,
		WorkflowIDReusePolicy: client.WorkflowIDReusePolicyAllowDuplicate,
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

// newChildWorkflowOptions builds and returns childWorkflowOptions for given namespace
func newChildWorkflowOptions(namespace string, wfID string) workflow.ChildWorkflowOptions {
	return workflow.ChildWorkflowOptions{
		Namespace:             namespace,
		WorkflowID:            wfID,
		TaskList:              taskListName,
		WorkflowRunTimeout:    childWorkflowTimeout,
		WorkflowTaskTimeout:   decisionTaskTimeout,
		WorkflowIDReusePolicy: client.WorkflowIDReusePolicyAllowDuplicate,
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
