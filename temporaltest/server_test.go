// The MIT License
//
// Copyright (c) 2021 Datadog, Inc.
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

package temporaltest_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/grpc/codes"

	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/temporal"
	"go.temporal.io/server/temporaltest"
)

// to be used in example code
var t *testing.T

func ExampleNewServer() {
	// Create test Temporal server and client
	ts := temporaltest.NewServer(temporaltest.WithT(t))
	c := ts.GetDefaultClient()

	// Register a new worker on the `hello_world` task queue
	ts.NewWorker("hello_world", func(registry worker.Registry) {
		RegisterWorkflowsAndActivities(registry)
	})

	// Start test workflow
	wfr, err := c.ExecuteWorkflow(
		context.Background(),
		client.StartWorkflowOptions{TaskQueue: "hello_world"},
		Greet,
		"world",
	)
	if err != nil {
		t.Fatal(err)
	}

	// Get workflow result
	var result string
	if err := wfr.Get(context.Background(), &result); err != nil {
		t.Fatal(err)
	}

	// Print result
	fmt.Println(result)
	// Output: Hello world
}

func TestNewServer(t *testing.T) {
	ts := temporaltest.NewServer(temporaltest.WithT(t))

	ts.NewWorker("hello_world", func(registry worker.Registry) {
		RegisterWorkflowsAndActivities(registry)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wfr, err := ts.GetDefaultClient().ExecuteWorkflow(
		ctx,
		client.StartWorkflowOptions{TaskQueue: "hello_world"},
		Greet,
		"world",
	)
	if err != nil {
		t.Fatal(err)
	}

	var result string
	if err := wfr.Get(ctx, &result); err != nil {
		t.Fatal(err)
	}

	if result != "Hello world" {
		t.Fatalf("unexpected result: %q", result)
	}
}

func TestNewWorkerWithOptions(t *testing.T) {
	ts := temporaltest.NewServer(temporaltest.WithT(t))
	c := ts.GetDefaultClient()

	ts.NewWorkerWithOptions(
		"hello_world",
		func(registry worker.Registry) {
			RegisterWorkflowsAndActivities(registry)
		},
		worker.Options{
			MaxConcurrentActivityExecutionSize:      1,
			MaxConcurrentLocalActivityExecutionSize: 1,
			// We will later verify this option was set by checking the identity of the task queue poller.
			Identity: "test-worker-with-options",
		},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Verify that workflows still run to completion
	wfr, err := c.ExecuteWorkflow(
		ctx,
		client.StartWorkflowOptions{TaskQueue: "hello_world"},
		Greet,
		"world",
	)
	if err != nil {
		t.Fatal(err)
	}

	var result string
	if err := wfr.Get(ctx, &result); err != nil {
		t.Fatal(err)
	}
	if result != "Hello world" {
		t.Fatalf("unexpected result: %q", result)
	}

	// Verify that the Identity worker option was set.
	resp, err := c.DescribeTaskQueue(ctx, "hello_world", enums.TASK_QUEUE_TYPE_WORKFLOW)
	if err != nil {
		t.Fatal(err)
	}
	poller := resp.GetPollers()[0]
	assert.Equal(t, "test-worker-with-options", poller.GetIdentity())
}

func TestDefaultWorkerOptions(t *testing.T) {
	ts := temporaltest.NewServer(
		temporaltest.WithT(t),
		temporaltest.WithBaseWorkerOptions(
			worker.Options{
				MaxConcurrentActivityExecutionSize:      1,
				MaxConcurrentLocalActivityExecutionSize: 1,
			},
		),
	)

	ts.NewWorker("hello_world", func(registry worker.Registry) {
		RegisterWorkflowsAndActivities(registry)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wfr, err := ts.GetDefaultClient().ExecuteWorkflow(
		ctx,
		client.StartWorkflowOptions{TaskQueue: "hello_world"},
		Greet,
		"world",
	)
	if err != nil {
		t.Fatal(err)
	}

	var result string
	if err := wfr.Get(ctx, &result); err != nil {
		t.Fatal(err)
	}

	if result != "Hello world" {
		t.Fatalf("unexpected result: %q", result)
	}
}

type denyAllClaimMapper struct{}

func (denyAllClaimMapper) GetClaims(*authorization.AuthInfo) (*authorization.Claims, error) {
	// Return claims that have no permissions within the cluster.
	return &authorization.Claims{
		Subject:    "test-identity",
		System:     authorization.RoleUndefined,
		Namespaces: nil,
		Extensions: nil,
	}, nil
}

func TestBaseServerOptions(t *testing.T) {
	// This test verifies that we can set custom claim mappers and authorizers
	// with BaseServerOptions.
	ts := temporaltest.NewServer(
		temporaltest.WithT(t),
		temporaltest.WithBaseServerOptions(
			temporal.WithClaimMapper(func(cfg *config.Config) authorization.ClaimMapper {
				return denyAllClaimMapper{}
			}),
			temporal.WithAuthorizer(authorization.NewDefaultAuthorizer()),
		),
	)

	_, err := ts.GetDefaultClient().ExecuteWorkflow(
		context.Background(),
		client.StartWorkflowOptions{},
		"test-workflow",
	)
	if err == nil {
		t.Fatal("err must be non-nil")
	}

	permissionDeniedErr := &serviceerror.PermissionDenied{}
	if !errors.As(err, &permissionDeniedErr) {
		t.Errorf("expected error %T, got %T", permissionDeniedErr, err)
	}
	assert.Equal(t, codes.PermissionDenied.String(), permissionDeniedErr.Status().Code().String())
}

func TestClientWithCustomInterceptor(t *testing.T) {
	var opts client.Options
	opts.Interceptors = append(opts.Interceptors, NewTestInterceptor())
	ts := temporaltest.NewServer(
		temporaltest.WithT(t),
		temporaltest.WithBaseClientOptions(opts),
	)

	ts.NewWorker(
		"hello_world",
		func(registry worker.Registry) {
			RegisterWorkflowsAndActivities(registry)
		},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wfr, err := ts.GetDefaultClient().ExecuteWorkflow(
		ctx,
		client.StartWorkflowOptions{TaskQueue: "hello_world"},
		Greet,
		"world",
	)
	if err != nil {
		t.Fatal(err)
	}

	var result string
	if err := wfr.Get(ctx, &result); err != nil {
		t.Fatal(err)
	}

	if result != "Hello world" {
		t.Fatalf("unexpected result: %q", result)
	}
}

func TestSearchAttributeRegistration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ts := temporaltest.NewServer(temporaltest.WithT(t))
	c := ts.GetDefaultClient()

	testSearchAttr := "MySearchAttr"

	// Create a search attribute
	if _, err := ts.GetDefaultClient().OperatorService().AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{
		SearchAttributes: map[string]enums.IndexedValueType{
			testSearchAttr: enums.INDEXED_VALUE_TYPE_KEYWORD,
		},
		Namespace: ts.GetDefaultNamespace(),
	}); err != nil {
		t.Fatal(err)
	}
	// Confirm search attribute is registered immediately
	// TODO(jlegrone): investigate why custom search attribute missing here while setting it from workflow succeeds.
	//resp, err := c.GetSearchAttributes(ctx)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//saType, ok := resp.GetKeys()[testSearchAttr]
	//if !ok {
	//	t.Fatalf("search attribute %q is missing from %v", testSearchAttr, resp.GetKeys())
	//}
	//if saType != enums.INDEXED_VALUE_TYPE_KEYWORD {
	//	t.Error("search attribute type does not match expected")
	//}

	// Run a workflow that sets the custom search attribute
	ts.NewWorker("test", func(registry worker.Registry) {
		registry.RegisterWorkflow(SearchAttrWorkflow)
	})
	wfr, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                       "search-attr-test",
		TaskQueue:                "test",
		WorkflowExecutionTimeout: 10 * time.Second,
	}, SearchAttrWorkflow, testSearchAttr)
	if err != nil {
		t.Fatal(err)
	}
	// Wait for workflow to complete
	if err := wfr.Get(ctx, nil); err != nil {
		t.Fatal(err)
	}

	// Wait a bit longer for the workflow to be indexed. This usually isn't necessary,
	// but helps avoid test flakiness.
	assert.Eventually(t, func() bool {
		// Confirm workflow has search attribute and shows up in custom list query
		listFilter := fmt.Sprintf("%s=%q", testSearchAttr, "foo")
		workflowList, err := c.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: ts.GetDefaultNamespace(),
			Query:     listFilter,
		})
		if err != nil {
			t.Fatal(err)
		}
		if numExecutions := len(workflowList.GetExecutions()); numExecutions != 1 {
			t.Logf("Expected list filter %q to return one workflow, got %d", listFilter, numExecutions)
			return false
		}

		searchAttrPayload, ok := workflowList.GetExecutions()[0].GetSearchAttributes().GetIndexedFields()[testSearchAttr]
		if !ok {
			t.Fatal("Workflow missing test search attr")
		}
		var searchAttrValue string
		if err := converter.GetDefaultDataConverter().FromPayload(searchAttrPayload, &searchAttrValue); err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, "foo", searchAttrValue)

		return true
	}, 30*time.Second, 100*time.Millisecond)
}

func BenchmarkRunWorkflow(b *testing.B) {
	ts := temporaltest.NewServer()
	defer ts.Stop()

	ts.NewWorker("hello_world", func(registry worker.Registry) {
		RegisterWorkflowsAndActivities(registry)
	})
	c := ts.GetDefaultClient()

	for i := 0; i < b.N; i++ {
		func(b *testing.B) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			wfr, err := c.ExecuteWorkflow(
				ctx,
				client.StartWorkflowOptions{TaskQueue: "hello_world"},
				Greet,
				"world",
			)
			if err != nil {
				b.Fatal(err)
			}

			if err := wfr.Get(ctx, nil); err != nil {
				b.Fatal(err)
			}
		}(b)
	}
}

func SearchAttrWorkflow(ctx workflow.Context, searchAttr string) error {
	return workflow.UpsertSearchAttributes(ctx, map[string]interface{}{
		searchAttr: "foo",
	})
}

// Example workflow/activity

// Greet implements a Temporal workflow that returns a salutation for a given subject.
func Greet(ctx workflow.Context, subject string) (string, error) {
	var greeting string
	if err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, workflow.ActivityOptions{ScheduleToCloseTimeout: time.Second}),
		PickGreeting,
	).Get(ctx, &greeting); err != nil {
		return "", err
	}

	return fmt.Sprintf("%s %s", greeting, subject), nil
}

// PickGreeting is a Temporal activity that returns some greeting text.
func PickGreeting(ctx context.Context) (string, error) {
	return "Hello", nil
}

func HandleIntercept(ctx context.Context) (string, error) {
	return "Ok", nil
}

func RegisterWorkflowsAndActivities(r worker.Registry) {
	r.RegisterWorkflow(Greet)
	r.RegisterActivity(PickGreeting)
	r.RegisterActivityWithOptions(HandleIntercept, activity.RegisterOptions{Name: "HandleIntercept"})
}

// Example interceptor

var _ interceptor.Interceptor = &Interceptor{}

type Interceptor struct {
	interceptor.InterceptorBase
}

type WorkflowInterceptor struct {
	interceptor.WorkflowInboundInterceptorBase
}

func NewTestInterceptor() *Interceptor {
	return &Interceptor{}
}

func (i *Interceptor) InterceptClient(next interceptor.ClientOutboundInterceptor) interceptor.ClientOutboundInterceptor {
	return i.InterceptorBase.InterceptClient(next)
}

func (i *Interceptor) InterceptWorkflow(ctx workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &WorkflowInterceptor{
		WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{
			Next: next,
		},
	}
}

func (i *WorkflowInterceptor) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	return i.Next.Init(outbound)
}

func (i *WorkflowInterceptor) ExecuteWorkflow(ctx workflow.Context, in *interceptor.ExecuteWorkflowInput) (interface{}, error) {
	version := workflow.GetVersion(ctx, "version", workflow.DefaultVersion, 1)
	var err error

	if version != workflow.DefaultVersion {
		var vpt string
		err = workflow.ExecuteLocalActivity(
			workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{ScheduleToCloseTimeout: time.Second}),
			"HandleIntercept",
		).Get(ctx, &vpt)

		if err != nil {
			return nil, err
		}
	}

	return i.Next.ExecuteWorkflow(ctx, in)
}
