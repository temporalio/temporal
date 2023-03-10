// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021 Datadog, Inc.

package temporaltest_test

import (
	"testing"
	"time"

	"context"
	"fmt"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"go.temporal.io/server/temporal/temporaltest"
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

	ts.NewWorkerWithOptions(
		"hello_world",
		func(registry worker.Registry) {
			RegisterWorkflowsAndActivities(registry)
		},
		worker.Options{
			MaxConcurrentActivityExecutionSize:      1,
			MaxConcurrentLocalActivityExecutionSize: 1,
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

func TestSearchAttributeCacheDisabled(t *testing.T) {
	// TODO(jlegrone) re-enable this test when advanced visibility is enabled in temporalite.
	t.Skip("This test case does not currently pass as of the 1.20 release")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ts := temporaltest.NewServer(temporaltest.WithT(t))

	testSearchAttr := "my-search-attr"

	// Create a search attribute
	_, err := ts.GetDefaultClient().OperatorService().AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{
		SearchAttributes: map[string]enums.IndexedValueType{
			testSearchAttr: enums.INDEXED_VALUE_TYPE_KEYWORD,
		},
		Namespace: ts.GetDefaultNamespace(),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Confirm it exists immediately
	resp, err := ts.GetDefaultClient().GetSearchAttributes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	saType, ok := resp.GetKeys()[testSearchAttr]
	if !ok {
		t.Fatalf("search attribute %q is missing", testSearchAttr)
	}
	if saType != enums.INDEXED_VALUE_TYPE_KEYWORD {
		t.Error("search attribute type does not match expected")
	}
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
