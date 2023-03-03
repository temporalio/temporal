// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021 Datadog, Inc.

package temporaltest_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/server/temporaltest"
	"go.temporal.io/server/temporaltest/internal/examples/helloworld"
)

// to be used in example code
var t *testing.T

func ExampleNewServer_testWorker() {
	// Create test Temporal server and client
	ts := temporaltest.NewServer(temporaltest.WithT(t))
	c := ts.DefaultClient()

	// Register a new worker on the `hello_world` task queue
	ts.NewWorker("hello_world", func(registry worker.Registry) {
		helloworld.RegisterWorkflowsAndActivities(registry)
	})

	// Start test workflow
	wfr, err := c.ExecuteWorkflow(
		context.Background(),
		client.StartWorkflowOptions{TaskQueue: "hello_world"},
		helloworld.Greet,
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
		helloworld.RegisterWorkflowsAndActivities(registry)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wfr, err := ts.DefaultClient().ExecuteWorkflow(
		ctx,
		client.StartWorkflowOptions{TaskQueue: "hello_world"},
		helloworld.Greet,
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
			helloworld.RegisterWorkflowsAndActivities(registry)
		},
		worker.Options{
			MaxConcurrentActivityExecutionSize:      1,
			MaxConcurrentLocalActivityExecutionSize: 1,
		},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wfr, err := ts.DefaultClient().ExecuteWorkflow(
		ctx,
		client.StartWorkflowOptions{TaskQueue: "hello_world"},
		helloworld.Greet,
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
		helloworld.RegisterWorkflowsAndActivities(registry)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wfr, err := ts.DefaultClient().ExecuteWorkflow(
		ctx,
		client.StartWorkflowOptions{TaskQueue: "hello_world"},
		helloworld.Greet,
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

func TestClientWithDefaultInterceptor(t *testing.T) {
	var opts client.Options
	opts.Interceptors = append(opts.Interceptors, helloworld.NewTestInterceptor())
	ts := temporaltest.NewServer(
		temporaltest.WithT(t),
		temporaltest.WithBaseClientOptions(opts),
	)

	ts.NewWorker(
		"hello_world",
		func(registry worker.Registry) {
			helloworld.RegisterWorkflowsAndActivities(registry)
		},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wfr, err := ts.DefaultClient().ExecuteWorkflow(
		ctx,
		client.StartWorkflowOptions{TaskQueue: "hello_world"},
		helloworld.Greet,
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ts := temporaltest.NewServer(temporaltest.WithT(t))

	// Create a search attribute
	_, err := ts.DefaultClient().OperatorService().AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{
		SearchAttributes: map[string]enums.IndexedValueType{
			"my-search-attr": enums.INDEXED_VALUE_TYPE_TEXT,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Confirm it exists immediately
	resp, err := ts.DefaultClient().GetSearchAttributes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Keys["my-search-attr"] != enums.INDEXED_VALUE_TYPE_TEXT {
		t.Fatal("search attribute not found")
	}
}

func BenchmarkRunWorkflow(b *testing.B) {
	ts := temporaltest.NewServer()
	defer ts.Stop()

	ts.NewWorker("hello_world", func(registry worker.Registry) {
		helloworld.RegisterWorkflowsAndActivities(registry)
	})
	c := ts.DefaultClient()

	for i := 0; i < b.N; i++ {
		func(b *testing.B) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			wfr, err := c.ExecuteWorkflow(
				ctx,
				client.StartWorkflowOptions{TaskQueue: "hello_world"},
				helloworld.Greet,
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
