package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "run":
		cmdRun(os.Args[2:])
	case "report":
		cmdReport(os.Args[2:])
	case "browse":
		cmdBrowse(os.Args[2:])
	default:
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `TemporalFS Research Agent Demo

Usage:
  research-agent-demo <command> [flags]

Commands:
  run      Run the demo (start workflows, show live dashboard)
  report   Generate HTML report from completed run
  browse   Browse a workflow's filesystem

Run 'research-agent-demo <command> -h' for command-specific help.
`)
}

func cmdRun(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	workflows := fs.Int("workflows", 200, "Number of research workflows to run")
	concurrency := fs.Int("concurrency", 50, "Max concurrent workflows")
	failureRate := fs.Float64("failure-rate", 1.0, "Failure rate multiplier (0=none, 2=double)")
	dataDir := fs.String("data-dir", "/tmp/tfs-demo", "PebbleDB data directory")
	seed := fs.Int64("seed", 0, "Random seed (0=random)")
	taskQueue := fs.String("task-queue", "research-demo", "Temporal task queue name")
	temporalAddr := fs.String("temporal-addr", "localhost:7233", "Temporal server address")
	noDashboard := fs.Bool("no-dashboard", false, "Disable live dashboard")
	_ = fs.Parse(args)

	// Set up context with signal handling.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nShutting down...")
		cancel()
	}()

	// Open shared PebbleDB.
	store, err := NewDemoStore(*dataDir)
	if err != nil {
		log.Fatalf("Failed to open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	// Connect to Temporal.
	c, err := sdkclient.Dial(sdkclient.Options{
		HostPort: *temporalAddr,
	})
	if err != nil {
		log.Fatalf("Failed to connect to Temporal: %v", err)
	}
	defer c.Close()

	// Start worker.
	activities := &Activities{baseStore: store.Base()}
	w := worker.New(c, *taskQueue, worker.Options{
		MaxConcurrentActivityExecutionSize: *concurrency,
	})
	w.RegisterWorkflow(ResearchWorkflow)
	w.RegisterActivity(activities)
	if err := w.Start(); err != nil {
		log.Fatalf("Failed to start worker: %v", err)
	}
	defer w.Stop()

	// Create runner.
	runner := NewRunner(c, store, RunConfig{
		Workflows:   *workflows,
		Concurrency: *concurrency,
		FailureRate: *failureRate,
		Seed:        *seed,
		TaskQueue:   *taskQueue,
	})

	// Start dashboard.
	if !*noDashboard {
		dash := NewDashboard(runner, *workflows)
		dash.Start()
		defer dash.Wait()
	}

	fmt.Printf("Starting %d research workflows (concurrency=%d, failure-rate=%.1f)\n",
		*workflows, *concurrency, *failureRate)
	fmt.Printf("Temporal UI: http://localhost:8233\n\n")

	// Run all workflows.
	if err := runner.Run(ctx); err != nil {
		log.Printf("Runner error: %v", err)
	}

	// Print final summary.
	fmt.Printf("\n\n%s=== Demo Complete ===%s\n", colorBold, colorReset)
	fmt.Printf("Workflows:  %d completed, %d failed\n",
		runner.stats.Completed.Load(), runner.stats.Failed.Load())
	fmt.Printf("Files:      %d created (%s)\n",
		runner.stats.FilesCreated.Load(), humanBytes(runner.stats.BytesWritten.Load()))
	fmt.Printf("Snapshots:  %d\n", runner.stats.Snapshots.Load())
	fmt.Printf("Retries:    %d\n", runner.stats.Retries.Load())
	fmt.Printf("\nGenerate report: go run . report --data-dir %s\n", *dataDir)
}

func cmdReport(args []string) {
	fs := flag.NewFlagSet("report", flag.ExitOnError)
	dataDir := fs.String("data-dir", "/tmp/tfs-demo", "PebbleDB data directory")
	output := fs.String("output", "demo-report.html", "Output HTML file")
	_ = fs.Parse(args)

	store, err := NewDemoStoreReadOnly(*dataDir)
	if err != nil {
		log.Fatalf("Failed to open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := generateHTMLReport(store, *output); err != nil {
		log.Fatalf("Failed to generate report: %v", err)
	}
	fmt.Printf("Report generated: %s\n", *output)
}

func cmdBrowse(args []string) {
	fs := flag.NewFlagSet("browse", flag.ExitOnError)
	dataDir := fs.String("data-dir", "/tmp/tfs-demo", "PebbleDB data directory")
	topic := fs.String("topic", "", "Topic slug to browse (required)")
	_ = fs.Parse(args)

	if *topic == "" {
		log.Fatal("--topic is required")
	}

	store, err := NewDemoStoreReadOnly(*dataDir)
	if err != nil {
		log.Fatalf("Failed to open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	browseWorkflow(store, *topic)
}
