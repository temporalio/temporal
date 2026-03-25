package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	sdkclient "go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
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
	workflows := fs.Int("workflows", 200, "Number of research workflows to run (ignored in continuous mode)")
	concurrency := fs.Int("concurrency", 50, "Max concurrent workflows")
	failureRate := fs.Float64("failure-rate", 1.0, "Failure rate multiplier (0=none, 2=double)")
	dataDir := fs.String("data-dir", "/tmp/tfs-demo", "PebbleDB data directory")
	seed := fs.Int64("seed", 0, "Random seed (0=random)")
	taskQueue := fs.String("task-queue", "", "Temporal task queue name (default: research-demo-<timestamp>)")
	temporalAddr := fs.String("temporal-addr", "localhost:7233", "Temporal server address")
	noDashboard := fs.Bool("no-dashboard", false, "Disable live dashboard")
	continuous := fs.Bool("continuous", false, "Run continuously until Ctrl+C, then generate report")
	reportOutput := fs.String("report", "", "Auto-generate HTML report on completion (path)")
	_ = fs.Parse(args)

	// Set up context with signal handling.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nShutting down gracefully... (waiting for in-flight workflows)")
		cancel()
	}()

	// Redirect all logs to a file so the dashboard isn't buried.
	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		log.Fatalf("Failed to create data dir: %v", err)
	}
	logPath := filepath.Join(*dataDir, "demo.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		log.Fatalf("Failed to create log file: %v", err)
	}
	defer logFile.Close()
	log.SetOutput(logFile)
	sdkLogger := sdklog.NewStructuredLogger(slog.New(slog.NewTextHandler(logFile, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	})))

	// Open shared PebbleDB.
	store, err := NewDemoStore(*dataDir)
	if err != nil {
		log.Fatalf("Failed to open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	// Connect to Temporal.
	c, err := sdkclient.Dial(sdkclient.Options{
		HostPort: *temporalAddr,
		Logger:   sdkLogger,
	})
	if err != nil {
		log.Fatalf("Failed to connect to Temporal: %v", err)
	}
	defer c.Close()

	// Use a unique task queue per run to avoid stale activity task interference
	// from previous runs on the same Temporal server.
	if *taskQueue == "" {
		*taskQueue = fmt.Sprintf("research-demo-%d", time.Now().UnixMilli())
	}

	// Create runner first so activities can share its stats.
	runner := NewRunner(c, store, RunConfig{
		Workflows:   *workflows,
		Concurrency: *concurrency,
		FailureRate: *failureRate,
		Seed:        *seed,
		TaskQueue:   *taskQueue,
		Continuous:  *continuous,
	})

	// Start worker with shared stats for real-time retry tracking.
	activities := &Activities{baseStore: store.Base(), stats: &runner.stats, eventCh: runner.EventCh}
	w := worker.New(c, *taskQueue, worker.Options{
		MaxConcurrentActivityExecutionSize: *concurrency,
	})
	w.RegisterWorkflow(ResearchWorkflow)
	w.RegisterActivity(activities)
	if err := w.Start(); err != nil {
		log.Fatalf("Failed to start worker: %v", err)
	}
	defer w.Stop()

	// Dashboard total: 0 means continuous (dashboard shows "∞").
	dashTotal := *workflows
	if *continuous {
		dashTotal = 0
	}

	// Start dashboard or drain events to prevent channel blocking.
	if !*noDashboard {
		dash := NewDashboard(runner, dashTotal)
		dash.Start()
		defer dash.Wait()
	} else {
		go func() {
			for range runner.EventCh {
			}
		}()
	}

	if *continuous {
		fmt.Printf("Running continuously (concurrency=%d, failure-rate=%.1f) — press Ctrl+C to stop\n",
			*concurrency, *failureRate)
	} else {
		fmt.Printf("Starting %d research workflows (concurrency=%d, failure-rate=%.1f)\n",
			*workflows, *concurrency, *failureRate)
	}
	fmt.Printf("Temporal UI: http://localhost:8233\n")
	fmt.Printf("Logs: %s\n\n", logPath)

	// Open Temporal UI in browser for continuous mode.
	if *continuous {
		openBrowser("http://localhost:8233")
	}

	// Run workflows.
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

	// Auto-generate report if requested or in continuous mode.
	reportPath := *reportOutput
	if reportPath == "" && *continuous {
		reportPath = filepath.Join(*dataDir, "report.html")
	}
	if reportPath != "" {
		fmt.Printf("\nGenerating report...\n")
		if err := generateHTMLReport(store, reportPath); err != nil {
			log.Printf("Failed to generate report: %v", err)
		} else {
			fmt.Printf("Report generated: %s\n", reportPath)
			openBrowser(reportPath)
		}
	} else {
		fmt.Printf("\nGenerate report: go run . report --data-dir %s\n", *dataDir)
	}
}

// openBrowser opens a URL or file in the default browser.
func openBrowser(url string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", url)
	case "linux":
		cmd = exec.Command("xdg-open", url)
	default:
		return
	}
	_ = cmd.Start()
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
