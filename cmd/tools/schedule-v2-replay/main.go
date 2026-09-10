package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/primitives"
	workerscheduler "go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type options struct {
	address           string
	namespace         string
	scheduleID        string
	runID             string
	historyOut        string
	timeout           time.Duration
	tls               bool
	serverName        string
	batch             bool
	allNamespaces     bool
	sampleSize        int
	cohortSize        int
	sampleSeed        string
	maxRuns           int
	maxRunAge         time.Duration
	maxScan           int
	maxHistoryEvents  int
	maxHistoryBytes   int64
	requestsPerSecond float64
	concurrency       int
	resume            bool
	sensitiveDataAck  bool
	collectorVersion  string
	serverVersion     string
	historyDir        string
	extractActions    string
	generateScenarios bool
	scenarioPrefix    string
}

func main() {
	opts, err := parseFlags(os.Args[1:])
	if err != nil {
		os.Exit(2)
	}
	if err := run(context.Background(), opts); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "schedule-v2-replay: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags(args []string) (options, error) {
	var opts options
	flags := flag.NewFlagSet("schedule-v2-replay", flag.ContinueOnError)
	flags.StringVar(&opts.address, "address", "localhost:7233", "Temporal frontend address")
	flags.StringVar(&opts.namespace, "namespace", "default", "Temporal namespace")
	flags.StringVar(&opts.scheduleID, "schedule-id", "", "schedule ID (required)")
	flags.StringVar(&opts.runID, "run-id", "", "legacy scheduler workflow run ID; defaults to the current run")
	flags.StringVar(&opts.historyOut, "history-out", "", "optional path for the downloaded JSON history (.gz is supported)")
	flags.DurationVar(&opts.timeout, "timeout", time.Minute, "timeout for API calls")
	flags.BoolVar(&opts.tls, "tls", false, "enable TLS")
	flags.StringVar(&opts.serverName, "tls-server-name", "", "TLS server name override")
	flags.BoolVar(&opts.batch, "batch", false, "replay a sample of V1 schedules")
	flags.BoolVar(&opts.allNamespaces, "all-namespaces", false, "sample schedules from every namespace (requires -batch)")
	flags.IntVar(&opts.sampleSize, "sample-size", 10, "maximum V1 schedules to replay per namespace in batch mode")
	flags.IntVar(&opts.cohortSize, "cohort-size", 2, "targeted examples per behavioral cohort in batch mode")
	flags.StringVar(&opts.sampleSeed, "sample-seed", "schedule-v2-replay", "deterministic batch sampling seed")
	flags.IntVar(&opts.maxRuns, "max-runs", 3, "maximum continue-as-new runs to collect per schedule")
	flags.DurationVar(&opts.maxRunAge, "max-run-age", 30*24*time.Hour, "maximum age of collected continue-as-new runs")
	flags.IntVar(&opts.maxScan, "max-scan", 0, "maximum schedules to inspect per namespace; defaults to 20 times sample-size")
	flags.IntVar(&opts.maxHistoryEvents, "max-history-events", 100000, "maximum events to collect per workflow run")
	flags.Int64Var(&opts.maxHistoryBytes, "max-history-bytes", 50<<20, "maximum protobuf bytes to collect per workflow run")
	flags.Float64Var(&opts.requestsPerSecond, "requests-per-second", 2, "maximum production API requests per second")
	flags.IntVar(&opts.concurrency, "concurrency", 2, "maximum namespaces collected concurrently")
	flags.BoolVar(&opts.resume, "resume", true, "resume collection from namespace manifests")
	flags.BoolVar(&opts.sensitiveDataAck, "acknowledge-sensitive-data", false, "confirm that the history directory is approved for raw production data")
	flags.StringVar(&opts.collectorVersion, "collector-version", currentBuildRevision(), "collector source revision recorded in manifests")
	flags.StringVar(&opts.historyDir, "history-dir", "schedule-v1-replay-histories", "batch history output directory")
	flags.StringVar(&opts.extractActions, "extract-action-executions", "", "print successful action workflow ID/run ID pairs from a saved V1 schedule history")
	flags.BoolVar(&opts.generateScenarios, "generate-scenarios", false, "create disposable V1 conformance scenarios and save their histories")
	flags.StringVar(&opts.scenarioPrefix, "scenario-prefix", "schedule-v1-conformance", "schedule ID prefix for generated scenarios")
	if err := flags.Parse(args); err != nil {
		return options{}, err
	}
	if opts.batch && opts.generateScenarios {
		return options{}, errors.New("-batch and -generate-scenarios are mutually exclusive")
	}
	if opts.batch {
		if opts.scheduleID != "" || opts.runID != "" || opts.historyOut != "" {
			return options{}, errors.New("-schedule-id, -run-id, and -history-out are single-schedule options")
		}
		if opts.sampleSize <= 0 {
			return options{}, errors.New("-sample-size must be greater than zero")
		}
		if opts.cohortSize < 0 || opts.maxRuns <= 0 || opts.maxRunAge <= 0 || opts.maxScan < 0 || opts.maxHistoryEvents <= 0 || opts.maxHistoryBytes <= 0 {
			return options{}, errors.New("batch limits must be positive; -cohort-size may be zero")
		}
		if opts.sampleSeed == "" || opts.requestsPerSecond <= 0 || opts.concurrency <= 0 {
			return options{}, errors.New("batch sampling and rate-limit options must be positive and non-empty")
		}
		if opts.historyDir == "" {
			return options{}, errors.New("-history-dir is required in batch mode")
		}
		if !opts.sensitiveDataAck {
			return options{}, errors.New("-acknowledge-sensitive-data is required in batch mode")
		}
	} else if opts.allNamespaces {
		return options{}, errors.New("-all-namespaces requires -batch")
	} else if opts.generateScenarios {
		if opts.historyDir == "" {
			return options{}, errors.New("-history-dir is required with -generate-scenarios")
		}
		if opts.scenarioPrefix == "" {
			return options{}, errors.New("-scenario-prefix is required with -generate-scenarios")
		}
	}
	return opts, nil
}

func run(parent context.Context, opts options) error {
	if opts.extractActions != "" {
		history, err := readHistoryFile(opts.extractActions)
		if err != nil {
			return err
		}
		executions, err := extractActionExecutions(history)
		if err != nil {
			return err
		}
		for _, execution := range executions {
			fmt.Printf("%s\t%s\n", execution.WorkflowID, execution.RunID)
		}
		return nil
	}
	if opts.batch {
		return runBatch(parent, opts)
	}
	if opts.generateScenarios {
		return generateScenarioFixtures(parent, opts)
	}
	if opts.scheduleID == "" {
		return errors.New("-schedule-id is required")
	}
	ctx, cancel := context.WithTimeout(parent, opts.timeout)
	defer cancel()

	clientOpts := client.Options{
		HostPort:  opts.address,
		Namespace: opts.namespace,
		Identity:  "schedule-v2-replay",
	}
	if opts.tls {
		clientOpts.ConnectionOptions.TLS = &tls.Config{ServerName: opts.serverName, MinVersion: tls.VersionTLS12}
	}
	if apiKey := os.Getenv("TEMPORAL_API_KEY"); apiKey != "" {
		clientOpts.Credentials = client.NewAPIKeyStaticCredentials(apiKey)
	}
	c, err := client.Dial(clientOpts)
	if err != nil {
		return fmt.Errorf("dial Temporal: %w", err)
	}
	defer c.Close()

	workflowID := primitives.ScheduleWorkflowIDPrefix + opts.scheduleID
	runID, err := resolveRunID(ctx, c, workflowID, opts.runID)
	if err != nil {
		return err
	}
	history, err := downloadHistory(ctx, c, workflowID, runID)
	if err != nil {
		return err
	}
	if opts.historyOut != "" {
		if err := writeHistory(opts.historyOut, history); err != nil {
			return err
		}
	}
	if err := replayHistory(history); err != nil {
		return fmt.Errorf("V1 replay failed: %w", err)
	}
	fmt.Printf("V1 replay passed: schedule=%q run=%q events=%d\n", opts.scheduleID, runID, len(history.Events))

	return nil
}

func readHistoryFile(path string) (*historypb.History, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open history: %w", err)
	}
	defer file.Close()
	var reader io.Reader = file
	if strings.HasSuffix(path, ".gz") {
		gzipReader, err := gzip.NewReader(file)
		if err != nil {
			return nil, fmt.Errorf("open compressed history: %w", err)
		}
		defer gzipReader.Close()
		reader = gzipReader
	}
	history, err := client.HistoryFromJSON(reader, client.HistoryJSONOptions{})
	if err != nil {
		return nil, fmt.Errorf("decode history: %w", err)
	}
	return history, nil
}

func runBatch(parent context.Context, opts options) error {
	return runCollectionBatch(parent, opts)
}

func dialClient(opts options, namespace string) (client.Client, error) {
	clientOpts := client.Options{
		HostPort:  opts.address,
		Namespace: namespace,
		Identity:  "schedule-v2-replay",
	}
	if opts.tls {
		clientOpts.ConnectionOptions.TLS = &tls.Config{ServerName: opts.serverName, MinVersion: tls.VersionTLS12}
	}
	if apiKey := os.Getenv("TEMPORAL_API_KEY"); apiKey != "" {
		clientOpts.Credentials = client.NewAPIKeyStaticCredentials(apiKey)
	}
	c, err := client.Dial(clientOpts)
	if err != nil {
		return nil, fmt.Errorf("dial Temporal namespace %q: %w", namespace, err)
	}
	return c, nil
}

func safePathComponent(value string) string {
	hash := sha256.Sum256([]byte(value))
	name := strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("._-", r) {
			return r
		}
		return '_'
	}, value)
	name = strings.Trim(name, ".")
	if len(name) > 80 {
		name = name[:80]
	}
	if name == "" {
		name = "schedule"
	}
	return fmt.Sprintf("%s-%x", name, hash[:6])
}

func resolveRunID(ctx context.Context, c client.Client, workflowID, runID string) (string, error) {
	resp, err := c.DescribeWorkflowExecution(ctx, workflowID, runID)
	if err != nil {
		return "", fmt.Errorf("describe legacy scheduler workflow: %w", err)
	}
	info := resp.GetWorkflowExecutionInfo()
	return info.GetExecution().GetRunId(), nil
}

func downloadHistory(ctx context.Context, c client.Client, workflowID, runID string) (*historypb.History, error) {
	iter := c.GetWorkflowHistory(ctx, workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	history := &historypb.History{}
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			return nil, fmt.Errorf("download workflow history: %w", err)
		}
		history.Events = append(history.Events, event)
	}
	if len(history.Events) == 0 {
		return nil, errors.New("downloaded workflow history is empty")
	}
	return history, nil
}

func replayHistory(history *historypb.History) error {
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(
		workerscheduler.SchedulerWorkflow,
		workflow.RegisterOptions{Name: workerscheduler.WorkflowType},
	)
	return replayer.ReplayWorkflowHistory(log.NewSdkLogger(log.NewTestLogger()), proto.CloneOf(history))
}

func writeHistory(path string, history *historypb.History) error {
	_, _, err := writeHistorySecure(path, history)
	return err
}

func writeHistorySecure(path string, history *historypb.History) (string, int64, error) {
	data, err := (protojson.MarshalOptions{Indent: "  ", UseProtoNames: true}).Marshal(history)
	if err != nil {
		return "", 0, fmt.Errorf("marshal history: %w", err)
	}
	if strings.HasSuffix(path, ".gz") {
		var compressed bytes.Buffer
		gz := gzip.NewWriter(&compressed)
		if _, err := gz.Write(data); err != nil {
			return "", 0, fmt.Errorf("compress history: %w", err)
		}
		if err := gz.Close(); err != nil {
			return "", 0, fmt.Errorf("compress history: %w", err)
		}
		data = compressed.Bytes()
	}
	if err := atomicWrite(path, data, 0o600); err != nil {
		return "", 0, err
	}
	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x", sum), int64(len(data)), nil
}

func atomicWrite(path string, data []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".schedule-v2-replay-*")
	if err != nil {
		return fmt.Errorf("create temporary output: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(mode); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("secure temporary output: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write temporary output: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync temporary output: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary output: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("replace output: %w", err)
	}
	return nil
}
