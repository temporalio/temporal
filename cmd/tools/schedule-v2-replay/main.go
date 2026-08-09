package main

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
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
	historyDir        string
	generateScenarios bool
	scenarioPrefix    string
}

type replayOutcome struct {
	isV1        bool
	runID       string
	historyPath string
	eventCount  int
	err         error
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
	flags.StringVar(&opts.historyDir, "history-dir", "schedule-v1-replay-histories", "batch history output directory")
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
		if opts.historyDir == "" {
			return options{}, errors.New("-history-dir is required in batch mode")
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

func runBatch(parent context.Context, opts options) error {
	namespaces := []string{opts.namespace}
	if opts.allNamespaces {
		c, err := dialClient(opts, opts.namespace)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(parent, opts.timeout)
		namespaces, err = listNamespaces(ctx, c)
		cancel()
		c.Close()
		if err != nil {
			return err
		}
	}
	if err := os.MkdirAll(opts.historyDir, 0o755); err != nil {
		return fmt.Errorf("create history directory: %w", err)
	}

	var failures int
	for _, namespace := range namespaces {
		failed, err := replayNamespaceSample(parent, opts, namespace)
		failures += failed
		if err != nil {
			failures++
			_, _ = fmt.Fprintf(os.Stderr, "namespace=%q: %v\n", namespace, err)
		}
	}
	if failures != 0 {
		return fmt.Errorf("batch replay completed with %d failure(s)", failures)
	}
	return nil
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

func listNamespaces(ctx context.Context, c client.Client) ([]string, error) {
	var namespaces []string
	var nextPageToken []byte
	for {
		response, err := c.WorkflowService().ListNamespaces(ctx, &workflowservice.ListNamespacesRequest{
			PageSize:      1000,
			NextPageToken: nextPageToken,
		})
		if err != nil {
			return nil, fmt.Errorf("list namespaces: %w", err)
		}
		for _, namespace := range response.GetNamespaces() {
			if name := namespace.GetNamespaceInfo().GetName(); name != "" {
				namespaces = append(namespaces, name)
			}
		}
		nextPageToken = response.GetNextPageToken()
		if len(nextPageToken) == 0 {
			break
		}
	}
	sort.Strings(namespaces)
	return namespaces, nil
}

func replayNamespaceSample(parent context.Context, opts options, namespace string) (int, error) {
	c, err := dialClient(opts, namespace)
	if err != nil {
		return 0, err
	}
	defer c.Close()

	var nextPageToken []byte
	var scanned, sampled, passed, failed int
	for sampled < opts.sampleSize {
		ctx, cancel := context.WithTimeout(parent, opts.timeout)
		response, err := c.WorkflowService().ListSchedules(ctx, &workflowservice.ListSchedulesRequest{
			Namespace:       namespace,
			MaximumPageSize: 100,
			NextPageToken:   nextPageToken,
		})
		cancel()
		if err != nil {
			return failed, fmt.Errorf("list schedules: %w", err)
		}
		for _, entry := range response.GetSchedules() {
			if sampled == opts.sampleSize {
				break
			}
			scanned++
			scheduleID := entry.GetScheduleId()
			outcome := replayScheduleSample(parent, c, opts, namespace, scheduleID)
			if outcome.isV1 {
				sampled++
			}
			if outcome.err != nil {
				failed++
				_, _ = fmt.Fprintf(os.Stderr, "namespace=%q schedule=%q run=%q: %v\n", namespace, scheduleID, outcome.runID, outcome.err)
				continue
			}
			if !outcome.isV1 {
				continue
			}
			passed++
			fmt.Printf("V1_REPLAY_PASS namespace=%q schedule=%q run=%q events=%d history=%q\n", namespace, scheduleID, outcome.runID, outcome.eventCount, outcome.historyPath)
		}
		nextPageToken = response.GetNextPageToken()
		if len(nextPageToken) == 0 {
			break
		}
	}
	fmt.Printf("SUMMARY namespace=%q scanned=%d sampled_v1=%d passed=%d failed=%d\n", namespace, scanned, sampled, passed, failed)
	return failed, nil
}

func replayScheduleSample(parent context.Context, c client.Client, opts options, namespace, scheduleID string) replayOutcome {
	ctx, cancel := context.WithTimeout(parent, opts.timeout)
	defer cancel()

	workflowID := primitives.ScheduleWorkflowIDPrefix + scheduleID
	runID, err := resolveRunID(ctx, c, workflowID, "")
	if err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return replayOutcome{}
		}
		return replayOutcome{err: err}
	}
	outcome := replayOutcome{
		isV1:        true,
		runID:       runID,
		historyPath: historyPath(opts.historyDir, namespace, scheduleID),
	}
	history, err := downloadHistory(ctx, c, workflowID, runID)
	if err == nil {
		err = writeHistory(outcome.historyPath, history)
	}
	if err == nil {
		err = replayHistory(history)
	}
	if err != nil {
		outcome.err = err
		return outcome
	}
	outcome.eventCount = len(history.Events)
	return outcome
}

func historyPath(root, namespace, scheduleID string) string {
	return filepath.Join(root, safePathComponent(namespace), safePathComponent(scheduleID)+".json.gz")
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
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create history directory: %w", err)
	}
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create history file: %w", err)
	}

	var writer interface {
		Write([]byte) (int, error)
	} = f
	var gz *gzip.Writer
	if len(path) >= 3 && path[len(path)-3:] == ".gz" {
		gz = gzip.NewWriter(f)
		writer = gz
	}
	data, err := (protojson.MarshalOptions{Indent: "  ", UseProtoNames: true}).Marshal(history)
	if err == nil {
		_, err = writer.Write(data)
	}
	if gz != nil {
		if closeErr := gz.Close(); err == nil {
			err = closeErr
		}
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("write history file: %w", err)
	}
	return nil
}
