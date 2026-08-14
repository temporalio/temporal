package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/primitives"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const collectionManifestVersion = 1

type collectionManifest struct {
	Version          int              `json:"version"`
	Namespace        string           `json:"namespace"`
	Seed             string           `json:"seed"`
	CapturedAt       time.Time        `json:"capturedAt"`
	ServerVersion    string           `json:"serverVersion,omitempty"`
	CollectorVersion string           `json:"collectorVersion,omitempty"`
	ListedPopulation int              `json:"listedPopulation"`
	LastCandidateKey string           `json:"lastCandidateKey,omitempty"`
	Inspected        int              `json:"inspected"`
	V1Inspected      int              `json:"v1Inspected"`
	BaseSelected     int              `json:"baseSelected"`
	SampleSize       int              `json:"sampleSize"`
	CohortSize       int              `json:"cohortSize"`
	MaxRuns          int              `json:"maxRuns"`
	MaxRunAge        time.Duration    `json:"maxRunAge"`
	MaxHistoryEvents int              `json:"maxHistoryEvents"`
	MaxHistoryBytes  int64            `json:"maxHistoryBytes"`
	Cases            []collectionCase `json:"cases"`
}

type collectionCase struct {
	ScheduleID string    `json:"scheduleId"`
	RunID      string    `json:"runId"`
	History    string    `json:"history,omitempty"`
	Checksum   string    `json:"sha256,omitempty"`
	Events     int       `json:"events,omitempty"`
	Bytes      int64     `json:"bytes,omitempty"`
	CapturedAt time.Time `json:"capturedAt"`
	Cohorts    []string  `json:"cohorts,omitempty"`
	Status     string    `json:"status"`
	Error      string    `json:"error,omitempty"`
}

type scheduleCandidate struct {
	scheduleID string
	key        string
}

type historyLimitError struct {
	events int
	bytes  int64
}

func (e *historyLimitError) Error() string {
	return fmt.Sprintf("history exceeds collection limit at %d events and %d protobuf bytes", e.events, e.bytes)
}

func runCollectionBatch(parent context.Context, opts options) error {
	if opts.maxScan == 0 {
		opts.maxScan = opts.sampleSize * 20
	}
	if err := ensureSecureDirectory(opts.historyDir); err != nil {
		return err
	}

	limiter := rate.NewLimiter(rate.Limit(opts.requestsPerSecond), max(1, opts.concurrency))
	namespaces := []string{opts.namespace}
	if opts.allNamespaces {
		c, err := dialClient(opts, opts.namespace)
		if err != nil {
			return err
		}
		namespaces, err = listNamespacesLimited(parent, c, opts, limiter)
		c.Close()
		if err != nil {
			return err
		}
	}
	systemClient, err := dialClient(opts, opts.namespace)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, opts.timeout)
	systemInfo, systemInfoErr := retryAPI(ctx, limiter, func() (*workflowservice.GetSystemInfoResponse, error) {
		return systemClient.WorkflowService().GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{})
	})
	cancel()
	systemClient.Close()
	if systemInfoErr != nil {
		return fmt.Errorf("get server version: %w", systemInfoErr)
	}
	opts.serverVersion = systemInfo.GetServerVersion()

	semaphore := make(chan struct{}, opts.concurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var failures int
	for _, namespace := range namespaces {
		namespace := namespace
		wg.Add(1)
		go func() {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()
			failed, err := collectNamespace(parent, opts, namespace, limiter)
			mu.Lock()
			defer mu.Unlock()
			failures += failed
			if err != nil {
				failures++
				_, _ = fmt.Fprintf(os.Stderr, "namespace=%q: %v\n", namespace, err)
			}
		}()
	}
	wg.Wait()
	if failures != 0 {
		return fmt.Errorf("collection completed with %d failure(s); completed histories remain replayable", failures)
	}
	return nil
}

func listNamespacesLimited(parent context.Context, c client.Client, opts options, limiter *rate.Limiter) ([]string, error) {
	var namespaces []string
	var nextPageToken []byte
	for {
		ctx, cancel := context.WithTimeout(parent, opts.timeout)
		response, err := retryAPI(ctx, limiter, func() (*workflowservice.ListNamespacesResponse, error) {
			return c.WorkflowService().ListNamespaces(ctx, &workflowservice.ListNamespacesRequest{
				PageSize: 100, NextPageToken: nextPageToken,
			})
		})
		cancel()
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
			sort.Strings(namespaces)
			return namespaces, nil
		}
	}
}

func collectNamespace(parent context.Context, opts options, namespace string, limiter *rate.Limiter) (int, error) {
	c, err := dialClient(opts, namespace)
	if err != nil {
		return 0, err
	}
	defer c.Close()

	candidates, err := listScheduleCandidates(parent, c, opts, namespace, limiter)
	if err != nil {
		return 0, err
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].key < candidates[j].key })

	directory := filepath.Join(opts.historyDir, opaqueID(namespace))
	if err := ensureSecureDirectory(directory); err != nil {
		return 0, err
	}
	manifestPath := filepath.Join(directory, "collection-manifest.json")
	manifest := collectionManifest{
		Version: collectionManifestVersion, Namespace: namespace, Seed: opts.sampleSeed,
		CapturedAt: time.Now().UTC(), ListedPopulation: len(candidates), SampleSize: opts.sampleSize,
		CohortSize: opts.cohortSize, MaxRuns: opts.maxRuns, MaxRunAge: opts.maxRunAge,
		MaxHistoryEvents: opts.maxHistoryEvents, MaxHistoryBytes: opts.maxHistoryBytes,
		CollectorVersion: opts.collectorVersion, ServerVersion: opts.serverVersion,
	}
	if opts.resume {
		if loaded, loadErr := loadCollectionManifest(manifestPath); loadErr == nil {
			if loaded.Seed != opts.sampleSeed || loaded.SampleSize != opts.sampleSize || loaded.CohortSize != opts.cohortSize ||
				loaded.MaxRuns != opts.maxRuns || loaded.MaxRunAge != opts.maxRunAge ||
				loaded.MaxHistoryEvents != opts.maxHistoryEvents || loaded.MaxHistoryBytes != opts.maxHistoryBytes {
				return 0, errors.New("existing manifest collection options differ; choose another history directory or disable -resume")
			}
			if err := validateManifestFiles(loaded); err != nil {
				return 0, err
			}
			manifest = loaded
		} else if !errors.Is(loadErr, os.ErrNotExist) {
			return 0, loadErr
		}
	}

	cohortCounts := make(map[string]int)
	selectedSchedules := make(map[string]struct{})
	for _, record := range manifest.Cases {
		if record.Status == "collected" {
			selectedSchedules[record.ScheduleID] = struct{}{}
		}
	}
	for scheduleID := range selectedSchedules {
		for _, record := range manifest.Cases {
			if record.ScheduleID == scheduleID {
				for _, cohort := range record.Cohorts {
					cohortCounts[cohort]++
				}
				break
			}
		}
	}

	limit := min(len(candidates), opts.maxScan)
	startIndex := 0
	if manifest.LastCandidateKey != "" {
		startIndex = sort.Search(len(candidates), func(index int) bool {
			return candidates[index].key > manifest.LastCandidateKey
		})
	} else if manifest.Inspected != 0 {
		startIndex = min(manifest.Inspected, len(candidates))
	}
	var failures int
	for index := startIndex; index < limit; index++ {
		candidate := candidates[index]
		manifest.Inspected++
		manifest.LastCandidateKey = candidate.key
		current, runID, isV1, collectErr := inspectCurrentRun(parent, c, opts, namespace, candidate.scheduleID, limiter)
		if collectErr != nil {
			failures++
			manifest.Cases = append(manifest.Cases, failedCollectionCase(candidate.scheduleID, runID, nil, collectErr))
			if err := writeCollectionManifest(manifestPath, manifest); err != nil {
				return failures, err
			}
			continue
		}
		if !isV1 {
			if err := writeCollectionManifest(manifestPath, manifest); err != nil {
				return failures, err
			}
			continue
		}
		manifest.V1Inspected++
		cohorts := historyCohorts(current)
		selected := manifest.BaseSelected < opts.sampleSize
		if !selected {
			for _, cohort := range cohorts {
				if cohortCounts[cohort] < opts.cohortSize {
					selected = true
					break
				}
			}
		}
		if !selected {
			if err := writeCollectionManifest(manifestPath, manifest); err != nil {
				return failures, err
			}
			continue
		}
		if manifest.BaseSelected < opts.sampleSize {
			manifest.BaseSelected++
		}
		for _, cohort := range cohorts {
			cohortCounts[cohort]++
		}
		selectedSchedules[candidate.scheduleID] = struct{}{}

		runHistory := current
		for runIndex := 0; runIndex < opts.maxRuns && runID != ""; runIndex++ {
			if runIndex > 0 {
				runHistory, collectErr = downloadHistoryLimited(parent, c, opts, namespace, primitives.ScheduleWorkflowIDPrefix+candidate.scheduleID, runID, limiter)
				if collectErr == nil {
					collectErr = replayHistory(runHistory)
				}
			}
			if collectErr != nil {
				failures++
				manifest.Cases = append(manifest.Cases, failedCollectionCase(candidate.scheduleID, runID, cohorts, collectErr))
				break
			}
			if runIndex > 0 && runHistory.GetEvents()[0].GetEventTime().AsTime().Before(manifest.CapturedAt.Add(-opts.maxRunAge)) {
				break
			}
			record, writeErr := saveCollectedHistory(opts.historyDir, namespace, candidate.scheduleID, runID, cohorts, runHistory)
			if writeErr != nil {
				failures++
				manifest.Cases = append(manifest.Cases, failedCollectionCase(candidate.scheduleID, runID, cohorts, writeErr))
				break
			}
			manifest.Cases = append(manifest.Cases, record)
			started := runHistory.GetEvents()[0].GetWorkflowExecutionStartedEventAttributes()
			runID = started.GetContinuedExecutionRunId()
		}
		if err := writeCollectionManifest(manifestPath, manifest); err != nil {
			return failures, err
		}
	}

	fmt.Printf("COLLECTION_SUMMARY namespace=%q listed=%d inspected=%d v1=%d selected=%d runs=%d failures=%d manifest=%q\n",
		namespace, manifest.ListedPopulation, manifest.Inspected, manifest.V1Inspected, len(selectedSchedules), len(manifest.Cases), failures, manifestPath)
	return failures, nil
}

func listScheduleCandidates(parent context.Context, c client.Client, opts options, namespace string, limiter *rate.Limiter) ([]scheduleCandidate, error) {
	var candidates []scheduleCandidate
	seen := make(map[string]struct{})
	var nextPageToken []byte
	for {
		ctx, cancel := context.WithTimeout(parent, opts.timeout)
		response, err := retryAPI(ctx, limiter, func() (*workflowservice.ListSchedulesResponse, error) {
			return c.WorkflowService().ListSchedules(ctx, &workflowservice.ListSchedulesRequest{
				Namespace: namespace, MaximumPageSize: 100, NextPageToken: nextPageToken,
			})
		})
		cancel()
		if err != nil {
			return nil, fmt.Errorf("list schedules: %w", err)
		}
		for _, entry := range response.GetSchedules() {
			candidates = appendUniqueScheduleCandidate(candidates, seen, opts.sampleSeed, namespace, entry.GetScheduleId())
		}
		nextPageToken = response.GetNextPageToken()
		if len(nextPageToken) == 0 {
			return candidates, nil
		}
	}
}

func appendUniqueScheduleCandidate(
	candidates []scheduleCandidate,
	seen map[string]struct{},
	seed string,
	namespace string,
	scheduleID string,
) []scheduleCandidate {
	if scheduleID == "" {
		return candidates
	}
	if _, ok := seen[scheduleID]; ok {
		return candidates
	}
	seen[scheduleID] = struct{}{}
	return append(candidates, scheduleCandidate{
		scheduleID: scheduleID,
		key:        sampleKey(seed, namespace, scheduleID),
	})
}

func inspectCurrentRun(parent context.Context, c client.Client, opts options, namespace, scheduleID string, limiter *rate.Limiter) (*historypb.History, string, bool, error) {
	ctx, cancel := context.WithTimeout(parent, opts.timeout)
	response, err := retryAPI(ctx, limiter, func() (*workflowservice.DescribeWorkflowExecutionResponse, error) {
		return c.DescribeWorkflowExecution(ctx, primitives.ScheduleWorkflowIDPrefix+scheduleID, "")
	})
	cancel()
	if err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return nil, "", false, nil
		}
		return nil, "", false, fmt.Errorf("describe legacy scheduler workflow: %w", err)
	}
	runID := response.GetWorkflowExecutionInfo().GetExecution().GetRunId()
	history, err := downloadHistoryLimited(parent, c, opts, namespace, primitives.ScheduleWorkflowIDPrefix+scheduleID, runID, limiter)
	if err != nil || !isV1ScheduleHistory(history) {
		return history, runID, false, err
	}
	return history, runID, true, replayHistory(history)
}

func isV1ScheduleHistory(history *historypb.History) bool {
	if len(history.GetEvents()) == 0 {
		return false
	}
	started := history.GetEvents()[0].GetWorkflowExecutionStartedEventAttributes()
	if started == nil {
		return false
	}
	var args schedulespb.StartScheduleArgs
	if converter.GetDefaultDataConverter().FromPayloads(started.GetInput(), &args) != nil {
		return false
	}
	return args.GetSchedule() != nil && args.GetState() != nil
}

func downloadHistoryLimited(parent context.Context, c client.Client, opts options, namespace, workflowID, runID string, limiter *rate.Limiter) (*historypb.History, error) {
	history := &historypb.History{}
	var nextPageToken []byte
	var size int64
	for {
		ctx, cancel := context.WithTimeout(parent, opts.timeout)
		response, err := retryAPI(ctx, limiter, func() (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
			return c.WorkflowService().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
				Namespace:              namespace,
				Execution:              &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
				MaximumPageSize:        100,
				NextPageToken:          nextPageToken,
				HistoryEventFilterType: enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT,
				SkipArchival:           true,
			})
		})
		cancel()
		if err != nil {
			return nil, fmt.Errorf("download workflow history: %w", err)
		}
		for _, event := range response.GetHistory().GetEvents() {
			size += int64(proto.Size(event))
			if len(history.Events)+1 > opts.maxHistoryEvents || size > opts.maxHistoryBytes {
				return nil, &historyLimitError{events: len(history.Events) + 1, bytes: size}
			}
			history.Events = append(history.Events, event)
		}
		nextPageToken = response.GetNextPageToken()
		if len(nextPageToken) == 0 {
			break
		}
	}
	if len(history.Events) == 0 {
		return nil, errors.New("downloaded workflow history is empty")
	}
	return history, nil
}

func retryAPI[T any](ctx context.Context, limiter *rate.Limiter, operation func() (T, error)) (T, error) {
	var zero T
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if err := limiter.Wait(ctx); err != nil {
			return zero, err
		}
		result, err := operation()
		if err == nil {
			return result, nil
		}
		lastErr = err
		code := status.Code(err)
		if code != codes.Unavailable && code != codes.ResourceExhausted && code != codes.Aborted {
			return zero, err
		}
		timer := time.NewTimer(time.Duration(1<<attempt) * 250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return zero, ctx.Err()
		case <-timer.C:
		}
	}
	return zero, fmt.Errorf("production API retry limit exhausted: %w", lastErr)
}

func historyCohorts(history *historypb.History) []string {
	cohorts := make(map[string]struct{})
	if len(history.GetEvents()) == 0 {
		return nil
	}
	started := history.GetEvents()[0].GetWorkflowExecutionStartedEventAttributes()
	var args schedulespb.StartScheduleArgs
	if converter.GetDefaultDataConverter().FromPayloads(started.GetInput(), &args) == nil {
		spec := args.GetSchedule().GetSpec()
		if len(spec.GetInterval()) != 0 {
			cohorts["spec_interval"] = struct{}{}
		}
		if len(spec.GetCalendar()) != 0 || len(spec.GetStructuredCalendar()) != 0 {
			cohorts["spec_calendar"] = struct{}{}
		}
		if len(spec.GetCronString()) != 0 {
			cohorts["spec_cron"] = struct{}{}
		}
		if args.GetSchedule().GetState().GetPaused() {
			cohorts["paused"] = struct{}{}
		}
		overlap := args.GetSchedule().GetPolicies().GetOverlapPolicy().String()
		cohorts["overlap_"+strings.ToLower(strings.TrimPrefix(overlap, "SCHEDULE_OVERLAP_POLICY_"))] = struct{}{}
	}
	activityTypes := make(map[int64]string)
	for _, event := range history.GetEvents() {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
			attributes := event.GetWorkflowExecutionSignaledEventAttributes()
			switch attributes.GetSignalName() {
			case legacyscheduler.SignalNameUpdate:
				cohorts["update"] = struct{}{}
			case legacyscheduler.SignalNamePatch:
				var patch schedulepb.SchedulePatch
				if converter.GetDefaultDataConverter().FromPayloads(attributes.GetInput(), &patch) == nil {
					if len(patch.GetBackfillRequest()) != 0 {
						cohorts["backfill"] = struct{}{}
					}
					if patch.GetPause() != "" || patch.GetUnpause() != "" {
						cohorts["pause_interaction"] = struct{}{}
					}
				}
			}
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
			attributes := event.GetActivityTaskScheduledEventAttributes()
			activityTypes[event.GetEventId()] = attributes.GetActivityType().GetName()
			if attributes.GetActivityType().GetName() == "WatchWorkflow" {
				cohorts["workflow_completion"] = struct{}{}
			}
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED, enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
			var scheduledEventID int64
			if event.GetActivityTaskFailedEventAttributes() != nil {
				scheduledEventID = event.GetActivityTaskFailedEventAttributes().GetScheduledEventId()
			} else {
				scheduledEventID = event.GetActivityTaskTimedOutEventAttributes().GetScheduledEventId()
			}
			if activityTypes[scheduledEventID] == "StartWorkflow" {
				cohorts["start_failure"] = struct{}{}
			}
		case enumspb.EVENT_TYPE_MARKER_RECORDED:
			attributes := event.GetMarkerRecordedEventAttributes()
			if attributes.GetMarkerName() != "LocalActivity" {
				continue
			}
			var metadata struct {
				ActivityType string
				Attempt      int32
			}
			if converter.GetDefaultDataConverter().FromPayloads(attributes.GetDetails()["data"], &metadata) != nil {
				continue
			}
			if metadata.ActivityType == "WatchWorkflow" {
				cohorts["workflow_completion"] = struct{}{}
			}
			if metadata.ActivityType == "StartWorkflow" && attributes.GetFailure() != nil {
				cohorts["start_failure"] = struct{}{}
			}
			if metadata.ActivityType == "StartWorkflow" && metadata.Attempt > 1 {
				cohorts["start_retry"] = struct{}{}
			}
		}
	}
	result := make([]string, 0, len(cohorts))
	for cohort := range cohorts {
		result = append(result, cohort)
	}
	sort.Strings(result)
	return result
}

func saveCollectedHistory(root, namespace, scheduleID, runID string, cohorts []string, history *historypb.History) (collectionCase, error) {
	path := filepath.Join(root, opaqueID(namespace), opaqueID(scheduleID)+"-"+opaqueID(runID)+".json.gz")
	checksum, size, err := writeHistorySecure(path, history)
	record := collectionCase{
		ScheduleID: scheduleID, RunID: runID, History: path, Checksum: checksum,
		Events: len(history.GetEvents()), Bytes: size, CapturedAt: time.Now().UTC(), Cohorts: cohorts, Status: "collected",
	}
	return record, err
}

func failedCollectionCase(scheduleID, runID string, cohorts []string, err error) collectionCase {
	statusName := "failed"
	var limitErr *historyLimitError
	if errors.As(err, &limitErr) {
		statusName = "truncated"
	}
	return collectionCase{
		ScheduleID: scheduleID, RunID: runID, CapturedAt: time.Now().UTC(), Cohorts: cohorts,
		Status: statusName, Error: err.Error(),
	}
}

func loadCollectionManifest(path string) (collectionManifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return collectionManifest{}, err
	}
	var manifest collectionManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return collectionManifest{}, fmt.Errorf("decode collection manifest: %w", err)
	}
	if manifest.Version != collectionManifestVersion {
		return collectionManifest{}, fmt.Errorf("unsupported collection manifest version %d", manifest.Version)
	}
	return manifest, nil
}

func writeCollectionManifest(path string, manifest collectionManifest) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("encode collection manifest: %w", err)
	}
	return atomicWrite(path, append(data, '\n'), 0o600)
}

func validateManifestFiles(manifest collectionManifest) error {
	for _, record := range manifest.Cases {
		if record.Status != "collected" {
			continue
		}
		file, err := os.Open(record.History)
		if err != nil {
			return fmt.Errorf("open collected history %q: %w", record.History, err)
		}
		info, statErr := file.Stat()
		if statErr != nil {
			_ = file.Close()
			return fmt.Errorf("inspect collected history %q: %w", record.History, statErr)
		}
		if info.Mode().Perm()&0o077 != 0 {
			_ = file.Close()
			return fmt.Errorf("collected history %q must not be accessible by group or other users", record.History)
		}
		if record.Bytes != 0 && info.Size() != record.Bytes {
			_ = file.Close()
			return fmt.Errorf("collected history %q size differs from manifest", record.History)
		}
		hash := sha256.New()
		_, copyErr := io.Copy(hash, file)
		closeErr := file.Close()
		if copyErr != nil {
			return fmt.Errorf("checksum collected history %q: %w", record.History, copyErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close collected history %q: %w", record.History, closeErr)
		}
		if hex.EncodeToString(hash.Sum(nil)) != record.Checksum {
			return fmt.Errorf("collected history %q checksum differs from manifest", record.History)
		}
	}
	return nil
}

func ensureSecureDirectory(path string) error {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return fmt.Errorf("create secure history directory: %w", err)
		}
		info, err = os.Stat(path)
	}
	if err != nil {
		return fmt.Errorf("inspect history directory: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("history path %q is not a directory", path)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("history directory %q must not be accessible by group or other users", path)
	}
	return nil
}

func sampleKey(seed, namespace, scheduleID string) string {
	sum := sha256.Sum256([]byte(seed + "\x00" + namespace + "\x00" + scheduleID))
	return hex.EncodeToString(sum[:])
}

func opaqueID(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:12])
}

func currentBuildRevision() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, setting := range info.Settings {
		if setting.Key == "vcs.revision" {
			return setting.Value
		}
	}
	return "unknown"
}
