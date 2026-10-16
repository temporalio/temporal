package tdbg

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/urfave/cli/v2"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/log"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const nexusChasmScanCursorVersion = 1

const (
	nexusChasmScanRetryMaxAttempts    = 10
	nexusChasmScanRetryInitialBackoff = 2 * time.Second
	nexusChasmScanRetryMaxBackoff     = 10 * time.Second
)

type nexusChasmScanInput struct {
	namespace      string
	query          string
	effectiveQuery string
	minPendingAge  time.Duration
	asOf           time.Time
	pageSize       int
	workers        int
	rps            int
	listRPS        int
	nextPageToken  []byte
}

type nexusChasmScanCursor struct {
	Version       int    `json:"version"`
	Namespace     string `json:"namespace"`
	QuerySHA256   string `json:"querySha256"`
	MinPendingAge string `json:"minPendingAge"`
	AsOf          string `json:"asOf"`
	NextPageToken string `json:"nextPageToken"`
}

type nexusChasmScanOperation struct {
	ScheduledEventID       int64      `json:"scheduledEventId"`
	State                  string     `json:"state"`
	Attempt                int32      `json:"attempt"`
	ScheduledTime          *time.Time `json:"scheduledTime,omitempty"`
	LastPersistedStateTime *time.Time `json:"lastPersistedStateTime,omitempty"`
	CancellationState      string     `json:"cancellationState,omitempty"`
	CancellationAttempt    int32      `json:"cancellationAttempt,omitempty"`
}

type nexusChasmHistoryEvent struct {
	EventID   int64     `json:"eventId"`
	EventType string    `json:"eventType"`
	EventTime time.Time `json:"eventTime"`
}

type nexusChasmScanCandidate struct {
	RecordType                       string                    `json:"recordType"`
	Namespace                        string                    `json:"namespace"`
	WorkflowID                       string                    `json:"workflowId"`
	RunID                            string                    `json:"runId"`
	Operations                       []nexusChasmScanOperation `json:"operations,omitempty"`
	LatestRelevantNexusEvent         *nexusChasmHistoryEvent   `json:"latestRelevantNexusEvent,omitempty"`
	QualifyingWorkflowTaskCompletion *nexusChasmHistoryEvent   `json:"qualifyingWorkflowTaskCompletion,omitempty"`
	RecommendedResetEventID          int64                     `json:"recommendedResetEventId,omitempty"`
	SafeToReset                      bool                      `json:"safeToReset"`
	UnsafeReason                     string                    `json:"unsafeReason,omitempty"`
}

type nexusChasmScanCheckpoint struct {
	RecordType       string    `json:"recordType"`
	Namespace        string    `json:"namespace"`
	AsOf             time.Time `json:"asOf"`
	VisibilityCutoff time.Time `json:"visibilityCutoff"`
	QuerySHA256      string    `json:"querySha256"`
	Listed           int64     `json:"listed"`
	Described        int64     `json:"described"`
	Candidates       int64     `json:"candidates"`
	Errors           int64     `json:"errors"`
	Complete         bool      `json:"complete"`
	ResumeToken      string    `json:"resumeToken,omitempty"`
}

type nexusChasmScanStats struct {
	listed     atomic.Int64
	described  atomic.Int64
	candidates atomic.Int64
	errors     atomic.Int64
}

// AdminScanNexusChasm lists and inspects workflow executions without mutating server state. It
// emits only minimal recovery metadata: never mutable-state blobs, payloads, headers, endpoint
// names, operation tokens, request IDs, or failure details.
func AdminScanNexusChasm(c *cli.Context, clientFactory ClientFactory) error {
	in, err := parseNexusChasmScanInput(c)
	if err != nil {
		return err
	}

	registry, err := newChasmRegistry(log.NewCLILogger())
	if err != nil {
		return fmt.Errorf("create CHASM registry: %w", err)
	}

	scanner := nexusChasmScanner{
		input:          in,
		adminClient:    clientFactory.AdminClient(c),
		workflowClient: clientFactory.WorkflowClient(c),
		registry:       registry,
		nonListLimiter: rate.NewLimiter(rate.Limit(in.rps), 1),
		listLimiter:    rate.NewLimiter(rate.Limit(in.listRPS), 1),
		rpcTimeout:     time.Duration(c.Int(FlagContextTimeout)) * time.Second,
	}
	return scanner.run(c.Context, c.App.Writer, c.App.ErrWriter)
}

func parseNexusChasmScanInput(c *cli.Context) (*nexusChasmScanInput, error) {
	if !c.IsSet(FlagNamespace) {
		return nil, errors.New("explicit --namespace is required for a fleet recovery scan")
	}
	if c.String(FlagOutput) != "jsonl" {
		return nil, errors.New("--output must be jsonl")
	}
	if c.Int(FlagPageSize) <= 0 {
		return nil, errors.New("--pagesize must be positive")
	}
	if c.Int(FlagWorkers) <= 0 {
		return nil, errors.New("--workers must be positive")
	}
	if c.Int(FlagRPS) <= 0 {
		return nil, errors.New("--rps must be positive")
	}
	if c.Int(FlagListRPS) <= 0 {
		return nil, errors.New("--list-rps must be positive")
	}
	if c.Duration(FlagMinPendingAge) < 0 {
		return nil, errors.New("--min-pending-age must not be negative")
	}

	in := &nexusChasmScanInput{
		namespace:     c.String(FlagNamespace),
		query:         strings.TrimSpace(c.String(FlagVisibilityQuery)),
		minPendingAge: c.Duration(FlagMinPendingAge),
		asOf:          time.Now().UTC(),
		pageSize:      c.Int(FlagPageSize),
		workers:       c.Int(FlagWorkers),
		rps:           c.Int(FlagRPS),
		listRPS:       c.Int(FlagListRPS),
	}
	if in.query == "" {
		return nil, errors.New("--query must not be empty")
	}
	if strings.Contains(strings.ToLower(in.query), strings.ToLower("TemporalNamespaceDivision")) {
		return nil, errors.New("--query must not reference TemporalNamespaceDivision")
	}

	if encoded := c.String(FlagResumeToken); encoded != "" {
		cursor, err := decodeNexusChasmScanCursor(encoded)
		if err != nil {
			return nil, fmt.Errorf("invalid --resume-token: %w", err)
		}
		if cursor.Version != nexusChasmScanCursorVersion {
			return nil, fmt.Errorf("unsupported resume token version %d", cursor.Version)
		}
		if cursor.Namespace != in.namespace || cursor.QuerySHA256 != nexusChasmQuerySHA256(in.query) || cursor.MinPendingAge != in.minPendingAge.String() {
			return nil, errors.New("resume token does not match --namespace, --query, and --min-pending-age")
		}
		in.asOf, err = time.Parse(time.RFC3339Nano, cursor.AsOf)
		if err != nil {
			return nil, fmt.Errorf("invalid resume token asOf: %w", err)
		}
		in.nextPageToken, err = base64.RawURLEncoding.DecodeString(cursor.NextPageToken)
		if err != nil {
			return nil, fmt.Errorf("invalid resume token page token: %w", err)
		}
	}

	cutoff := in.asOf.Add(-in.minPendingAge).Format(time.RFC3339Nano)
	in.effectiveQuery = fmt.Sprintf("(%s) AND ExecutionStatus = 'Running' AND StartTime <= '%s'", in.query, cutoff)
	return in, nil
}

func encodeNexusChasmScanCursor(in *nexusChasmScanInput, pageToken []byte) (string, error) {
	cursor := nexusChasmScanCursor{
		Version:       nexusChasmScanCursorVersion,
		Namespace:     in.namespace,
		QuerySHA256:   nexusChasmQuerySHA256(in.query),
		MinPendingAge: in.minPendingAge.String(),
		AsOf:          in.asOf.Format(time.RFC3339Nano),
		NextPageToken: base64.RawURLEncoding.EncodeToString(pageToken),
	}
	b, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func nexusChasmQuerySHA256(query string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(query)))
}

func decodeNexusChasmScanCursor(encoded string) (nexusChasmScanCursor, error) {
	var cursor nexusChasmScanCursor
	b, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return cursor, err
	}
	if err := json.Unmarshal(b, &cursor); err != nil {
		return cursor, err
	}
	return cursor, nil
}

type nexusChasmScanner struct {
	input          *nexusChasmScanInput
	adminClient    adminservice.AdminServiceClient
	workflowClient workflowservice.WorkflowServiceClient
	registry       *chasm.Registry
	nonListLimiter *rate.Limiter
	listLimiter    *rate.Limiter
	rpcTimeout     time.Duration
	stats          nexusChasmScanStats
}

func (s *nexusChasmScanner) run(ctx context.Context, output, progress io.Writer) error {
	encoder := json.NewEncoder(output)
	nextPageToken := s.input.nextPageToken
	for {
		page, err := s.listPage(ctx, nextPageToken)
		if err != nil {
			return err
		}

		s.stats.listed.Add(int64(len(page.GetExecutions())))
		records, err := s.inspectPage(ctx, page.GetExecutions())
		if err != nil {
			return err
		}
		if err := s.writeScanRecords(encoder, records); err != nil {
			return err
		}

		nextPageToken = page.GetNextPageToken()
		checkpoint, err := s.writeCheckpoint(encoder, progress, nextPageToken)
		if err != nil {
			return err
		}
		if checkpoint.Complete {
			return nil
		}
	}
}

func (s *nexusChasmScanner) listPage(
	ctx context.Context,
	nextPageToken []byte,
) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	page, err := callNexusChasmScanRPC(ctx, s.listLimiter, s.rpcTimeout, func(rpcCtx context.Context) (*workflowservice.ListWorkflowExecutionsResponse, error) {
		return s.workflowClient.ListWorkflowExecutions(rpcCtx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace:     s.input.namespace,
			Query:         s.input.effectiveQuery,
			PageSize:      int32(s.input.pageSize),
			NextPageToken: nextPageToken,
		})
	})
	if err != nil {
		return nil, fmt.Errorf("list executions: %w", err)
	}
	return page, nil
}

func (s *nexusChasmScanner) writeScanRecords(
	encoder *json.Encoder,
	records []*nexusChasmScanCandidate,
) error {
	for _, record := range records {
		if record == nil {
			continue
		}
		if err := encoder.Encode(record); err != nil {
			return fmt.Errorf("write candidate: %w", err)
		}
		if record.RecordType == "scan_error" {
			s.stats.errors.Add(1)
		} else {
			s.stats.candidates.Add(1)
		}
	}
	return nil
}

func (s *nexusChasmScanner) writeCheckpoint(
	encoder *json.Encoder,
	progress io.Writer,
	nextPageToken []byte,
) (nexusChasmScanCheckpoint, error) {
	checkpoint := nexusChasmScanCheckpoint{
		RecordType:       "checkpoint",
		Namespace:        s.input.namespace,
		AsOf:             s.input.asOf,
		VisibilityCutoff: s.input.asOf.Add(-s.input.minPendingAge),
		QuerySHA256:      nexusChasmQuerySHA256(s.input.query),
		Listed:           s.stats.listed.Load(),
		Described:        s.stats.described.Load(),
		Candidates:       s.stats.candidates.Load(),
		Errors:           s.stats.errors.Load(),
		Complete:         len(nextPageToken) == 0,
	}
	if len(nextPageToken) != 0 {
		var err error
		checkpoint.ResumeToken, err = encodeNexusChasmScanCursor(s.input, nextPageToken)
		if err != nil {
			return checkpoint, fmt.Errorf("encode resume token: %w", err)
		}
	}
	if err := encoder.Encode(checkpoint); err != nil {
		return checkpoint, fmt.Errorf("write checkpoint: %w", err)
	}
	_, _ = fmt.Fprintf(progress, "scan checkpoint: %d listed, %d described, %d candidates, %d errors, complete=%t\n",
		checkpoint.Listed, checkpoint.Described, checkpoint.Candidates, checkpoint.Errors, checkpoint.Complete)
	return checkpoint, nil
}

func (s *nexusChasmScanner) scanError(
	execution *workflowpb.WorkflowExecutionInfo,
	reason string,
) *nexusChasmScanCandidate {
	return &nexusChasmScanCandidate{
		RecordType:   "scan_error",
		Namespace:    s.input.namespace,
		WorkflowID:   execution.GetExecution().GetWorkflowId(),
		RunID:        execution.GetExecution().GetRunId(),
		SafeToReset:  false,
		UnsafeReason: reason,
	}
}

func (s *nexusChasmScanner) inspectPage(
	ctx context.Context,
	executions []*workflowpb.WorkflowExecutionInfo,
) ([]*nexusChasmScanCandidate, error) {
	results := make([]*nexusChasmScanCandidate, len(executions))
	g, groupCtx := errgroup.WithContext(ctx)
	semaphore := make(chan struct{}, s.input.workers)
	for i, execution := range executions {
		if execution.GetStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
			continue
		}
		g.Go(func() error {
			select {
			case semaphore <- struct{}{}:
				defer func() { <-semaphore }()
			case <-groupCtx.Done():
				return groupCtx.Err()
			}
			candidate, err := s.inspectExecution(groupCtx, execution)
			if err != nil {
				return fmt.Errorf("inspect workflow %q run %q: %w",
					execution.GetExecution().GetWorkflowId(), execution.GetExecution().GetRunId(), err)
			}
			results[i] = candidate
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return results, nil
}

func (s *nexusChasmScanner) inspectExecution(
	ctx context.Context,
	execution *workflowpb.WorkflowExecutionInfo,
) (*nexusChasmScanCandidate, error) {
	description, err := callNexusChasmScanRPC(ctx, s.nonListLimiter, s.rpcTimeout, func(rpcCtx context.Context) (*adminservice.DescribeMutableStateResponse, error) {
		return s.adminClient.DescribeMutableState(rpcCtx, &adminservice.DescribeMutableStateRequest{
			Namespace:       s.input.namespace,
			Execution:       execution.GetExecution(),
			ArchetypeId:     chasm.WorkflowArchetypeID,
			SkipForceReload: true,
		})
	})
	if err != nil {
		if common.IsNotFoundError(err) {
			return nil, nil
		}
		return s.scanError(execution, "DescribeMutableState failed"), nil
	}
	s.stats.described.Add(1)
	if description.GetDatabaseMutableState().GetExecutionState().GetStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return nil, nil
	}

	operations, err := decodePendingChasmNexusOperations(
		ctx,
		description.GetDatabaseMutableState(),
		s.registry,
		s.input.asOf.Add(-s.input.minPendingAge),
	)
	if errors.Is(err, chasm.ErrNoChasmNodes) {
		return nil, nil
	}
	if err != nil {
		return s.scanError(execution, "CHASM Nexus operation decode failed"), nil
	}
	if len(operations) == 0 {
		return nil, nil
	}

	history, err := s.loadHistory(ctx, execution.GetExecution().GetWorkflowId(), execution.GetExecution().GetRunId(), operations)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return s.scanError(execution, "workflow history read failed"), nil
	}
	candidate := analyzeNexusChasmResetSafety(
		s.input.namespace,
		execution.GetExecution().GetWorkflowId(),
		execution.GetExecution().GetRunId(),
		operations,
		history,
	)
	return &candidate, nil
}

func decodePendingChasmNexusOperations(
	ctx context.Context,
	mutableState *persistencespb.WorkflowMutableState,
	registry *chasm.Registry,
	cutoff time.Time,
) (operations []nexusChasmScanOperation, retErr error) {
	// Component fields deserialize lazily and surface corrupt persisted data as a panic. Convert
	// that framework panic to a per-execution scan error instead of terminating a fleet scan.
	defer func() {
		if recovered := recover(); recovered != nil {
			retErr = fmt.Errorf("decode persisted CHASM component: %v", recovered)
		}
	}()

	hasNexusOperation := false
	for _, node := range mutableState.GetChasmNodes() {
		if node.GetMetadata().GetComponentAttributes().GetTypeId() == nexusoperation.ArchetypeID {
			hasNexusOperation = true
			break
		}
	}
	if !hasNexusOperation {
		return nil, nil
	}

	workflow, chasmCtx, err := chasm.NewDetachedExecution[*chasmworkflow.Workflow](ctx, mutableState, registry)
	if err != nil {
		return nil, err
	}
	for scheduledEventID, operationField := range workflow.Operations {
		operation := operationField.Get(chasmCtx)
		if operation == nil || operation.LifecycleState(chasmCtx).IsClosed() {
			continue
		}
		scheduledTime := latestTimestamp(operation.GetScheduledTime())
		if scheduledTime != nil && scheduledTime.After(cutoff) {
			continue
		}

		lastStateTime := latestTimestamp(
			operation.GetScheduledTime(),
			operation.GetStartedTime(),
			operation.GetLastAttemptCompleteTime(),
		)
		result := nexusChasmScanOperation{
			ScheduledEventID:       scheduledEventID,
			State:                  operation.GetStatus().String(),
			Attempt:                operation.GetAttempt(),
			ScheduledTime:          scheduledTime,
			LastPersistedStateTime: lastStateTime,
		}
		if cancellation, ok := operation.Cancellation.TryGet(chasmCtx); ok {
			result.CancellationState = cancellation.GetStatus().String()
			result.CancellationAttempt = cancellation.GetAttempt()
			result.LastPersistedStateTime = latestTimestamp(
				operation.GetScheduledTime(),
				operation.GetStartedTime(),
				operation.GetLastAttemptCompleteTime(),
				cancellation.GetRequestedTime(),
				cancellation.GetLastAttemptCompleteTime(),
			)
		}
		operations = append(operations, result)
	}
	slices.SortFunc(operations, func(a, b nexusChasmScanOperation) int {
		return cmp.Compare(a.ScheduledEventID, b.ScheduledEventID)
	})
	return operations, nil
}

func (s *nexusChasmScanner) loadHistory(
	ctx context.Context,
	workflowID string,
	runID string,
	operations []nexusChasmScanOperation,
) ([]*historypb.HistoryEvent, error) {
	collector := newNexusChasmReverseHistoryCollector(operations)
	var nextPageToken []byte
	for {
		resp, err := callNexusChasmScanRPC(ctx, s.nonListLimiter, s.rpcTimeout, func(rpcCtx context.Context) (*workflowservice.GetWorkflowExecutionHistoryReverseResponse, error) {
			return s.workflowClient.GetWorkflowExecutionHistoryReverse(rpcCtx, &workflowservice.GetWorkflowExecutionHistoryReverseRequest{
				Namespace:       s.input.namespace,
				Execution:       &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
				MaximumPageSize: 1000,
				NextPageToken:   nextPageToken,
			})
		})
		if err != nil {
			return nil, fmt.Errorf("get workflow history: %w", err)
		}
		collector.addPage(resp.GetHistory().GetEvents())
		nextPageToken = resp.GetNextPageToken()
		if collector.done(nextPageToken) {
			return collector.sortedEvents(), nil
		}
	}
}

type nexusChasmReverseHistoryCollector struct {
	events                       []*historypb.HistoryEvent
	targetScheduledIDs           map[int64]struct{}
	foundScheduledIDs            map[int64]struct{}
	unresolvedCancelRequestedIDs map[int64]struct{}
	minScheduledEventID          int64
	reachedOldestTarget          bool
}

func newNexusChasmReverseHistoryCollector(operations []nexusChasmScanOperation) *nexusChasmReverseHistoryCollector {
	collector := &nexusChasmReverseHistoryCollector{
		targetScheduledIDs:           make(map[int64]struct{}, len(operations)),
		foundScheduledIDs:            make(map[int64]struct{}, len(operations)),
		unresolvedCancelRequestedIDs: make(map[int64]struct{}),
	}
	for _, operation := range operations {
		collector.targetScheduledIDs[operation.ScheduledEventID] = struct{}{}
		if collector.minScheduledEventID == 0 || operation.ScheduledEventID < collector.minScheduledEventID {
			collector.minScheduledEventID = operation.ScheduledEventID
		}
	}
	return collector
}

func (c *nexusChasmReverseHistoryCollector) addPage(events []*historypb.HistoryEvent) {
	for _, event := range events {
		c.addEvent(event)
	}
}

func (c *nexusChasmReverseHistoryCollector) addEvent(event *historypb.HistoryEvent) {
	if event.GetEventId() <= c.minScheduledEventID {
		c.reachedOldestTarget = true
	}
	if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
		c.events = append(c.events, event)
		return
	}

	scheduledID, relevant, terminal := nexusHistoryEventScheduledID(event, nil)
	legacyCancelOutcome := c.trackLegacyCancelOutcome(event)
	if event.GetEventType() == enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUESTED {
		delete(c.unresolvedCancelRequestedIDs, event.GetEventId())
	}
	if event.GetEventType() == enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED {
		if _, target := c.targetScheduledIDs[event.GetEventId()]; target {
			c.foundScheduledIDs[event.GetEventId()] = struct{}{}
		}
	}
	_, target := c.targetScheduledIDs[scheduledID]
	if legacyCancelOutcome || (target && (relevant || terminal)) {
		c.events = append(c.events, event)
	}
}

func (c *nexusChasmReverseHistoryCollector) trackLegacyCancelOutcome(event *historypb.HistoryEvent) bool {
	var requestedEventID int64
	switch event.GetEventType() {
	case enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_COMPLETED:
		attrs := event.GetNexusOperationCancelRequestCompletedEventAttributes()
		if attrs.GetScheduledEventId() == 0 {
			requestedEventID = attrs.GetRequestedEventId()
		}
	case enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_FAILED:
		attrs := event.GetNexusOperationCancelRequestFailedEventAttributes()
		if attrs.GetScheduledEventId() == 0 {
			requestedEventID = attrs.GetRequestedEventId()
		}
	default:
		return false
	}
	if requestedEventID == 0 {
		return false
	}
	c.unresolvedCancelRequestedIDs[requestedEventID] = struct{}{}
	return true
}

func (c *nexusChasmReverseHistoryCollector) done(nextPageToken []byte) bool {
	return len(nextPageToken) == 0 ||
		(c.reachedOldestTarget &&
			len(c.foundScheduledIDs) == len(c.targetScheduledIDs) &&
			len(c.unresolvedCancelRequestedIDs) == 0)
}

func (c *nexusChasmReverseHistoryCollector) sortedEvents() []*historypb.HistoryEvent {
	slices.SortFunc(c.events, func(a, b *historypb.HistoryEvent) int {
		return cmp.Compare(a.GetEventId(), b.GetEventId())
	})
	return c.events
}

func callNexusChasmScanRPC[T any](
	ctx context.Context,
	limiter *rate.Limiter,
	rpcTimeout time.Duration,
	call func(context.Context) (T, error),
) (T, error) {
	policy := backoff.NewExponentialRetryPolicy(nexusChasmScanRetryInitialBackoff).
		WithMaximumInterval(nexusChasmScanRetryMaxBackoff).
		WithMaximumAttempts(nexusChasmScanRetryMaxAttempts)
	return backoff.ThrottleRetryContextWithReturn(ctx, func(ctx context.Context) (T, error) {
		var zero T
		if limiter != nil {
			if err := limiter.Wait(ctx); err != nil {
				return zero, err
			}
		}
		rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
		defer cancel()
		return call(rpcCtx)
	}, policy, common.IsServiceTransientError)
}

func analyzeNexusChasmResetSafety(
	namespace string,
	workflowID string,
	runID string,
	operations []nexusChasmScanOperation,
	events []*historypb.HistoryEvent,
) nexusChasmScanCandidate {
	candidate := nexusChasmScanCandidate{
		RecordType: "candidate",
		Namespace:  namespace,
		WorkflowID: workflowID,
		RunID:      runID,
		Operations: operations,
	}
	pendingIDs, issues := validatePendingNexusOperations(operations)
	evidence := collectNexusResetHistoryEvidence(pendingIDs, events)
	issues = append(issues, validateNexusResetHistory(pendingIDs, evidence)...)
	candidate.LatestRelevantNexusEvent = evidence.latestRelevant
	if evidence.latestRelevant == nil {
		issues = append(issues, "no relevant Nexus event exists for the pending operations")
	} else {
		candidate.QualifyingWorkflowTaskCompletion = latestWorkflowTaskCompletionAfter(
			evidence.workflowTaskCompletions,
			evidence.latestRelevant.EventID,
		)
		if candidate.QualifyingWorkflowTaskCompletion == nil {
			issues = append(issues, fmt.Sprintf(
				"no WorkflowTaskCompleted event exists after Nexus event %d",
				evidence.latestRelevant.EventID,
			))
		} else {
			candidate.RecommendedResetEventID = candidate.QualifyingWorkflowTaskCompletion.EventID
		}
	}

	slices.Sort(issues)
	candidate.SafeToReset = len(issues) == 0
	candidate.UnsafeReason = strings.Join(issues, "; ")
	return candidate
}

func validatePendingNexusOperations(
	operations []nexusChasmScanOperation,
) (map[int64]struct{}, []string) {
	pendingIDs := make(map[int64]struct{}, len(operations))
	issues := make([]string, 0)
	for _, operation := range operations {
		if operation.ScheduledEventID <= 0 {
			issues = append(issues, "pending operation has invalid scheduled event ID")
			continue
		}
		if operation.ScheduledTime == nil {
			issues = append(issues, fmt.Sprintf(
				"pending operation %d has no persisted scheduled time",
				operation.ScheduledEventID,
			))
		}
		if _, duplicate := pendingIDs[operation.ScheduledEventID]; duplicate {
			issues = append(issues, fmt.Sprintf(
				"duplicate pending scheduled event ID %d",
				operation.ScheduledEventID,
			))
		}
		pendingIDs[operation.ScheduledEventID] = struct{}{}
	}
	return pendingIDs, issues
}

type nexusResetHistoryEvidence struct {
	scheduledFound          map[int64]bool
	latestRelevant          *nexusChasmHistoryEvent
	terminalEvents          map[int64][]nexusChasmHistoryEvent
	workflowTaskCompletions []nexusChasmHistoryEvent
}

func collectNexusResetHistoryEvidence(
	pendingIDs map[int64]struct{},
	events []*historypb.HistoryEvent,
) nexusResetHistoryEvidence {
	evidence := nexusResetHistoryEvidence{
		scheduledFound: make(map[int64]bool, len(pendingIDs)),
		terminalEvents: make(map[int64][]nexusChasmHistoryEvent),
	}
	cancelRequestedToScheduled := make(map[int64]int64)
	for _, event := range events {
		evidence.addEvent(event, pendingIDs, cancelRequestedToScheduled)
	}
	return evidence
}

func (e *nexusResetHistoryEvidence) addEvent(
	event *historypb.HistoryEvent,
	pendingIDs map[int64]struct{},
	cancelRequestedToScheduled map[int64]int64,
) {
	if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
		e.workflowTaskCompletions = append(e.workflowTaskCompletions, summarizeHistoryEvent(event))
		return
	}

	scheduledID, relevant, terminal := nexusHistoryEventScheduledID(event, cancelRequestedToScheduled)
	if event.GetEventType() == enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUESTED && scheduledID > 0 {
		cancelRequestedToScheduled[event.GetEventId()] = scheduledID
	}
	if _, pending := pendingIDs[scheduledID]; !pending {
		return
	}
	if event.GetEventType() == enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED {
		e.scheduledFound[scheduledID] = true
	}
	summary := summarizeHistoryEvent(event)
	if relevant && (e.latestRelevant == nil || summary.EventID > e.latestRelevant.EventID) {
		summaryCopy := summary
		e.latestRelevant = &summaryCopy
	}
	if terminal {
		e.terminalEvents[scheduledID] = append(e.terminalEvents[scheduledID], summary)
	}
}

func validateNexusResetHistory(
	pendingIDs map[int64]struct{},
	evidence nexusResetHistoryEvidence,
) []string {
	issues := make([]string, 0)
	for scheduledID := range pendingIDs {
		if !evidence.scheduledFound[scheduledID] {
			issues = append(issues, fmt.Sprintf("scheduled event %d is missing from history", scheduledID))
		}
		terminal := evidence.terminalEvents[scheduledID]
		if len(terminal) != 0 {
			latest := terminal[len(terminal)-1]
			issues = append(issues, fmt.Sprintf(
				"pending operation %d has terminal history event %d (%s)",
				scheduledID,
				latest.EventID,
				latest.EventType,
			))
		}
	}
	return issues
}

func latestWorkflowTaskCompletionAfter(
	completions []nexusChasmHistoryEvent,
	eventID int64,
) *nexusChasmHistoryEvent {
	var latest *nexusChasmHistoryEvent
	for i := range completions {
		completion := completions[i]
		if completion.EventID <= eventID || latest != nil && completion.EventID <= latest.EventID {
			continue
		}
		completionCopy := completion
		latest = &completionCopy
	}
	return latest
}

func nexusHistoryEventScheduledID(
	event *historypb.HistoryEvent,
	cancelRequestedToScheduled map[int64]int64,
) (scheduledID int64, relevant bool, terminal bool) {
	switch event.GetEventType() {
	case enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED:
		return event.GetEventId(), true, false
	case enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED:
		return event.GetNexusOperationStartedEventAttributes().GetScheduledEventId(), true, false
	case enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUESTED:
		return event.GetNexusOperationCancelRequestedEventAttributes().GetScheduledEventId(), true, false
	case enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_COMPLETED:
		attrs := event.GetNexusOperationCancelRequestCompletedEventAttributes()
		scheduledID = attrs.GetScheduledEventId()
		if scheduledID == 0 {
			scheduledID = cancelRequestedToScheduled[attrs.GetRequestedEventId()]
		}
		return scheduledID, true, false
	case enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_FAILED:
		attrs := event.GetNexusOperationCancelRequestFailedEventAttributes()
		scheduledID = attrs.GetScheduledEventId()
		if scheduledID == 0 {
			scheduledID = cancelRequestedToScheduled[attrs.GetRequestedEventId()]
		}
		return scheduledID, true, false
	case enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED:
		return event.GetNexusOperationCompletedEventAttributes().GetScheduledEventId(), false, true
	case enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED:
		return event.GetNexusOperationFailedEventAttributes().GetScheduledEventId(), false, true
	case enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED:
		return event.GetNexusOperationCanceledEventAttributes().GetScheduledEventId(), false, true
	case enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT:
		return event.GetNexusOperationTimedOutEventAttributes().GetScheduledEventId(), false, true
	default:
		return 0, false, false
	}
}

func summarizeHistoryEvent(event *historypb.HistoryEvent) nexusChasmHistoryEvent {
	return nexusChasmHistoryEvent{
		EventID:   event.GetEventId(),
		EventType: event.GetEventType().String(),
		EventTime: event.GetEventTime().AsTime().UTC(),
	}
}

func latestTimestamp(timestamps ...*timestamppb.Timestamp) *time.Time {
	var latest time.Time
	for _, timestamp := range timestamps {
		if timestamp == nil || !timestamp.IsValid() {
			continue
		}
		value := timestamp.AsTime().UTC()
		if value.After(latest) {
			latest = value
		}
	}
	if latest.IsZero() {
		return nil
	}
	return &latest
}
