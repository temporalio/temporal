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
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/temporalio/sqlparser"
	"github.com/urfave/cli/v2"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/sqlquery"
	hsmnexusoperations "go.temporal.io/server/service/history/hsm/nexusoperations"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	nexusChasmScanCursorVersion = 3
	nexusChasmScanMaxPageSize   = 1000
)

const (
	nexusChasmScanRetryMaxAttempts    = 10
	nexusChasmScanRetryInitialBackoff = 2 * time.Second
	nexusChasmScanRetryMaxBackoff     = 10 * time.Second
)

type nexusChasmScanInput struct {
	namespace        string
	query            string
	effectiveQuery   string
	minPendingAge    time.Duration
	asOf             time.Time
	pageSize         int
	workers          int
	rps              int
	listRPS          int
	maxHistoryPages  int
	nextPageToken    []byte
	resumeListed     int64
	resumeDescribed  int64
	resumeCandidates int64
	resumeErrors     int64
}

type nexusChasmScanCursor struct {
	Version         int    `json:"version"`
	Namespace       string `json:"namespace"`
	QuerySHA256     string `json:"querySha256"`
	MinPendingAge   string `json:"minPendingAge"`
	MaxHistoryPages int    `json:"maxHistoryPages"`
	AsOf            string `json:"asOf"`
	NextPageToken   string `json:"nextPageToken"`
	Listed          int64  `json:"listed"`
	Described       int64  `json:"described"`
	Candidates      int64  `json:"candidates"`
	Errors          int64  `json:"errors"`
}

type nexusChasmScanOperation struct {
	ScheduledEventID       int64      `json:"scheduledEventId"`
	Backend                string     `json:"backend"`
	CandidateTrigger       bool       `json:"candidateTrigger"`
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
	ResetEventID                     int64                     `json:"resetEventId,omitempty"`
	ResetPointValid                  bool                      `json:"resetPointValid"`
	OperatorConfirmationRequired     bool                      `json:"operatorConfirmationRequired"`
	ReviewReason                     string                    `json:"reviewReason"`
	ResetPointIssue                  string                    `json:"resetPointIssue,omitempty"`
}

func (*nexusChasmScanCandidate) nexusChasmScanRecord() {}

type nexusChasmScanError struct {
	RecordType string `json:"recordType"`
	Namespace  string `json:"namespace"`
	WorkflowID string `json:"workflowId"`
	RunID      string `json:"runId"`
	Stage      string `json:"stage"`
	ErrorClass string `json:"errorClass"`
}

func (*nexusChasmScanError) nexusChasmScanRecord() {}

type nexusChasmScanRecord interface {
	nexusChasmScanRecord()
}

type nexusChasmInspectionJob struct {
	index     int
	execution *workflowpb.WorkflowExecutionInfo
}

type nexusChasmScanCheckpoint struct {
	RecordType       string    `json:"recordType"`
	Namespace        string    `json:"namespace"`
	SnapshotAsOf     time.Time `json:"snapshotAsOf"`
	VisibilityCutoff time.Time `json:"visibilityCutoff"`
	QuerySHA256      string    `json:"querySha256"`
	Listed           int64     `json:"listed"`
	Described        int64     `json:"described"`
	Candidates       int64     `json:"candidates"`
	Errors           int64     `json:"errors"`
	SnapshotComplete bool      `json:"snapshotComplete"`
	ResumeToken      string    `json:"resumeToken,omitempty"`
}

type nexusChasmScanStats struct {
	listed     atomic.Int64
	described  atomic.Int64
	candidates atomic.Int64
	errors     atomic.Int64
}

type nexusChasmScanStage string

const (
	nexusChasmScanStageDescribe nexusChasmScanStage = "describe"
	nexusChasmScanStageDecode   nexusChasmScanStage = "decode"
	nexusChasmScanStageHistory  nexusChasmScanStage = "history"
)

type nexusChasmScanExecutionError struct {
	stage nexusChasmScanStage
	err   error
}

func (e *nexusChasmScanExecutionError) Error() string {
	return fmt.Sprintf("%s: %v", e.stage, e.err)
}

func (e *nexusChasmScanExecutionError) Unwrap() error {
	return e.err
}

var errNexusChasmHistoryPageLimit = errors.New("history page limit exceeded")

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
	scanner.stats.listed.Store(in.resumeListed)
	scanner.stats.described.Store(in.resumeDescribed)
	scanner.stats.candidates.Store(in.resumeCandidates)
	scanner.stats.errors.Store(in.resumeErrors)
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
	if c.Int(FlagPageSize) > nexusChasmScanMaxPageSize {
		return nil, fmt.Errorf("--pagesize must not exceed %d", nexusChasmScanMaxPageSize)
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
	if c.Int(FlagMaxHistoryPages) <= 0 {
		return nil, errors.New("--max-history-pages must be positive")
	}
	if c.Duration(FlagMinPendingAge) < 0 {
		return nil, errors.New("--min-pending-age must not be negative")
	}

	in := &nexusChasmScanInput{
		namespace:       c.String(FlagNamespace),
		query:           strings.TrimSpace(c.String(FlagVisibilityQuery)),
		minPendingAge:   c.Duration(FlagMinPendingAge),
		asOf:            time.Now().UTC(),
		pageSize:        c.Int(FlagPageSize),
		workers:         c.Int(FlagWorkers),
		rps:             c.Int(FlagRPS),
		listRPS:         c.Int(FlagListRPS),
		maxHistoryPages: c.Int(FlagMaxHistoryPages),
	}
	if in.query == "" {
		return nil, errors.New("--query must not be empty")
	}
	if strings.Contains(strings.ToLower(in.query), strings.ToLower("TemporalNamespaceDivision")) {
		return nil, errors.New("--query must not reference TemporalNamespaceDivision")
	}
	queryPredicate, err := canonicalVisibilityPredicate(in.query)
	if err != nil {
		return nil, fmt.Errorf("invalid --query: %w", err)
	}

	if encoded := c.String(FlagResumeToken); encoded != "" {
		cursor, err := decodeNexusChasmScanCursor(encoded)
		if err != nil {
			return nil, fmt.Errorf("invalid --resume-token: %w", err)
		}
		if cursor.Version != nexusChasmScanCursorVersion {
			return nil, fmt.Errorf("unsupported resume token version %d", cursor.Version)
		}
		if cursor.Namespace != in.namespace || cursor.QuerySHA256 != nexusChasmQuerySHA256(in.query) ||
			cursor.MinPendingAge != in.minPendingAge.String() || cursor.MaxHistoryPages != in.maxHistoryPages {
			return nil, errors.New("resume token does not match --namespace, --query, --min-pending-age, and --max-history-pages")
		}
		// Keep the original cutoff stable while paging. A resumed scan completes that
		// time-bounded snapshot; a new invocation is required to include later workflows.
		in.asOf, err = time.Parse(time.RFC3339Nano, cursor.AsOf)
		if err != nil {
			return nil, fmt.Errorf("invalid resume token asOf: %w", err)
		}
		in.nextPageToken, err = base64.RawURLEncoding.DecodeString(cursor.NextPageToken)
		if err != nil {
			return nil, fmt.Errorf("invalid resume token page token: %w", err)
		}
		in.resumeListed = cursor.Listed
		in.resumeDescribed = cursor.Described
		in.resumeCandidates = cursor.Candidates
		in.resumeErrors = cursor.Errors
	}

	cutoff := in.asOf.Add(-in.minPendingAge).Format(time.RFC3339Nano)
	in.effectiveQuery = fmt.Sprintf("(%s) AND ExecutionStatus = 'Running' AND StartTime <= '%s'", queryPredicate, cutoff)
	return in, nil
}

func canonicalVisibilityPredicate(query string) (string, error) {
	statement, err := sqlparser.Parse(fmt.Sprintf(sqlquery.QueryTemplate, query))
	if err != nil {
		return "", err
	}
	selectStatement, ok := statement.(*sqlparser.Select)
	if !ok || selectStatement.Where == nil {
		return "", errors.New("query must be a Visibility predicate")
	}
	if len(selectStatement.GroupBy) != 0 || selectStatement.Having != nil ||
		len(selectStatement.OrderBy) != 0 || selectStatement.Limit != nil {
		return "", errors.New("GROUP BY, HAVING, ORDER BY, and LIMIT are not supported")
	}
	return sqlparser.String(selectStatement.Where.Expr), nil
}

func encodeNexusChasmScanCursor(
	in *nexusChasmScanInput,
	pageToken []byte,
	listed int64,
	described int64,
	candidates int64,
	scanErrors int64,
) (string, error) {
	cursor := nexusChasmScanCursor{
		Version:         nexusChasmScanCursorVersion,
		Namespace:       in.namespace,
		QuerySHA256:     nexusChasmQuerySHA256(in.query),
		MinPendingAge:   in.minPendingAge.String(),
		MaxHistoryPages: in.maxHistoryPages,
		AsOf:            in.asOf.Format(time.RFC3339Nano),
		NextPageToken:   base64.RawURLEncoding.EncodeToString(pageToken),
		Listed:          listed,
		Described:       described,
		Candidates:      candidates,
		Errors:          scanErrors,
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
		if checkpoint.SnapshotComplete {
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
	records []nexusChasmScanRecord,
) error {
	for _, record := range records {
		if record == nil {
			continue
		}
		if err := encoder.Encode(record); err != nil {
			return fmt.Errorf("write scan record: %w", err)
		}
		switch record.(type) {
		case *nexusChasmScanCandidate:
			s.stats.candidates.Add(1)
		case *nexusChasmScanError:
			s.stats.errors.Add(1)
		default:
			return fmt.Errorf("unsupported scan record type %T", record)
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
		SnapshotAsOf:     s.input.asOf,
		VisibilityCutoff: s.input.asOf.Add(-s.input.minPendingAge),
		QuerySHA256:      nexusChasmQuerySHA256(s.input.query),
		Listed:           s.stats.listed.Load(),
		Described:        s.stats.described.Load(),
		Candidates:       s.stats.candidates.Load(),
		Errors:           s.stats.errors.Load(),
		SnapshotComplete: len(nextPageToken) == 0,
	}
	if len(nextPageToken) != 0 {
		var err error
		checkpoint.ResumeToken, err = encodeNexusChasmScanCursor(
			s.input,
			nextPageToken,
			checkpoint.Listed,
			checkpoint.Described,
			checkpoint.Candidates,
			checkpoint.Errors,
		)
		if err != nil {
			return checkpoint, fmt.Errorf("encode resume token: %w", err)
		}
	}
	if err := encoder.Encode(checkpoint); err != nil {
		return checkpoint, fmt.Errorf("write checkpoint: %w", err)
	}
	_, _ = fmt.Fprintf(progress, "scan checkpoint: %d listed, %d described, %d candidates, %d errors, snapshot-complete=%t\n",
		checkpoint.Listed, checkpoint.Described, checkpoint.Candidates, checkpoint.Errors, checkpoint.SnapshotComplete)
	return checkpoint, nil
}

func (s *nexusChasmScanner) scanError(
	execution *workflowpb.WorkflowExecutionInfo,
	err error,
) *nexusChasmScanError {
	inspectionErr, ok := errors.AsType[*nexusChasmScanExecutionError](err)
	if !ok {
		return &nexusChasmScanError{
			RecordType: "scan_error",
			Namespace:  s.input.namespace,
			WorkflowID: execution.GetExecution().GetWorkflowId(),
			RunID:      execution.GetExecution().GetRunId(),
			Stage:      "unknown",
			ErrorClass: "unknown",
		}
	}
	return &nexusChasmScanError{
		RecordType: "scan_error",
		Namespace:  s.input.namespace,
		WorkflowID: execution.GetExecution().GetWorkflowId(),
		RunID:      execution.GetExecution().GetRunId(),
		Stage:      string(inspectionErr.stage),
		ErrorClass: nexusChasmScanErrorClass(inspectionErr.stage, inspectionErr.err),
	}
}

func nexusChasmScanFatalError(ctx context.Context, err error) bool {
	if ctx.Err() != nil || errors.Is(err, context.Canceled) {
		return true
	}
	inspectionErr, ok := errors.AsType[*nexusChasmScanExecutionError](err)
	if !ok {
		return true
	}
	switch nexusChasmScanStatusCode(inspectionErr.err) {
	case codes.PermissionDenied, codes.Unauthenticated, codes.Unimplemented:
		return true
	default:
		return false
	}
}

func nexusChasmScanErrorClass(stage nexusChasmScanStage, err error) string {
	if errors.Is(err, errNexusChasmHistoryPageLimit) {
		return "history_page_limit"
	}
	if code := nexusChasmScanStatusCode(err); code != codes.Unknown {
		return strings.ToLower(code.String())
	}
	return string(stage) + "_failed"
}

func nexusChasmScanStatusCode(err error) codes.Code {
	if errors.Is(err, context.Canceled) {
		return codes.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return codes.DeadlineExceeded
	}
	for current := err; current != nil; current = errors.Unwrap(current) {
		if rpcStatus, ok := common.GetRPCStatus(current); ok {
			return rpcStatus.Code()
		}
	}
	return codes.Unknown
}

func (s *nexusChasmScanner) inspectPage(
	ctx context.Context,
	executions []*workflowpb.WorkflowExecutionInfo,
) ([]nexusChasmScanRecord, error) {
	results := make([]nexusChasmScanRecord, len(executions))
	g, groupCtx := errgroup.WithContext(ctx)
	jobs := make(chan nexusChasmInspectionJob)
	g.Go(func() error {
		defer close(jobs)
		for i, execution := range executions {
			if execution.GetStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
				continue
			}
			select {
			case jobs <- nexusChasmInspectionJob{index: i, execution: execution}:
			case <-groupCtx.Done():
				return groupCtx.Err()
			}
		}
		return nil
	})
	workerCount := min(s.input.workers, len(executions))
	for range workerCount {
		g.Go(func() error { return s.inspectJobs(groupCtx, jobs, results) })
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return results, nil
}

func (s *nexusChasmScanner) inspectJobs(
	ctx context.Context,
	jobs <-chan nexusChasmInspectionJob,
	results []nexusChasmScanRecord,
) error {
	for job := range jobs {
		candidate, err := s.inspectExecution(ctx, job.execution)
		if err != nil {
			if nexusChasmScanFatalError(ctx, err) {
				return fmt.Errorf("inspect workflow %q run %q: %w",
					job.execution.GetExecution().GetWorkflowId(), job.execution.GetExecution().GetRunId(), err)
			}
			results[job.index] = s.scanError(job.execution, err)
			continue
		}
		if candidate != nil {
			results[job.index] = candidate
		}
	}
	return nil
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
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if common.IsNotFoundError(err) {
			return nil, nil
		}
		return nil, &nexusChasmScanExecutionError{stage: nexusChasmScanStageDescribe, err: err}
	}
	s.stats.described.Add(1)
	if description.GetDatabaseMutableState().GetExecutionState().GetStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return nil, nil
	}

	operations, err := decodePendingNexusOperations(
		ctx,
		description.GetDatabaseMutableState(),
		s.registry,
	)
	if err != nil {
		return nil, &nexusChasmScanExecutionError{stage: nexusChasmScanStageDecode, err: err}
	}
	if !markNexusChasmCandidateTriggers(operations, s.input.asOf.Add(-s.input.minPendingAge)) {
		return nil, nil
	}

	history, err := s.loadHistory(ctx, execution.GetExecution().GetWorkflowId(), execution.GetExecution().GetRunId(), operations)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, &nexusChasmScanExecutionError{stage: nexusChasmScanStageHistory, err: err}
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

func decodePendingNexusOperations(
	ctx context.Context,
	mutableState *persistencespb.WorkflowMutableState,
	registry *chasm.Registry,
) (operations []nexusChasmScanOperation, retErr error) {
	// Component fields deserialize lazily and surface corrupt persisted data as a panic. Convert
	// that framework panic to an error so the scan stops without checkpointing past the execution.
	defer func() {
		if recovered := recover(); recovered != nil {
			retErr = fmt.Errorf("decode persisted Nexus state: %v", recovered)
		}
	}()

	chasmOperations, err := decodePendingChasmNexusOperations(ctx, mutableState, registry)
	if err != nil && !errors.Is(err, chasm.ErrNoChasmNodes) {
		return nil, err
	}
	operations = append(operations, chasmOperations...)

	hsmOperations, err := decodePendingHSMNexusOperations(mutableState)
	if err != nil {
		return nil, err
	}
	operations = append(operations, hsmOperations...)
	slices.SortFunc(operations, func(a, b nexusChasmScanOperation) int {
		return cmp.Compare(a.ScheduledEventID, b.ScheduledEventID)
	})
	return operations, nil
}

func decodePendingChasmNexusOperations(
	ctx context.Context,
	mutableState *persistencespb.WorkflowMutableState,
	registry *chasm.Registry,
) ([]nexusChasmScanOperation, error) {
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
	operations := make([]nexusChasmScanOperation, 0, len(workflow.Operations))
	for scheduledEventID, operationField := range workflow.Operations {
		operation := operationField.Get(chasmCtx)
		if operation == nil || operation.LifecycleState(chasmCtx).IsClosed() {
			continue
		}
		result := nexusChasmScanOperation{
			ScheduledEventID:       scheduledEventID,
			Backend:                "chasm",
			State:                  operation.GetStatus().String(),
			Attempt:                operation.GetAttempt(),
			ScheduledTime:          latestTimestamp(operation.GetScheduledTime()),
			LastPersistedStateTime: latestTimestamp(operation.GetScheduledTime(), operation.GetStartedTime(), operation.GetLastAttemptCompleteTime()),
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
	return operations, nil
}

func decodePendingHSMNexusOperations(
	mutableState *persistencespb.WorkflowMutableState,
) ([]nexusChasmScanOperation, error) {
	machineMap := mutableState.GetExecutionInfo().GetSubStateMachinesByType()[hsmnexusoperations.OperationMachineType]
	if machineMap == nil {
		return nil, nil
	}
	operations := make([]nexusChasmScanOperation, 0, len(machineMap.GetMachinesById()))
	for operationID, node := range machineMap.GetMachinesById() {
		scheduledEventID, err := strconv.ParseInt(operationID, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("decode HSM Nexus operation ID %q: %w", operationID, err)
		}
		operation := &persistencespb.NexusOperationInfo{}
		if err := proto.Unmarshal(node.GetData(), operation); err != nil {
			return nil, fmt.Errorf("decode HSM Nexus operation %d: %w", scheduledEventID, err)
		}
		if operation.GetState() == enumsspb.NEXUS_OPERATION_STATE_UNSPECIFIED {
			return nil, fmt.Errorf("decode HSM Nexus operation %d: state is unspecified", scheduledEventID)
		}
		if hsmNexusOperationClosed(operation.GetState()) {
			continue
		}
		result := nexusChasmScanOperation{
			ScheduledEventID:       scheduledEventID,
			Backend:                "hsm",
			State:                  operation.GetState().String(),
			Attempt:                operation.GetAttempt(),
			ScheduledTime:          latestTimestamp(operation.GetScheduledTime()),
			LastPersistedStateTime: latestTimestamp(operation.GetScheduledTime(), operation.GetStartedTime(), operation.GetLastAttemptCompleteTime()),
		}
		if cancellationMap := node.GetChildren()[hsmnexusoperations.CancelationMachineType]; cancellationMap != nil {
			if cancellationNode := cancellationMap.GetMachinesById()[hsmnexusoperations.CancelationMachineKey.ID]; cancellationNode != nil {
				cancellation := &persistencespb.NexusOperationCancellationInfo{}
				if err := proto.Unmarshal(cancellationNode.GetData(), cancellation); err != nil {
					return nil, fmt.Errorf("decode HSM Nexus operation cancellation %d: %w", scheduledEventID, err)
				}
				result.CancellationState = cancellation.GetState().String()
				result.CancellationAttempt = cancellation.GetAttempt()
				result.LastPersistedStateTime = latestTimestamp(
					operation.GetScheduledTime(),
					operation.GetStartedTime(),
					operation.GetLastAttemptCompleteTime(),
					cancellation.GetRequestedTime(),
					cancellation.GetLastAttemptCompleteTime(),
				)
			}
		}
		operations = append(operations, result)
	}
	return operations, nil
}

func hsmNexusOperationClosed(state enumsspb.NexusOperationState) bool {
	switch state { // nolint:exhaustive
	case enumsspb.NEXUS_OPERATION_STATE_CANCELED,
		enumsspb.NEXUS_OPERATION_STATE_FAILED,
		enumsspb.NEXUS_OPERATION_STATE_SUCCEEDED,
		enumsspb.NEXUS_OPERATION_STATE_TIMED_OUT:
		return true
	default:
		return false
	}
}

func markNexusChasmCandidateTriggers(operations []nexusChasmScanOperation, cutoff time.Time) bool {
	hasCandidate := false
	for i := range operations {
		operation := &operations[i]
		if operation.Backend != "chasm" {
			continue
		}
		operation.CandidateTrigger = operation.ScheduledTime == nil || !operation.ScheduledTime.After(cutoff)
		hasCandidate = hasCandidate || operation.CandidateTrigger
	}
	return hasCandidate
}

func (s *nexusChasmScanner) loadHistory(
	ctx context.Context,
	workflowID string,
	runID string,
	operations []nexusChasmScanOperation,
) ([]*historypb.HistoryEvent, error) {
	collector := newNexusChasmReverseHistoryCollector(operations)
	var nextPageToken []byte
	for pageNumber := 1; ; pageNumber++ {
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
		if pageNumber >= s.input.maxHistoryPages {
			return nil, fmt.Errorf("%w: exceeded --max-history-pages=%d", errNexusChasmHistoryPageLimit, s.input.maxHistoryPages)
		}
	}
}

type nexusChasmReverseHistoryCollector struct {
	events              []*historypb.HistoryEvent
	minScheduledEventID int64
	reachedOldestTarget bool
}

func newNexusChasmReverseHistoryCollector(operations []nexusChasmScanOperation) *nexusChasmReverseHistoryCollector {
	collector := &nexusChasmReverseHistoryCollector{}
	for _, operation := range operations {
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

	_, relevant, terminal := nexusHistoryEventScheduledID(event, nil)
	if relevant || terminal {
		c.events = append(c.events, event)
	}
}

func (c *nexusChasmReverseHistoryCollector) done(nextPageToken []byte) bool {
	return len(nextPageToken) == 0 || c.reachedOldestTarget
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
	}, policy, func(err error) bool {
		return nexusChasmScanRetryable(ctx, err)
	})
}

func nexusChasmScanRetryable(parent context.Context, err error) bool {
	if parent.Err() != nil || errors.Is(err, context.Canceled) {
		return false
	}
	// A per-RPC timeout is retryable while the parent scan is still active.
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return common.IsRetryableRPCError(err)
}

func analyzeNexusChasmResetSafety(
	namespace string,
	workflowID string,
	runID string,
	operations []nexusChasmScanOperation,
	events []*historypb.HistoryEvent,
) nexusChasmScanCandidate {
	candidate := nexusChasmScanCandidate{
		RecordType:                   "candidate",
		Namespace:                    namespace,
		WorkflowID:                   workflowID,
		RunID:                        runID,
		Operations:                   operations,
		OperatorConfirmationRequired: true,
		ReviewReason:                 "pending age identifies a recovery candidate but does not prove that an operation is stuck",
	}
	pendingIDs, issues := validatePendingNexusOperations(operations)
	evidence := collectNexusResetHistoryEvidence(pendingIDs, events)
	issues = append(issues, validateNexusResetHistory(pendingIDs, evidence)...)
	candidate.LatestRelevantNexusEvent = evidence.latestRelevant
	if evidence.latestRelevant == nil {
		issues = append(issues, "no Nexus event exists in the inspected history")
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
			candidate.ResetEventID = candidate.QualifyingWorkflowTaskCompletion.EventID
		}
	}

	slices.Sort(issues)
	candidate.ResetPointValid = len(issues) == 0
	candidate.ResetPointIssue = strings.Join(issues, "; ")
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
				"pending operation %d exists in both state trees",
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
	if !relevant && !terminal {
		return
	}
	if event.GetEventType() == enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUESTED && scheduledID > 0 {
		cancelRequestedToScheduled[event.GetEventId()] = scheduledID
	}
	summary := summarizeHistoryEvent(event)
	// A reset must not discard any Nexus event, including terminal events for operations that
	// are no longer pending in mutable state. Use the latest Nexus event across the entire
	// inspected history window when choosing the Workflow Task boundary.
	if e.latestRelevant == nil || summary.EventID > e.latestRelevant.EventID {
		summaryCopy := summary
		e.latestRelevant = &summaryCopy
	}
	if _, pending := pendingIDs[scheduledID]; !pending {
		return
	}
	if event.GetEventType() == enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED {
		e.scheduledFound[scheduledID] = true
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
