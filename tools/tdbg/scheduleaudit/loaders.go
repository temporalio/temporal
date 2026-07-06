package scheduleaudit

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"strings"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// grpcRetrier wraps RPCs with retry-on-ResourceExhausted, backing off exponentially with jitter.
type grpcRetrier struct {
	log io.Writer
}

const (
	retryMaxAttempts     = 10
	retryInitialBackoff  = time.Second
	retryMaxBackoff      = 10 * time.Second
	retryBackoffJitter   = 0.2
	retryLogAfterAttempt = 3
)

func (t *grpcRetrier) do(ctx context.Context, opName string, f func() error) error {
	var err error
	for attempt := range retryMaxAttempts {
		err = f()
		if err == nil {
			return nil
		}
		if status.Code(err) != codes.ResourceExhausted {
			return err
		}
		if attempt == retryMaxAttempts-1 {
			return err
		}
		wait := backoffForAttempt(attempt)
		nextAttempt := attempt + 2 // attempt is 0-indexed; this is the (attempt+1)-th retry about to start
		if t.log != nil && nextAttempt > retryLogAfterAttempt {
			_, _ = fmt.Fprintf(t.log, "    rate limited on %s; sleeping %s before retry %d/%d\n",
				opName, wait, nextAttempt, retryMaxAttempts)
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

func backoffForAttempt(attempt int) time.Duration {
	wait := min(time.Duration(float64(retryInitialBackoff)*math.Pow(2, float64(attempt))), retryMaxBackoff)
	jitter := (rand.Float64()*2 - 1) * retryBackoffJitter
	return time.Duration(float64(wait) * (1 + jitter))
}

// NewGRPCScheduleLoader returns a ScheduleLoader backed by the workflow-service frontend. log receives rate-limit
// retry lines; pass io.Discard to silence them. limiter paces every RPC per namespace; pass nil to disable pacing.
func NewGRPCScheduleLoader(client workflowservice.WorkflowServiceClient, log io.Writer, limiter *NamespaceRateLimiter) ScheduleLoader {
	return &grpcScheduleLoader{client: client, retrier: &grpcRetrier{log: log}, limiter: limiter}
}

type grpcScheduleLoader struct {
	client  workflowservice.WorkflowServiceClient
	retrier *grpcRetrier
	limiter *NamespaceRateLimiter
}

// ListScheduleIDs pages through ListSchedules and calls yield with each schedule ID as pages arrive, so a caller can
// begin describing/analyzing schedules without waiting for the full namespace listing. Describing each schedule
// (LookupSchedule) is left to the caller, which fans it out concurrently.
func (l *grpcScheduleLoader) ListScheduleIDs(ctx context.Context, namespace string, yield func(id string) error) error {
	var pageToken []byte
	for {
		if err := l.limiter.Wait(ctx, namespace); err != nil {
			return err
		}
		var resp *workflowservice.ListSchedulesResponse
		err := l.retrier.do(ctx, fmt.Sprintf("ListSchedules(%s)", namespace), func() error {
			var rpcErr error
			resp, rpcErr = l.client.ListSchedules(ctx, &workflowservice.ListSchedulesRequest{
				Namespace:       namespace,
				MaximumPageSize: visibilityPageSize,
				NextPageToken:   pageToken,
			})
			return rpcErr
		})
		if err != nil {
			return err
		}
		for _, s := range resp.GetSchedules() {
			if s.GetInfo() == nil {
				continue
			}
			if err := yield(s.GetScheduleId()); err != nil {
				return err
			}
		}
		if len(resp.GetNextPageToken()) == 0 {
			return nil
		}
		pageToken = resp.GetNextPageToken()
	}
}

func (l *grpcScheduleLoader) LookupSchedule(ctx context.Context, namespace, scheduleID string) (ScheduleEntry, error) {
	if err := l.limiter.Wait(ctx, namespace); err != nil {
		return ScheduleEntry{}, err
	}
	var resp *workflowservice.DescribeScheduleResponse
	err := l.retrier.do(ctx, fmt.Sprintf("DescribeSchedule(%s/%s)", namespace, scheduleID), func() error {
		var rpcErr error
		resp, rpcErr = l.client.DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
		})
		return rpcErr
	})
	if err != nil {
		return ScheduleEntry{}, err
	}
	info := resp.GetInfo()
	sched := resp.GetSchedule()
	state := sched.GetState()
	policies := sched.GetPolicies()
	entry := ScheduleEntry{
		ID:           scheduleID,
		Spec:         sched.GetSpec(),
		WorkflowType: sched.GetAction().GetStartWorkflow().GetWorkflowType().GetName(),
		Paused:       state.GetPaused(),
		Policies:     policies,
		Exhausted:    state.GetLimitedActions() && state.GetRemainingActions() == 0,
	}
	if ct := info.GetCreateTime(); ct != nil {
		entry.CreateTime = ct.AsTime()
	}
	if ut := info.GetUpdateTime(); ut != nil {
		entry.UpdateTime = ut.AsTime()
	}
	if cw := policies.GetCatchupWindow(); cw != nil {
		entry.CatchupWindow = cw.AsDuration()
	}
	return entry, nil
}

func (l *grpcScheduleLoader) NamespaceRetention(ctx context.Context, namespace string) (time.Duration, error) {
	if err := l.limiter.Wait(ctx, namespace); err != nil {
		return 0, err
	}
	var resp *workflowservice.DescribeNamespaceResponse
	err := l.retrier.do(ctx, fmt.Sprintf("DescribeNamespace(%s)", namespace), func() error {
		var rpcErr error
		resp, rpcErr = l.client.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
			Namespace: namespace,
		})
		return rpcErr
	})
	if err != nil {
		return 0, err
	}
	if d := resp.GetConfig().GetWorkflowExecutionRetentionTtl(); d != nil {
		return d.AsDuration(), nil
	}
	return 0, nil
}

// NewGRPCExecutionLoader returns an ExecutionLoader backed by the workflow-service frontend. log receives rate-limit
// retry and pagination-progress lines; pass io.Discard to silence them. limiter paces every RPC per namespace; pass
// nil to disable pacing.
func NewGRPCExecutionLoader(client workflowservice.WorkflowServiceClient, log io.Writer, limiter *NamespaceRateLimiter) ExecutionLoader {
	return &grpcExecutionLoader{client: client, retrier: &grpcRetrier{log: log}, log: log, limiter: limiter}
}

type grpcExecutionLoader struct {
	client  workflowservice.WorkflowServiceClient
	retrier *grpcRetrier
	log     io.Writer
	limiter *NamespaceRateLimiter
}

// visibilityPageSize is the per-call page size for visibility queries. 1000 is a safe default; the server may clamp
// to a lower max, and each call is bounded by the 4MB gRPC limit (~5000 rows in practice).
const visibilityPageSize = 1000
const pageProgressEvery = 5

// ListExecutions issues one paginated visibility query returning every workflow started by scheduleID that was alive
// at some moment in [windowStart, queryEnd]. The predicate `StartTime <= queryEnd AND (CloseTime >= windowStart OR
// CloseTime IS NULL)` captures in-window starts, still-running long-runners, closed long-runners that crossed
// windowStart, and heavily-delayed buffered actions in one query.
func (l *grpcExecutionLoader) ListExecutions(ctx context.Context, namespace, scheduleID string, windowStart, queryEnd time.Time) ([]Execution, error) {
	query := fmt.Sprintf(
		`TemporalScheduledById = %q AND StartTime <= %q AND (CloseTime >= %q OR CloseTime IS NULL)`,
		scheduleID,
		queryEnd.UTC().Format(time.RFC3339),
		windowStart.UTC().Format(time.RFC3339),
	)
	return l.paginate(ctx, namespace, scheduleID, query)
}

// paginate runs the per-schedule visibility query to completion, collecting the matching executions.
func (l *grpcExecutionLoader) paginate(ctx context.Context, namespace, scheduleID, query string) ([]Execution, error) {
	opLabel := fmt.Sprintf("ListWorkflowExecutions(%s/%s)", namespace, scheduleID)
	var out []Execution
	var pageToken []byte
	var page, skipped int
	for {
		resp, err := l.fetchPage(ctx, namespace, query, pageToken, opLabel)
		if err != nil {
			return nil, err
		}
		page++
		out, skipped = appendPageEntries(resp, scheduleID, out, skipped)
		if l.log != nil && page%pageProgressEvery == 0 {
			_, _ = fmt.Fprintf(l.log, "      %s/%s: page %d, %d entries (%d skipped)\n",
				namespace, scheduleID, page, len(out), skipped)
		}
		if len(resp.GetNextPageToken()) == 0 {
			return out, nil
		}
		pageToken = resp.GetNextPageToken()
	}
}

func (l *grpcExecutionLoader) fetchPage(ctx context.Context, namespace, query string, pageToken []byte, opLabel string) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	if err := l.limiter.Wait(ctx, namespace); err != nil {
		return nil, err
	}
	var resp *workflowservice.ListWorkflowExecutionsResponse
	err := l.retrier.do(ctx, opLabel, func() error {
		var rpcErr error
		resp, rpcErr = l.client.ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace:     namespace,
			Query:         query,
			PageSize:      visibilityPageSize,
			NextPageToken: pageToken,
		})
		return rpcErr
	})
	return resp, err
}

// appendPageEntries parses each execution on the page into an Execution and appends it to out. Rows whose
// TemporalScheduledById doesn't decode to the expected scheduleID (a visibility quirk) or that lack both a nominal and
// a start time are skipped. Returns the grown slice and running skipped count.
func appendPageEntries(resp *workflowservice.ListWorkflowExecutionsResponse, scheduleID string, out []Execution, skipped int) ([]Execution, int) {
	for _, exec := range resp.GetExecutions() {
		sa := exec.GetSearchAttributes().GetIndexedFields()
		if id, ok := decodeScheduledByID(sa["TemporalScheduledById"]); !ok || id != scheduleID {
			skipped++
			continue
		}
		entry, ok := buildExecution(exec, sa)
		if !ok {
			skipped++
			continue
		}
		out = append(out, entry)
	}
	return out, skipped
}

// buildExecution constructs an Execution from one visibility row. Returns ok=false if the row has neither nominal nor
// start time, since there's nothing to match against.
func buildExecution(exec *workflowpb.WorkflowExecutionInfo, sa map[string]*commonpb.Payload) (Execution, bool) {
	entry := Execution{
		WorkflowID: exec.GetExecution().GetWorkflowId(),
		RunID:      exec.GetExecution().GetRunId(),
		Status:     exec.GetStatus(),
	}
	if st := exec.GetStartTime(); st != nil {
		entry.StartTime = st.AsTime()
	}
	if ct := exec.GetCloseTime(); ct != nil {
		t := ct.AsTime()
		entry.CloseTime = &t
	}
	if nominal, ok := decodeNominalStartTime(sa["TemporalScheduledStartTime"]); ok {
		entry.NominalTime = nominal
	}
	if entry.NominalTime.IsZero() && entry.StartTime.IsZero() {
		return Execution{}, false
	}
	if entry.NominalTime.IsZero() {
		entry.NominalTime = entry.StartTime
	}
	return entry, true
}

// decodeScheduledByID extracts the schedule ID from a TemporalScheduledById payload, returning false on a missing or
// unexpectedly-encoded payload so the caller can skip the row.
func decodeScheduledByID(payload *commonpb.Payload) (string, bool) {
	if payload == nil {
		return "", false
	}
	if !strings.HasPrefix(string(payload.GetMetadata()["encoding"]), "json/plain") {
		return "", false
	}
	var raw string
	if err := json.Unmarshal(payload.GetData(), &raw); err != nil {
		return "", false
	}
	return raw, true
}

// decodeNominalStartTime extracts the nominal scheduled start time from a TemporalScheduledStartTime payload (a
// JSON-encoded RFC3339 string), returning false on a missing or unexpectedly-encoded payload.
func decodeNominalStartTime(payload *commonpb.Payload) (time.Time, bool) {
	if payload == nil {
		return time.Time{}, false
	}
	if !strings.HasPrefix(string(payload.GetMetadata()["encoding"]), "json/plain") {
		return time.Time{}, false
	}
	var raw string
	if err := json.Unmarshal(payload.GetData(), &raw); err != nil {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}
