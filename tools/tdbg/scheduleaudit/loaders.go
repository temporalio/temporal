package scheduleaudit

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

// grpcRetrier wraps RPCs with the server's standard transient-error retry policy.
type grpcRetrier struct {
	log    io.Writer
	policy backoff.RetryPolicy
}

type requestLimiter interface {
	Wait(context.Context, string) error
}

const (
	retryMaxAttempts     = 10
	retryInitialBackoff  = 2 * time.Second
	retryMaxBackoff      = 10 * time.Second
	retryLogAfterAttempt = 3
)

func (t *grpcRetrier) do(ctx context.Context, opName string, f func() error) error {
	attempt := 0
	policy := t.policy
	if policy == nil {
		policy = backoff.NewExponentialRetryPolicy(retryInitialBackoff).
			WithMaximumInterval(retryMaxBackoff).
			WithMaximumAttempts(retryMaxAttempts)
	}
	_, err := backoff.ThrottleRetryContextWithReturn(ctx, func(context.Context) (struct{}, error) {
		attempt++
		err := f()
		if err != nil && common.IsServiceTransientError(err) && attempt > retryLogAfterAttempt && t.log != nil {
			_, _ = fmt.Fprintf(t.log, "    transient error on %s; retrying attempt %d/%d: %v\n",
				opName, attempt, retryMaxAttempts, err)
		}
		return struct{}{}, err
	}, policy, common.IsServiceTransientError)
	return err
}

// NewGRPCScheduleLoader returns a ScheduleLoader backed by the workflow-service frontend. log receives rate-limit
// retry lines; pass io.Discard to silence them. limiter paces describe requests, while listLimiter paces
// ListSchedules requests. Pass nil to disable either limit.
func NewGRPCScheduleLoader(client workflowservice.WorkflowServiceClient, log io.Writer, limiter, listLimiter *NamespaceRateLimiter) ScheduleLoader {
	return &grpcScheduleLoader{client: client, retrier: &grpcRetrier{log: log}, limiter: limiter, listLimiter: listLimiter}
}

type grpcScheduleLoader struct {
	client      workflowservice.WorkflowServiceClient
	retrier     *grpcRetrier
	limiter     requestLimiter
	listLimiter requestLimiter
}

// ListScheduleIDs pages through ListSchedules and calls yield with each schedule ID as pages arrive, so a caller can
// begin describing/analyzing schedules without waiting for the full namespace listing. Describing each schedule
// (LookupSchedule) is left to the caller, which fans it out concurrently.
func (l *grpcScheduleLoader) ListScheduleIDs(ctx context.Context, namespace string, yield func(id string) error) error {
	var pageToken []byte
	for {
		var resp *workflowservice.ListSchedulesResponse
		err := l.retrier.do(ctx, fmt.Sprintf("ListSchedules(%s)", namespace), func() error {
			if err := l.listLimiter.Wait(ctx, namespace); err != nil {
				return err
			}
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
	var resp *workflowservice.DescribeScheduleResponse
	err := l.retrier.do(ctx, fmt.Sprintf("DescribeSchedule(%s/%s)", namespace, scheduleID), func() error {
		if err := l.limiter.Wait(ctx, namespace); err != nil {
			return err
		}
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

func (l *grpcScheduleLoader) DescribeNamespace(ctx context.Context, namespace string) (NamespaceInfo, error) {
	var resp *workflowservice.DescribeNamespaceResponse
	err := l.retrier.do(ctx, fmt.Sprintf("DescribeNamespace(%s)", namespace), func() error {
		if err := l.limiter.Wait(ctx, namespace); err != nil {
			return err
		}
		var rpcErr error
		resp, rpcErr = l.client.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
			Namespace: namespace,
		})
		return rpcErr
	})
	if err != nil {
		return NamespaceInfo{}, err
	}
	info := NamespaceInfo{ID: resp.GetNamespaceInfo().GetId()}
	if d := resp.GetConfig().GetWorkflowExecutionRetentionTtl(); d != nil {
		info.Retention = d.AsDuration()
	}
	return info, nil
}

// NewGRPCExecutionLoader returns an ExecutionLoader backed by the workflow-service frontend. log receives rate-limit
// retry and pagination-progress lines; pass io.Discard to silence them. limiter paces every
// ListWorkflowExecutions request per namespace; pass nil to disable pacing.
func NewGRPCExecutionLoader(client workflowservice.WorkflowServiceClient, log io.Writer, limiter *NamespaceRateLimiter) ExecutionLoader {
	return &grpcExecutionLoader{client: client, retrier: &grpcRetrier{log: log}, log: log, limiter: limiter}
}

type grpcExecutionLoader struct {
	client  workflowservice.WorkflowServiceClient
	retrier *grpcRetrier
	log     io.Writer
	limiter requestLimiter
}

// visibilityPageSize is the per-call page size for visibility queries. 1000 is a safe default; the server may clamp
// to a lower max, and each call is bounded by the 4MB gRPC limit (~5000 rows in practice).
const visibilityPageSize = 1000
const pageProgressEvery = 5

// ListExecutions issues one paginated visibility query returning the workflows this audit needs, keyed on the nominal
// (pre-jitter) scheduled time recorded in the TemporalScheduledStartTime search attribute rather than on actual
// StartTime. Two OR'd groups:
//
//   - nominal in [queryStart, windowEnd]: every action that could be scheduled inside the window, regardless of when it actually
//     started -- so a heavily-delayed action (nominal in-window, started long after windowEnd) is still captured
//     without a look-ahead buffer, and actions scheduled after the window aren't pulled in.
//   - nominal <= queryStart AND still alive at queryStart (CloseTime >= queryStart OR running): pre-window
//     long-runners that were active during the window, needed to classify overlap skips at the leading edge.
//
// Matching nominal on the search attribute is what makes the delayed-fire and retention-safety buffers unnecessary.
func (l *grpcExecutionLoader) ListExecutions(ctx context.Context, namespace string, scheduleIDs []string, queryStart, windowEnd time.Time) (map[string][]Execution, error) {
	if len(scheduleIDs) == 0 {
		return map[string][]Execution{}, nil
	}
	query := executionQuery(scheduleIDs, queryStart, windowEnd)
	return l.paginate(ctx, namespace, scheduleIDs, query)
}

func executionQuery(scheduleIDs []string, queryStart, windowEnd time.Time) string {
	quotedIDs := make([]string, len(scheduleIDs))
	for i, id := range scheduleIDs {
		quotedIDs[i] = strconv.Quote(id)
	}
	start := queryStart.UTC().Format(time.RFC3339)
	end := windowEnd.UTC().Format(time.RFC3339)
	return fmt.Sprintf(
		`%s IN (%s) AND (`+
			`(%s >= %q AND %s <= %q) OR `+
			`(%s <= %q AND (CloseTime >= %q OR CloseTime IS NULL)))`,
		sadefs.TemporalScheduledById, strings.Join(quotedIDs, ", "),
		sadefs.TemporalScheduledStartTime, start, sadefs.TemporalScheduledStartTime, end,
		sadefs.TemporalScheduledStartTime, start, start,
	)
}

// paginate runs a batched visibility query to completion and groups executions by schedule ID.
func (l *grpcExecutionLoader) paginate(ctx context.Context, namespace string, scheduleIDs []string, query string) (map[string][]Execution, error) {
	opLabel := fmt.Sprintf("ListWorkflowExecutions(%s/%d schedules)", namespace, len(scheduleIDs))
	wanted := make(map[string]struct{}, len(scheduleIDs))
	out := make(map[string][]Execution, len(scheduleIDs))
	for _, id := range scheduleIDs {
		wanted[id] = struct{}{}
	}
	var pageToken []byte
	var page, entries, skipped int
	for {
		resp, err := l.fetchPage(ctx, namespace, query, pageToken, opLabel)
		if err != nil {
			return nil, err
		}
		page++
		pageEntries, pageSkipped := appendPageEntries(resp, wanted, out)
		entries += pageEntries
		skipped += pageSkipped
		if l.log != nil && page%pageProgressEvery == 0 {
			_, _ = fmt.Fprintf(l.log, "      %s/%d schedules: page %d, %d entries (%d skipped)\n",
				namespace, len(scheduleIDs), page, entries, skipped)
		}
		if len(resp.GetNextPageToken()) == 0 {
			return out, nil
		}
		pageToken = resp.GetNextPageToken()
	}
}

func (l *grpcExecutionLoader) fetchPage(ctx context.Context, namespace, query string, pageToken []byte, opLabel string) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	var resp *workflowservice.ListWorkflowExecutionsResponse
	err := l.retrier.do(ctx, opLabel, func() error {
		if err := l.limiter.Wait(ctx, namespace); err != nil {
			return err
		}
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

// appendPageEntries parses and groups one visibility page. Rows with a missing or unexpected schedule ID, or without
// both a nominal and start time, are skipped.
func appendPageEntries(
	resp *workflowservice.ListWorkflowExecutionsResponse,
	wanted map[string]struct{},
	out map[string][]Execution,
) (entries int, skipped int) {
	for _, exec := range resp.GetExecutions() {
		sa := exec.GetSearchAttributes().GetIndexedFields()
		id, ok := decodeScheduledByID(sa[sadefs.TemporalScheduledById])
		if _, requested := wanted[id]; !ok || !requested {
			skipped++
			continue
		}
		entry, ok := buildExecution(exec, sa)
		if !ok {
			skipped++
			continue
		}
		out[id] = append(out[id], entry)
		entries++
	}
	return
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
	if nominal, ok := decodeNominalStartTime(sa[sadefs.TemporalScheduledStartTime]); ok {
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
	raw, err := sadefs.DecodeValue(payload, enumspb.INDEXED_VALUE_TYPE_KEYWORD, false)
	if err != nil {
		return "", false
	}
	id, ok := raw.(string)
	return id, ok
}

// decodeNominalStartTime extracts the nominal scheduled start time from a TemporalScheduledStartTime payload (a
// JSON-encoded RFC3339 string), returning false on a missing or unexpectedly-encoded payload.
func decodeNominalStartTime(payload *commonpb.Payload) (time.Time, bool) {
	if payload == nil {
		return time.Time{}, false
	}
	raw, err := sadefs.DecodeValue(payload, enumspb.INDEXED_VALUE_TYPE_DATETIME, false)
	if err != nil {
		return time.Time{}, false
	}
	t, ok := raw.(time.Time)
	return t, ok
}
