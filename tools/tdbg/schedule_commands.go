package tdbg

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/urfave/cli/v2"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/namespace"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
)

type missedTime struct {
	ScheduleID   string `json:"schedule_id"`
	ExpectedTime string `json:"expected_time"`
	Reason       string `json:"reason"`
}

// AdminScheduleMissedTimes lists scheduled times that did not result in a workflow execution.
func AdminScheduleMissedTimes(c *cli.Context, clientFactory ClientFactory) error {
	nsName, err := getRequiredOption(c, FlagNamespace)
	if err != nil {
		return err
	}

	lookbackStr, err := getRequiredOption(c, FlagLookback)
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	lookbackStart, err := parseTimeRange(lookbackStr, now)
	if err != nil {
		return fmt.Errorf("invalid lookback duration %q: %w", lookbackStr, err)
	}

	outputFormat := c.String(FlagOutput)
	if outputFormat == "" {
		outputFormat = "table"
	}
	if outputFormat != "table" && outputFormat != "jsonl" {
		return fmt.Errorf("invalid output format %q, valid values are: table, jsonl", outputFormat)
	}

	scheduleIDs, err := collectScheduleIDs(c)
	if err != nil {
		return err
	}

	wfClient := clientFactory.WorkflowClient(c)

	ctx, cancel := newContext(c)
	defer cancel()

	// Resolve namespace ID for jitter seed computation.
	nsID, err := getNamespaceID(c, clientFactory, namespace.Name(nsName))
	if err != nil {
		return fmt.Errorf("unable to resolve namespace %q: %w", nsName, err)
	}

	specBuilder := legacyscheduler.NewSpecBuilder()

	var allMissed []missedTime
	for _, scheduleID := range scheduleIDs {
		missed, err := findMissedTimes(ctx, wfClient, specBuilder, nsName, nsID.String(), scheduleID, lookbackStart, now)
		if err != nil {
			return fmt.Errorf("error processing schedule %q: %w", scheduleID, err)
		}
		allMissed = append(allMissed, missed...)
	}

	return writeMissedTimes(c, allMissed, outputFormat)
}

// collectScheduleIDs reads schedule IDs from --schedule-id flag or stdin.
func collectScheduleIDs(c *cli.Context) ([]string, error) {
	if sid := c.String(FlagScheduleID); sid != "" {
		return []string{sid}, nil
	}

	// Read from stdin.
	stat, _ := os.Stdin.Stat()
	if (stat.Mode() & os.ModeCharDevice) != 0 {
		return nil, fmt.Errorf("either --%s must be provided or schedule IDs must be piped via stdin", FlagScheduleID)
	}

	var ids []string
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			ids = append(ids, line)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading stdin: %w", err)
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf("no schedule IDs provided via stdin")
	}
	return ids, nil
}

// findMissedTimes computes expected nominal times for a schedule and diffs against actual workflow runs.
func findMissedTimes(
	ctx context.Context,
	wfClient workflowservice.WorkflowServiceClient,
	specBuilder *legacyscheduler.SpecBuilder,
	nsName string,
	nsID string,
	scheduleID string,
	start, end time.Time,
) ([]missedTime, error) {
	// 1. Describe the schedule to get its spec and policies.
	descResp, err := wfClient.DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  nsName,
		ScheduleId: scheduleID,
	})
	if err != nil {
		return nil, fmt.Errorf("DescribeSchedule failed: %w", err)
	}

	schedule := descResp.GetSchedule()
	spec := schedule.GetSpec()
	if spec == nil {
		return nil, nil // No spec means no expected times.
	}

	baseWorkflowID := schedule.GetAction().GetStartWorkflow().GetWorkflowId()
	if baseWorkflowID == "" {
		return nil, fmt.Errorf("schedule has no StartWorkflow action with a workflow ID")
	}

	catchupWindow := schedule.GetPolicies().GetCatchupWindow().AsDuration()
	if catchupWindow == 0 {
		// Server default is 365 days.
		catchupWindow = 365 * 24 * time.Hour
	}

	// 2. Compile the spec and compute expected nominal times.
	compiledSpec, err := specBuilder.NewCompiledSpec(spec)
	if err != nil {
		return nil, fmt.Errorf("failed to compile schedule spec: %w", err)
	}

	jitterSeed := fmt.Sprintf("%s-%s", nsID, scheduleID)

	var expectedNominals []time.Time
	t := start
	for {
		result := compiledSpec.GetNextTime(jitterSeed, t)
		if result.Nominal.IsZero() || result.Nominal.After(end) {
			break
		}
		expectedNominals = append(expectedNominals, result.Nominal)
		t = result.Next // Advance past the jittered time to avoid infinite loops.
	}

	if len(expectedNominals) == 0 {
		return nil, nil
	}

	// 3. List actual workflow executions for this schedule in the time range.
	actualNominals, err := listActualNominalTimes(ctx, wfClient, nsName, scheduleID, baseWorkflowID, start, end)
	if err != nil {
		return nil, err
	}

	// 4. Diff expected vs actual.
	actualSet := make(map[int64]struct{}, len(actualNominals))
	for _, t := range actualNominals {
		actualSet[t.Truncate(time.Second).Unix()] = struct{}{}
	}

	var missed []missedTime
	for _, nominal := range expectedNominals {
		key := nominal.Truncate(time.Second).Unix()
		if _, ok := actualSet[key]; ok {
			continue
		}

		reason := "unknown"
		// Check if this time would have been outside the catchup window.
		// The scheduler drops actions when the gap between "now" (end of processing window)
		// and the scheduled time exceeds the catchup window.
		if end.Sub(nominal) > catchupWindow {
			reason = "outside catchup window"
		}

		missed = append(missed, missedTime{
			ScheduleID:   scheduleID,
			ExpectedTime: nominal.Truncate(time.Second).UTC().Format(time.RFC3339),
			Reason:       reason,
		})
	}

	return missed, nil
}

// listActualNominalTimes queries ListWorkflowExecutions for workflows started by the given schedule
// and extracts the nominal time from each workflow ID.
func listActualNominalTimes(
	ctx context.Context,
	wfClient workflowservice.WorkflowServiceClient,
	nsName string,
	scheduleID string,
	baseWorkflowID string,
	start, end time.Time,
) ([]time.Time, error) {
	query := fmt.Sprintf(
		`TemporalScheduledById = %q AND StartTime >= %q AND StartTime <= %q`,
		scheduleID,
		start.UTC().Format(time.RFC3339Nano),
		end.UTC().Format(time.RFC3339Nano),
	)

	var nominals []time.Time
	var nextPageToken []byte
	prefix := baseWorkflowID + "-"

	for {
		resp, err := wfClient.ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace:     nsName,
			Query:         query,
			NextPageToken: nextPageToken,
		})
		if err != nil {
			return nil, fmt.Errorf("ListWorkflowExecutions failed: %w", err)
		}

		for _, exec := range resp.GetExecutions() {
			wfID := exec.GetExecution().GetWorkflowId()
			if !strings.HasPrefix(wfID, prefix) {
				continue
			}
			timeSuffix := wfID[len(prefix):]
			nominal, err := time.Parse(time.RFC3339, timeSuffix)
			if err != nil {
				continue // Skip IDs we can't parse.
			}
			nominals = append(nominals, nominal)
		}

		nextPageToken = resp.GetNextPageToken()
		if len(nextPageToken) == 0 {
			break
		}
	}

	return nominals, nil
}

// writeMissedTimes outputs the missed times in the requested format.
func writeMissedTimes(c *cli.Context, missed []missedTime, format string) error {
	if len(missed) == 0 {
		return nil
	}

	switch format {
	case "jsonl":
		enc := json.NewEncoder(c.App.Writer)
		for _, m := range missed {
			if err := enc.Encode(m); err != nil {
				return fmt.Errorf("failed to encode JSON: %w", err)
			}
		}
	case "table":
		table := tablewriter.NewWriter(c.App.Writer)
		table.SetBorder(false)
		table.SetColumnSeparator("|")
		table.SetHeader([]string{"Schedule ID", "Expected Time", "Reason"})
		table.SetHeaderLine(false)
		for _, m := range missed {
			table.Append([]string{m.ScheduleID, m.ExpectedTime, m.Reason})
		}
		table.Render()
	}

	return nil
}
