package main

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/primitives"
)

const scenarioWorkflowType = "schedule-v1-replay-fixture-workflow"

type fixtureScenario struct {
	name             string
	spec             client.ScheduleSpec
	overlap          enumspb.ScheduleOverlapPolicy
	runFor           time.Duration
	paused           bool
	immediate        bool
	remainingActions int
	targetActions    int
	interact         func(context.Context, client.ScheduleHandle) error
}

func generateScenarioFixtures(parent context.Context, opts options) error {
	c, err := dialClient(opts, opts.namespace)
	if err != nil {
		return err
	}
	defer c.Close()

	taskQueue := opts.scenarioPrefix + "-task-queue"
	w := worker.New(c, taskQueue, worker.Options{})
	w.RegisterWorkflowWithOptions(scenarioWorkflow, workflow.RegisterOptions{Name: scenarioWorkflowType})
	if err := w.Start(); err != nil {
		return fmt.Errorf("start fixture worker: %w", err)
	}
	defer w.Stop()

	for _, scenario := range fixtureScenarios() {
		if err := generateScenarioFixture(parent, c, opts, taskQueue, scenario); err != nil {
			return fmt.Errorf("scenario %q: %w", scenario.name, err)
		}
	}
	return nil
}

func generateScenarioFixture(
	parent context.Context,
	c client.Client,
	opts options,
	taskQueue string,
	scenario fixtureScenario,
) error {
	ctx, cancel := context.WithTimeout(parent, opts.timeout)
	defer cancel()

	scheduleID := opts.scenarioPrefix + "-" + scenario.name
	handle, err := c.ScheduleClient().Create(ctx, client.ScheduleOptions{
		ID:   scheduleID,
		Spec: scenario.spec,
		Action: &client.ScheduleWorkflowAction{
			ID:        scheduleID + "-action",
			Workflow:  scenarioWorkflowType,
			Args:      []interface{}{scenario.runFor},
			TaskQueue: taskQueue,
		},
		Overlap:            scenario.overlap,
		Paused:             scenario.paused,
		RemainingActions:   scenario.remainingActions,
		TriggerImmediately: scenario.immediate,
	})
	if err != nil {
		return fmt.Errorf("create schedule: %w", err)
	}

	workflowID := primitives.ScheduleWorkflowIDPrefix + scheduleID
	runID, scenarioErr := resolveRunID(ctx, c, workflowID, "")
	if scenarioErr != nil {
		scenarioErr = fmt.Errorf("schedule was not created as a V1 workflow; disable CHASM schedule creation for fixture generation: %w", scenarioErr)
	}
	if scenarioErr == nil && scenario.interact != nil {
		scenarioErr = scenario.interact(ctx, handle)
	}
	if scenarioErr == nil {
		scenarioErr = waitForActions(ctx, handle, scenario.targetActions)
	}
	if scenarioErr == nil {
		settleFor := time.Second
		if scenario.runFor > 0 {
			settleFor = scenario.runFor + time.Second
		}
		timer := time.NewTimer(settleFor)
		select {
		case <-ctx.Done():
			scenarioErr = fmt.Errorf("wait for fixture history to settle: %w", ctx.Err())
		case <-timer.C:
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(parent, 10*time.Second)
	defer cleanupCancel()
	deleteErr := handle.Delete(cleanupCtx)
	if deleteErr != nil {
		deleteErr = fmt.Errorf("delete disposable schedule: %w", deleteErr)
	}
	if scenarioErr != nil || deleteErr != nil {
		return errors.Join(scenarioErr, deleteErr)
	}

	history, err := downloadHistory(ctx, c, workflowID, runID)
	if err != nil {
		return err
	}
	if err := replayHistory(history); err != nil {
		return fmt.Errorf("generated V1 history does not replay: %w", err)
	}
	path := filepath.Join(opts.historyDir, "current-v1", scenario.name+".json.gz")
	if err := writeHistory(path, history); err != nil {
		return err
	}
	fmt.Printf("FIXTURE_PASS scenario=%q events=%d history=%q\n", scenario.name, len(history.Events), path)
	return nil
}

func fixtureScenarios() []fixtureScenario {
	everySecond := client.ScheduleSpec{Intervals: []client.ScheduleIntervalSpec{{Every: time.Second}}}
	quick := func(name string, spec client.ScheduleSpec) fixtureScenario {
		return fixtureScenario{
			name: name, spec: spec, overlap: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			immediate: true, remainingActions: 1, targetActions: 2,
		}
	}
	return []fixtureScenario{
		quick("interval", everySecond),
		quick("calendar", client.ScheduleSpec{Calendars: []client.ScheduleCalendarSpec{{
			Second:     []client.ScheduleRange{{Start: 0, End: 59, Step: 1}},
			Minute:     []client.ScheduleRange{{Start: 0, End: 59, Step: 1}},
			Hour:       []client.ScheduleRange{{Start: 0, End: 23, Step: 1}},
			DayOfMonth: []client.ScheduleRange{{Start: 1, End: 31, Step: 1}},
			Month:      []client.ScheduleRange{{Start: 1, End: 12, Step: 1}},
			DayOfWeek:  []client.ScheduleRange{{Start: 0, End: 6, Step: 1}},
		}}}),
		quick("cron", client.ScheduleSpec{CronExpressions: []string{"*/1 * * * * * *"}}),
		quick("jitter", client.ScheduleSpec{
			Intervals: []client.ScheduleIntervalSpec{{Every: 2 * time.Second}},
			Jitter:    time.Second,
		}),
		{
			name: "update", spec: client.ScheduleSpec{Intervals: []client.ScheduleIntervalSpec{{Every: time.Hour}}},
			overlap: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, paused: true, remainingActions: 2, targetActions: 2,
			interact: func(ctx context.Context, handle client.ScheduleHandle) error {
				if err := handle.Update(ctx, client.ScheduleUpdateOptions{DoUpdate: func(input client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
					input.Description.Schedule.Spec = &everySecond
					return &client.ScheduleUpdate{Schedule: &input.Description.Schedule}, nil
				}}); err != nil {
					return fmt.Errorf("update schedule: %w", err)
				}
				return handle.Unpause(ctx, client.ScheduleUnpauseOptions{Note: "updated fixture"})
			},
		},
		{
			name: "pause-unpause", spec: everySecond, overlap: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			immediate: true, remainingActions: 1, targetActions: 2,
			interact: func(ctx context.Context, handle client.ScheduleHandle) error {
				if err := handle.Pause(ctx, client.SchedulePauseOptions{Note: "fixture pause"}); err != nil {
					return fmt.Errorf("pause schedule: %w", err)
				}
				return handle.Unpause(ctx, client.ScheduleUnpauseOptions{Note: "fixture unpause"})
			},
		},
		{
			name: "backfill-allow-all", spec: everySecond,
			overlap: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, paused: true, targetActions: 3,
			interact: func(ctx context.Context, handle client.ScheduleHandle) error {
				now := time.Now().UTC().Truncate(time.Second)
				return handle.Backfill(ctx, client.ScheduleBackfillOptions{Backfill: []client.ScheduleBackfill{{
					Start: now.Add(-4 * time.Second), End: now,
					Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
				}}})
			},
		},
		{
			name: "buffer-all-running", spec: everySecond,
			overlap: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, runFor: 3 * time.Second,
			immediate: true, remainingActions: 1, targetActions: 2,
		},
		{
			name: "skip-running", spec: everySecond,
			overlap: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, runFor: 3 * time.Second,
			immediate: true, remainingActions: 1, targetActions: 2,
		},
	}
}

func waitForActions(ctx context.Context, handle client.ScheduleHandle, target int) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		description, err := handle.Describe(ctx)
		if err != nil {
			return fmt.Errorf("describe schedule: %w", err)
		}
		if description.Info.NumActions >= target {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for %d actions: %w", target, ctx.Err())
		case <-ticker.C:
		}
	}
}

func scenarioWorkflow(ctx workflow.Context, runFor time.Duration) error {
	if runFor == 0 {
		return nil
	}
	return workflow.Sleep(ctx, runFor)
}
