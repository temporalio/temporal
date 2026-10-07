package tests

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/tests/testcore"
)

type ScheduleTestEnv struct {
	*testcore.TestEnv
}

func newScheduleEnv(t *testing.T, opts ...testcore.TestOption) *ScheduleTestEnv {
	t.Helper()
	opts = append(opts, testcore.WithDynamicConfig(dynamicconfig.FrontendAllowedExperiments, []string{"*"}))
	env := testcore.NewEnv(t, opts...)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(chasmContextFactory(testcore.NewContext()), 30*time.Second)
		defer cancel()

		var scheduleIDs []string
		var nextPageToken []byte
		for {
			response, err := env.FrontendClient().ListSchedules(ctx, &workflowservice.ListSchedulesRequest{
				Namespace:       env.Namespace().String(),
				MaximumPageSize: 1000,
				NextPageToken:   nextPageToken,
			})
			if err != nil {
				if t.Failed() {
					t.Logf("schedule cleanup failed: list schedules: %v", err)
				} else {
					t.Errorf("schedule cleanup failed: list schedules: %v", err)
				}
				return
			}
			for _, schedule := range response.GetSchedules() {
				scheduleIDs = append(scheduleIDs, schedule.GetScheduleId())
			}
			nextPageToken = response.GetNextPageToken()
			if len(nextPageToken) == 0 {
				break
			}
		}

		var cleanupErr error
		for _, scheduleID := range scheduleIDs {
			_, err := env.FrontendClient().DeleteSchedule(ctx, &workflowservice.DeleteScheduleRequest{
				Namespace:  env.Namespace().String(),
				ScheduleId: scheduleID,
				Identity:   "test cleanup",
			})
			var notFoundErr *serviceerror.NotFound
			if err != nil && !errors.As(err, &notFoundErr) {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("delete schedule %q: %w", scheduleID, err))
			}
		}
		if cleanupErr != nil {
			if t.Failed() {
				t.Logf("schedule cleanup failed: %v", cleanupErr)
			} else {
				t.Errorf("schedule cleanup failed: %v", cleanupErr)
			}
		}
	})
	return &ScheduleTestEnv{TestEnv: env}
}
