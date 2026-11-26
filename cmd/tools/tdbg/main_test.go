package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/service/worker/scheduler"
)

func TestScheduleTimezones(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		zone  string
		start string
		want  []string
	}{
		{
			name:  "New York spring DST",
			zone:  "America/New_York",
			start: "2026-03-07T00:00:00Z",
			want:  []string{"2026-03-07T14:00:00Z", "2026-03-08T13:00:00Z"},
		},
		{
			name:  "New York fall DST",
			zone:  "America/New_York",
			start: "2026-10-31T00:00:00Z",
			want:  []string{"2026-10-31T13:00:00Z", "2026-11-01T14:00:00Z"},
		},
		{
			name:  "Kolkata half hour offset",
			zone:  "Asia/Kolkata",
			start: "2026-03-07T00:00:00Z",
			want:  []string{"2026-03-07T03:30:00Z", "2026-03-08T03:30:00Z"},
		},
		{
			name:  "Etc UTC named zone",
			zone:  "Etc/UTC",
			start: "2026-03-07T00:00:00Z",
			want:  []string{"2026-03-07T09:00:00Z", "2026-03-08T09:00:00Z"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			builder := scheduler.NewSpecBuilder(
				dynamicconfig.GetIntPropertyFn(scheduler.DefaultWarnIterations),
				dynamicconfig.GetIntPropertyFn(0),
			)
			compiled, err := builder.NewCompiledSpec(&schedulepb.ScheduleSpec{
				CronString:   []string{"0 9 * * *"},
				TimezoneName: tc.zone,
			})
			require.NoError(t, err)
			cursor, err := time.Parse(time.RFC3339, tc.start)
			require.NoError(t, err)
			var got []string
			for range tc.want {
				next, err := compiled.GetNextTime("", cursor)
				require.NoError(t, err)
				got = append(got, next.Nominal.UTC().Format(time.RFC3339))
				cursor = next.Next
			}
			require.Equal(t, tc.want, got)
		})
	}
}
