package backoff

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var crontests = []struct {
	cron          string
	scheduledTime string
	now           string
	result        time.Duration
	err           string
}{
	{"0 0 31 2 *", "2018-12-17T08:00:00-08:00", "", 0, "invalid CronSchedule, no time can be found to satisfy the schedule"},
	{"invalid-cron-spec", "2018-12-17T00:04:00+00:00", "2018-12-17T01:02:00+00:00", NoBackoff, "invalid CronSchedule"},

	{"0 10 * * *", "2018-12-17T08:00:00-08:00", "", time.Hour * 18, ""},
	{"0 10 * * *", "2018-12-18T02:00:00-08:00", "", time.Hour * 24, ""},
	{"0 * * * *", "2018-12-17T08:08:00+00:00", "", time.Minute * 52, ""},
	{"0 * * * *", "2018-12-17T09:00:00+00:00", "", time.Hour, ""},
	{"* * * * *", "2018-12-17T08:08:18+00:00", "", time.Second * 42, ""},
	{"0 * * * *", "2018-12-17T09:00:00+00:00", "", time.Minute * 60, ""},
	{"0 10 * * *", "2018-12-17T08:00:00+00:00", "2018-12-20T00:00:00+00:00", time.Hour * 10, ""},
	{"0 10 * * *", "2018-12-17T08:00:00+00:00", "2018-12-17T09:00:00+00:00", time.Hour, ""},
	{"*/10 * * * *", "2018-12-17T00:04:00+00:00", "2018-12-17T01:02:00+00:00", time.Minute * 8, ""},
	{"@every 5h", "2018-12-17T08:00:00+00:00", "2018-12-17T09:00:00+00:00", time.Hour * 4, ""},
	{"@every 5h", "2018-12-17T08:00:00+00:00", "2018-12-18T00:00:00+00:00", time.Hour * 4, ""},
	{"0 3 * * 0-6", "2018-12-17T08:00:00-08:00", "", time.Hour * 11, ""},
	{"@every 30s", "2020-07-17T09:00:02-01:00", "2020-07-17T09:00:02-01:00", time.Second * 30, ""},
	{"@every 30s", "2020-07-17T09:00:02-01:00", "2020-09-17T03:00:53-01:00", time.Second * 9, ""},
	{"@every 30m", "2020-07-17T09:00:00-01:00", "2020-07-17T08:45:00-01:00", time.Minute * 15, ""},
	// At 16:05 East Coast (Day light saving on)
	{"CRON_TZ=America/New_York 5 16 * * *", "2021-03-14T00:00:00-04:00", "2021-03-14T15:05:00-04:00", time.Hour * 1, ""},
	// At 04:05 East Coast (Day light saving off)
	{"CRON_TZ=America/New_York 5 4 * * *", "2021-11-25T00:00:00-05:00", "2021-11-25T03:05:00-05:00", time.Hour * 1, ""},
}

func TestCron(t *testing.T) {
	for idx, tt := range crontests {
		t.Run(strconv.Itoa(idx), func(t *testing.T) {
			start, err := time.Parse(time.RFC3339, tt.scheduledTime)
			assert.NoError(t, err, "Parse start time: %v", tt.scheduledTime)
			end := start
			if tt.now != "" {
				end, err = time.Parse(time.RFC3339, tt.now)
				assert.NoError(t, err, "Parse end time: %v", tt.now)
			}
			err = ValidateSchedule(tt.cron)
			if len(tt.err) > 0 {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.err)
			} else {
				assert.NoError(t, err, "Failed on cron schedule: %v", tt.cron)

				backoff := GetBackoffForNextSchedule(tt.cron, start, end)
				assert.Equal(t, tt.result, backoff, "The cron spec is %s and the expected result is %s", tt.cron, tt.result)
			}
		})
	}
}
