package backoff

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var crontests = []struct {
	cron      string
	startTime string
	endTime   string
	result    time.Duration
}{
	{"0 10 * * *", "2018-12-17T08:00:00-08:00", "", time.Hour * 18},
	{"0 10 * * *", "2018-12-18T02:00:00-08:00", "", time.Hour * 24},
	{"0 * * * *", "2018-12-17T08:08:00+00:00", "", time.Minute * 52},
	{"0 * * * *", "2018-12-17T09:00:00+00:00", "", time.Hour},
	{"* * * * *", "2018-12-17T08:08:18+00:00", "", time.Second * 42},
	{"0 * * * *", "2018-12-17T09:00:00+00:00", "", time.Minute * 60},
	{"0 10 * * *", "2018-12-17T08:00:00+00:00", "2018-12-20T00:00:00+00:00", time.Hour * 10},
	{"0 10 * * *", "2018-12-17T08:00:00+00:00", "2018-12-17T09:00:00+00:00", time.Hour},
	{"*/10 * * * *", "2018-12-17T00:04:00+00:00", "2018-12-17T01:02:00+00:00", time.Minute * 8},
	{"invalid-cron-spec", "2018-12-17T00:04:00+00:00", "2018-12-17T01:02:00+00:00", NoBackoff},
	{"@every 5h", "2018-12-17T08:00:00+00:00", "2018-12-17T09:00:00+00:00", time.Hour * 4},
	{"@every 5h", "2018-12-17T08:00:00+00:00", "2018-12-18T00:00:00+00:00", time.Hour * 4},
	{"0 3 * * 0-6", "2018-12-17T08:00:00-08:00", "", time.Hour * 11},
}

func TestCron(t *testing.T) {
	for idx, tt := range crontests {
		t.Run(strconv.Itoa(idx), func(t *testing.T) {
			start, _ := time.Parse(time.RFC3339, tt.startTime)
			end := start
			if tt.endTime != "" {
				end, _ = time.Parse(time.RFC3339, tt.endTime)
			}
			err := ValidateSchedule(tt.cron)
			if tt.result != NoBackoff {
				assert.NoError(t, err)
			}
			backoff := GetBackoffForNextSchedule(tt.cron, start, end)
			assert.Equal(t, tt.result, backoff, "The cron spec is %s and the expected result is %s", tt.cron, tt.result)
		})
	}
}
