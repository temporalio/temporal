// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package backoff

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_NextCronSchedule(t *testing.T) {
	a := assert.New(t)

	// every day cron
	now, _ := time.Parse(time.RFC3339, "2018-12-17T08:00:00-08:00") // UTC: 2018-12-17 16:00:00 +0000 UTC
	cronSpec := "0 10 * * *"
	backoff := GetBackoffForNextSchedule(cronSpec, now)
	a.Equal(time.Hour*18, backoff)
	backoff = GetBackoffForNextSchedule(cronSpec, now.Add(backoff))
	a.Equal(time.Hour*24, backoff)

	// every hour cron
	now, _ = time.Parse(time.RFC3339, "2018-12-17T08:08:00+00:00")
	cronSpec = "0 * * * *"
	backoff = GetBackoffForNextSchedule(cronSpec, now)
	a.Equal(time.Minute*52, backoff)
	backoff = GetBackoffForNextSchedule(cronSpec, now.Add(backoff))
	a.Equal(time.Hour, backoff)

	// every minute cron
	now, _ = time.Parse(time.RFC3339, "2018-12-17T08:08:18+00:00")
	cronSpec = "* * * * *"
	backoff = GetBackoffForNextSchedule(cronSpec, now)
	a.Equal(time.Second*42, backoff)
	backoff = GetBackoffForNextSchedule(cronSpec, now.Add(backoff))
	a.Equal(time.Minute, backoff)

	// invalid cron spec
	cronSpec = "invalid-cron-spec"
	backoff = GetBackoffForNextSchedule(cronSpec, now)
	a.Equal(NoBackoff, backoff)
}
