package health

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/stats"
)

func TestRecordAndRead(t *testing.T) {
	type recordCall struct {
		key     string
		latency time.Duration
		err     error
	}

	// groupExpectation is what one bucket should read back after the records are applied.
	// p50/p99 are latency and are asserted with a delta since the t-digest is approximate
	type groupExpectation struct {
		p50        float64
		p99        float64
		errorRatio float64
	}

	testCases := []struct {
		desc string

		settings Settings
		records  []recordCall
		// isUnhealthy, when set, is passed via WithIsUnhealthy; nil counts every error
		isUnhealthy func(error) bool

		// expected is keyed by group name; use overallGroupName for the overall bucket
		expected map[string]groupExpectation
	}{
		{
			desc: "overall latency and error ratio",
			settings: Settings{
				Overall: Thresholds{
					WindowConfig:        &stats.WindowConfig{WindowSize: 5 * time.Second, WindowCount: 10},
					ErrorRatioThreshold: &ErrorRatioThreshold{WindowSize: 10 * time.Second, BufferSize: 5000, Threshold: 0.5},
				},
			},
			records: []recordCall{
				{key: "a", latency: 100 * time.Millisecond},
				{key: "a", latency: 100 * time.Millisecond},
				{key: "a", latency: 100 * time.Millisecond},
				{key: "a", latency: 100 * time.Millisecond},
				{key: "a", latency: 100 * time.Millisecond},
				{key: "a", latency: 100 * time.Millisecond},
				{key: "a", latency: 100 * time.Millisecond},
				{key: "a", latency: 100 * time.Millisecond},
				// 2 of 10 calls failed -> 0.2 error ratio
				{key: "a", latency: 500 * time.Millisecond, err: errors.New("boom")},
				{key: "a", latency: 500 * time.Millisecond, err: errors.New("boom")},
			},
			expected: map[string]groupExpectation{
				overallGroupName: {p50: 100, p99: 500, errorRatio: 0.2},
			},
		},
		{
			desc: "critical group isolates traffic that overall dilutes",
			settings: Settings{
				Overall: Thresholds{
					WindowConfig:        &stats.WindowConfig{WindowSize: 5 * time.Second, WindowCount: 10},
					ErrorRatioThreshold: &ErrorRatioThreshold{WindowSize: 10 * time.Second, BufferSize: 5000},
				},
				Groups: []Group{
					{
						Name: "critical",
						Keys: []string{"crit"},
						// tighter windows than overall
						Thresholds: Thresholds{
							WindowConfig:        &stats.WindowConfig{WindowSize: 2 * time.Second, WindowCount: 10},
							ErrorRatioThreshold: &ErrorRatioThreshold{WindowSize: 5 * time.Second, BufferSize: 1000},
						},
					},
				},
			},
			records: []recordCall{
				// normal traffic: ungrouped, fast and healthy, only feeds overall
				{key: "norm", latency: 100 * time.Millisecond},
				{key: "norm", latency: 100 * time.Millisecond},
				{key: "norm", latency: 100 * time.Millisecond},
				{key: "norm", latency: 100 * time.Millisecond},
				{key: "norm", latency: 100 * time.Millisecond},
				{key: "norm", latency: 100 * time.Millisecond},
				{key: "norm", latency: 100 * time.Millisecond},
				{key: "norm", latency: 100 * time.Millisecond},
				{key: "norm", latency: 100 * time.Millisecond},
				{key: "norm", latency: 100 * time.Millisecond},
				{key: "norm", latency: 100 * time.Millisecond},
				{key: "norm", latency: 100 * time.Millisecond},
				// critical traffic: feeds both overall and the critical group;
				// 2 of 8 slow calls failed -> 0.25 for the group
				{key: "crit", latency: 200 * time.Millisecond},
				{key: "crit", latency: 200 * time.Millisecond},
				{key: "crit", latency: 200 * time.Millisecond},
				{key: "crit", latency: 200 * time.Millisecond},
				{key: "crit", latency: 200 * time.Millisecond},
				{key: "crit", latency: 200 * time.Millisecond},
				{key: "crit", latency: 800 * time.Millisecond, err: errors.New("boom")},
				{key: "crit", latency: 800 * time.Millisecond, err: errors.New("boom")},
			},
			expected: map[string]groupExpectation{
				// overall is dominated by the healthy normal traffic: 2 of 20 failed -> 0.1
				overallGroupName: {p50: 100, p99: 800, errorRatio: 0.1},
				// the critical group surfaces the problem the overall bucket hides
				"critical": {p50: 200, p99: 800, errorRatio: 0.25},
			},
		},
		{
			desc: "isUnhealthy classifier excludes some errors from the ratio",
			// only errors that aren't "ignored" count toward the ratio
			isUnhealthy: func(err error) bool { return err.Error() != "ignored" },
			settings: Settings{
				Overall: Thresholds{
					WindowConfig:        &stats.WindowConfig{WindowSize: 5 * time.Second, WindowCount: 10},
					ErrorRatioThreshold: &ErrorRatioThreshold{WindowSize: 10 * time.Second, BufferSize: 5000},
				},
			},
			records: []recordCall{
				{key: "a", latency: 100 * time.Millisecond},
				{key: "a", latency: 100 * time.Millisecond},
				{key: "a", latency: 100 * time.Millisecond},
				{key: "a", latency: 100 * time.Millisecond},
				{key: "a", latency: 100 * time.Millisecond},
				{key: "a", latency: 100 * time.Millisecond},
				{key: "a", latency: 100 * time.Millisecond},
				{key: "a", latency: 100 * time.Millisecond},
				// both are errors, but only the counted one is classified unhealthy -> 1 of 10
				{key: "a", latency: 100 * time.Millisecond, err: errors.New("ignored")},
				{key: "a", latency: 100 * time.Millisecond, err: errors.New("counted")},
			},
			expected: map[string]groupExpectation{
				overallGroupName: {p50: 100, p99: 100, errorRatio: 0.1},
			},
		},
		{
			desc: "latency-only and error-ratio-only groups",
			settings: Settings{
				Overall: Thresholds{
					WindowConfig:        &stats.WindowConfig{WindowSize: 5 * time.Second, WindowCount: 10},
					ErrorRatioThreshold: &ErrorRatioThreshold{WindowSize: 10 * time.Second, BufferSize: 5000},
				},
				Groups: []Group{
					{
						Name: "latencyonly",
						Keys: []string{"lat"},
						// no ErrorRatioThreshold -> error ratio not tracked for this group
						Thresholds: Thresholds{
							WindowConfig: &stats.WindowConfig{WindowSize: 5 * time.Second, WindowCount: 10},
						},
					},
					{
						Name: "erroronly",
						Keys: []string{"err"},
						// no WindowConfig -> latency not tracked for this group
						Thresholds: Thresholds{
							ErrorRatioThreshold: &ErrorRatioThreshold{WindowSize: 10 * time.Second, BufferSize: 5000},
						},
					},
				},
			},
			records: []recordCall{
				// latency-only group: latency tracked, ratio reads 0
				{key: "lat", latency: 100 * time.Millisecond},
				{key: "lat", latency: 100 * time.Millisecond},
				{key: "lat", latency: 100 * time.Millisecond},
				{key: "lat", latency: 100 * time.Millisecond},
				{key: "lat", latency: 100 * time.Millisecond},
				{key: "lat", latency: 100 * time.Millisecond},
				{key: "lat", latency: 100 * time.Millisecond},
				{key: "lat", latency: 100 * time.Millisecond},
				{key: "lat", latency: 500 * time.Millisecond},
				{key: "lat", latency: 500 * time.Millisecond},
				// error-only group: ratio tracked (3 of 10 fail), latency reads 0
				{key: "err", latency: 100 * time.Millisecond},
				{key: "err", latency: 100 * time.Millisecond},
				{key: "err", latency: 100 * time.Millisecond},
				{key: "err", latency: 100 * time.Millisecond},
				{key: "err", latency: 100 * time.Millisecond},
				{key: "err", latency: 100 * time.Millisecond},
				{key: "err", latency: 100 * time.Millisecond},
				{key: "err", latency: 100 * time.Millisecond, err: errors.New("boom")},
				{key: "err", latency: 100 * time.Millisecond, err: errors.New("boom")},
				{key: "err", latency: 100 * time.Millisecond, err: errors.New("boom")},
			},
			expected: map[string]groupExpectation{
				"latencyonly": {p50: 100, p99: 500, errorRatio: 0},
				"erroronly":   {p50: 0, p99: 0, errorRatio: 0.3},
			},
		},
		{
			desc: "group aggregates multiple keys",
			settings: Settings{
				Overall: Thresholds{
					WindowConfig:        &stats.WindowConfig{WindowSize: 5 * time.Second, WindowCount: 10},
					ErrorRatioThreshold: &ErrorRatioThreshold{WindowSize: 10 * time.Second, BufferSize: 5000},
				},
				Groups: []Group{
					{
						Name: "critical",
						Keys: []string{"a", "b"},
						Thresholds: Thresholds{
							WindowConfig:        &stats.WindowConfig{WindowSize: 2 * time.Second, WindowCount: 10},
							ErrorRatioThreshold: &ErrorRatioThreshold{WindowSize: 5 * time.Second, BufferSize: 1000},
						},
					},
				},
			},
			records: []recordCall{
				// both keys feed the one critical bucket: 8@200 + 2@800, 2 of 10 fail
				{key: "a", latency: 200 * time.Millisecond},
				{key: "a", latency: 200 * time.Millisecond},
				{key: "a", latency: 200 * time.Millisecond},
				{key: "a", latency: 200 * time.Millisecond},
				{key: "b", latency: 200 * time.Millisecond},
				{key: "b", latency: 200 * time.Millisecond},
				{key: "b", latency: 200 * time.Millisecond},
				{key: "b", latency: 200 * time.Millisecond},
				{key: "a", latency: 800 * time.Millisecond, err: errors.New("boom")},
				{key: "b", latency: 800 * time.Millisecond, err: errors.New("boom")},
			},
			expected: map[string]groupExpectation{
				"critical": {p50: 200, p99: 800, errorRatio: 0.2},
			},
		},
		{
			desc: "key claimed by two groups feeds only the last one",
			settings: Settings{
				Overall: Thresholds{
					WindowConfig:        &stats.WindowConfig{WindowSize: 5 * time.Second, WindowCount: 10},
					ErrorRatioThreshold: &ErrorRatioThreshold{WindowSize: 10 * time.Second, BufferSize: 5000},
				},
				Groups: []Group{
					{
						Name: "first",
						Keys: []string{"dup"},
						Thresholds: Thresholds{
							WindowConfig:        &stats.WindowConfig{WindowSize: 2 * time.Second, WindowCount: 10},
							ErrorRatioThreshold: &ErrorRatioThreshold{WindowSize: 5 * time.Second, BufferSize: 1000},
						},
					},
					{
						Name: "second",
						Keys: []string{"dup"},
						Thresholds: Thresholds{
							WindowConfig:        &stats.WindowConfig{WindowSize: 2 * time.Second, WindowCount: 10},
							ErrorRatioThreshold: &ErrorRatioThreshold{WindowSize: 5 * time.Second, BufferSize: 1000},
						},
					},
				},
			},
			records: []recordCall{
				{key: "dup", latency: 200 * time.Millisecond},
				{key: "dup", latency: 200 * time.Millisecond},
				{key: "dup", latency: 200 * time.Millisecond},
				{key: "dup", latency: 200 * time.Millisecond},
				{key: "dup", latency: 200 * time.Millisecond},
				{key: "dup", latency: 200 * time.Millisecond},
				{key: "dup", latency: 200 * time.Millisecond},
				{key: "dup", latency: 200 * time.Millisecond},
				// 2 of 10 failed -> 0.2
				{key: "dup", latency: 800 * time.Millisecond, err: errors.New("boom")},
				{key: "dup", latency: 800 * time.Millisecond, err: errors.New("boom")},
			},
			expected: map[string]groupExpectation{
				// refresh binds each key to one bucket, so the later group silently wins and
				// "first" is left configured but never fed
				"first":  {p50: 0, p99: 0, errorRatio: 0},
				"second": {p50: 200, p99: 800, errorRatio: 0.2},
			},
		},
		{
			desc: "empty overall settings fall back to defaults",
			// Overall left zero-valued; refresh seeds fallback latency + error-ratio windows
			settings: Settings{},
			records: []recordCall{
				{key: "a", latency: 100 * time.Millisecond},
				{key: "a", latency: 100 * time.Millisecond},
				{key: "a", latency: 100 * time.Millisecond},
				{key: "a", latency: 100 * time.Millisecond},
				{key: "a", latency: 100 * time.Millisecond},
				{key: "a", latency: 100 * time.Millisecond},
				{key: "a", latency: 100 * time.Millisecond},
				{key: "a", latency: 100 * time.Millisecond},
				{key: "a", latency: 500 * time.Millisecond, err: errors.New("boom")},
				{key: "a", latency: 500 * time.Millisecond, err: errors.New("boom")},
			},
			expected: map[string]groupExpectation{
				overallGroupName: {p50: 100, p99: 500, errorRatio: 0.2},
			},
		},
		{
			desc: "all calls succeed -> zero error ratio",
			settings: Settings{
				Overall: Thresholds{
					WindowConfig:        &stats.WindowConfig{WindowSize: 5 * time.Second, WindowCount: 10},
					ErrorRatioThreshold: &ErrorRatioThreshold{WindowSize: 10 * time.Second, BufferSize: 5000},
				},
			},
			records: []recordCall{
				{key: "a", latency: 100 * time.Millisecond},
				{key: "a", latency: 100 * time.Millisecond},
				{key: "a", latency: 100 * time.Millisecond},
				{key: "a", latency: 100 * time.Millisecond},
				{key: "a", latency: 100 * time.Millisecond},
			},
			expected: map[string]groupExpectation{
				overallGroupName: {p50: 100, p99: 100, errorRatio: 0},
			},
		},
		{
			desc: "all calls fail -> full error ratio",
			settings: Settings{
				Overall: Thresholds{
					WindowConfig:        &stats.WindowConfig{WindowSize: 5 * time.Second, WindowCount: 10},
					ErrorRatioThreshold: &ErrorRatioThreshold{WindowSize: 10 * time.Second, BufferSize: 5000},
				},
			},
			records: []recordCall{
				{key: "a", latency: 100 * time.Millisecond, err: errors.New("boom")},
				{key: "a", latency: 100 * time.Millisecond, err: errors.New("boom")},
				{key: "a", latency: 100 * time.Millisecond, err: errors.New("boom")},
				{key: "a", latency: 100 * time.Millisecond, err: errors.New("boom")},
				{key: "a", latency: 100 * time.Millisecond, err: errors.New("boom")},
			},
			expected: map[string]groupExpectation{
				overallGroupName: {p50: 100, p99: 100, errorRatio: 1},
			},
		},
		{
			desc: "ungrouped key feeds only overall",
			settings: Settings{
				Overall: Thresholds{
					WindowConfig:        &stats.WindowConfig{WindowSize: 5 * time.Second, WindowCount: 10},
					ErrorRatioThreshold: &ErrorRatioThreshold{WindowSize: 10 * time.Second, BufferSize: 5000},
				},
				Groups: []Group{
					{
						Name: "critical",
						Keys: []string{"crit"},
						Thresholds: Thresholds{
							WindowConfig:        &stats.WindowConfig{WindowSize: 2 * time.Second, WindowCount: 10},
							ErrorRatioThreshold: &ErrorRatioThreshold{WindowSize: 5 * time.Second, BufferSize: 1000},
						},
					},
				},
			},
			records: []recordCall{
				// only ungrouped "norm" traffic; the critical group never sees a record
				{key: "norm", latency: 100 * time.Millisecond},
				{key: "norm", latency: 100 * time.Millisecond},
				{key: "norm", latency: 100 * time.Millisecond},
				{key: "norm", latency: 100 * time.Millisecond},
				{key: "norm", latency: 100 * time.Millisecond},
				{key: "norm", latency: 100 * time.Millisecond},
				{key: "norm", latency: 100 * time.Millisecond},
				{key: "norm", latency: 100 * time.Millisecond},
				{key: "norm", latency: 500 * time.Millisecond, err: errors.New("boom")},
				{key: "norm", latency: 500 * time.Millisecond, err: errors.New("boom")},
			},
			expected: map[string]groupExpectation{
				overallGroupName: {p50: 100, p99: 500, errorRatio: 0.2},
				"critical":       {p50: 0, p99: 0, errorRatio: 0},
			},
		},
		{
			desc: "unknown group reads zero",
			settings: Settings{
				Overall: Thresholds{
					WindowConfig:        &stats.WindowConfig{WindowSize: 5 * time.Second, WindowCount: 10},
					ErrorRatioThreshold: &ErrorRatioThreshold{WindowSize: 10 * time.Second, BufferSize: 5000},
				},
			},
			records: []recordCall{
				{key: "a", latency: 100 * time.Millisecond},
				{key: "a", latency: 100 * time.Millisecond},
				{key: "a", latency: 100 * time.Millisecond, err: errors.New("boom")},
			},
			expected: map[string]groupExpectation{
				"does-not-exist": {p50: 0, p99: 0, errorRatio: 0},
			},
		},
		{
			desc: "no records reads zero",
			settings: Settings{
				Overall: Thresholds{
					WindowConfig:        &stats.WindowConfig{WindowSize: 5 * time.Second, WindowCount: 10},
					ErrorRatioThreshold: &ErrorRatioThreshold{WindowSize: 10 * time.Second, BufferSize: 5000},
				},
				Groups: []Group{
					{
						Name: "critical",
						Keys: []string{"crit"},
						Thresholds: Thresholds{
							WindowConfig:        &stats.WindowConfig{WindowSize: 2 * time.Second, WindowCount: 10},
							ErrorRatioThreshold: &ErrorRatioThreshold{WindowSize: 5 * time.Second, BufferSize: 1000},
						},
					},
				},
			},
			records: []recordCall{},
			expected: map[string]groupExpectation{
				overallGroupName: {p50: 0, p99: 0, errorRatio: 0},
				"critical":       {p50: 0, p99: 0, errorRatio: 0},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			var opts []Option
			if tc.isUnhealthy != nil {
				opts = append(opts, WithIsUnhealthy(tc.isUnhealthy))
			}

			s := NewSignalAggregator(
				log.NewNoopLogger(),
				func() Settings { return tc.settings },
				opts...,
			)

			for _, r := range tc.records {
				s.Record(r.key, r.latency, r.err)
			}

			for group, exp := range tc.expected {
				errorRatio, _ := s.ErrorRatioByGroup(group)
				require.InDelta(t, exp.errorRatio, errorRatio, 0.001, "group %q error ratio", group)

				p50, _ := s.LatencyQuantileByGroup(group, 0.5)
				require.InDelta(t, exp.p50, p50, 1, "group %q p50", group)

				p99, _ := s.LatencyQuantileByGroup(group, 0.99)
				require.InDelta(t, exp.p99, p99, 1, "group %q p99", group)
			}
		})
	}
}

func TestRefresh(t *testing.T) {
	overall := Thresholds{
		WindowConfig:        &stats.WindowConfig{WindowSize: 5 * time.Second, WindowCount: 10},
		ErrorRatioThreshold: &ErrorRatioThreshold{WindowSize: 10 * time.Second, BufferSize: 5000},
	}
	critical := Group{
		Name: "critical",
		Keys: []string{"crit"},
		Thresholds: Thresholds{
			WindowConfig:        &stats.WindowConfig{WindowSize: 2 * time.Second, WindowCount: 10},
			ErrorRatioThreshold: &ErrorRatioThreshold{WindowSize: 5 * time.Second, BufferSize: 1000},
		},
	}

	t.Run("adding a group starts a new bucket", func(t *testing.T) {
		settings := Settings{Overall: overall}
		s := NewSignalAggregator(log.NewNoopLogger(), func() Settings { return settings })

		// the group doesn't exist yet, so the lookup misses rather than reading zero
		_, found := s.ErrorRatioByGroup("critical")
		require.False(t, found)

		// add the critical group and pick it up
		settings = Settings{Overall: overall, Groups: []Group{critical}}
		s.refresh()

		s.Record("crit", 200*time.Millisecond, nil)
		s.Record("crit", 200*time.Millisecond, nil)
		s.Record("crit", 200*time.Millisecond, nil)
		// 1 of 4 failed -> 0.25
		s.Record("crit", 800*time.Millisecond, errors.New("boom"))

		p50, found := s.LatencyQuantileByGroup("critical", 0.5)
		require.True(t, found)
		require.InDelta(t, 200, p50, 1)

		errorRatio, found := s.ErrorRatioByGroup("critical")
		require.True(t, found)
		require.InDelta(t, 0.25, errorRatio, 0.001)
	})

	t.Run("removing a group drops its bucket", func(t *testing.T) {
		settings := Settings{Overall: overall, Groups: []Group{critical}}
		s := NewSignalAggregator(log.NewNoopLogger(), func() Settings { return settings })

		s.Record("crit", 200*time.Millisecond, errors.New("boom"))
		errorRatio, found := s.ErrorRatioByGroup("critical")
		require.True(t, found)
		require.InDelta(t, 1, errorRatio, 0.001)

		// drop the group; its bucket goes away entirely
		settings = Settings{Overall: overall}
		s.refresh()

		_, found = s.ErrorRatioByGroup("critical")
		require.False(t, found)

		_, found = s.LatencyQuantileByGroup("critical", 0.5)
		require.False(t, found)
	})

	t.Run("rebuilding a bucket resets its recorded data", func(t *testing.T) {
		settings := Settings{Overall: overall}
		s := NewSignalAggregator(log.NewNoopLogger(), func() Settings { return settings })

		// 1 of 2 failed -> 0.5
		s.Record("a", 100*time.Millisecond, errors.New("boom"))
		s.Record("a", 100*time.Millisecond, nil)

		errorRatio, _ := s.ErrorRatioByGroup(overallGroupName)
		require.InDelta(t, 0.5, errorRatio, 0.001)

		p50, _ := s.LatencyQuantileByGroup(overallGroupName, 0.5)
		require.InDelta(t, 100, p50, 1)

		// any settings change rebuilds the maps, discarding recorded data
		settings = Settings{
			Overall: Thresholds{
				WindowConfig:        &stats.WindowConfig{WindowSize: 30 * time.Second, WindowCount: 10},
				ErrorRatioThreshold: &ErrorRatioThreshold{WindowSize: 10 * time.Second, BufferSize: 5000},
			},
		}
		s.refresh()

		// the buckets still exist, they just read zero again
		errorRatio, found := s.ErrorRatioByGroup(overallGroupName)
		require.True(t, found)
		require.Zero(t, errorRatio)

		p50, found = s.LatencyQuantileByGroup(overallGroupName, 0.5)
		require.True(t, found)
		require.Zero(t, p50)
	})

	t.Run("refresh with unchanged settings keeps recorded data", func(t *testing.T) {
		settings := Settings{Overall: overall}
		s := NewSignalAggregator(log.NewNoopLogger(), func() Settings { return settings })

		// 1 of 2 failed -> 0.5
		s.Record("a", 100*time.Millisecond, errors.New("boom"))
		s.Record("a", 100*time.Millisecond, nil)

		// settings are identical, so refresh is a no-op and does not rebuild the buckets
		s.refresh()

		errorRatio, _ := s.ErrorRatioByGroup(overallGroupName)
		require.InDelta(t, 0.5, errorRatio, 0.001)

		p50, _ := s.LatencyQuantileByGroup(overallGroupName, 0.5)
		require.InDelta(t, 100, p50, 1)
	})
}

func TestSettingsChanged(t *testing.T) {
	testCases := []struct {
		desc string
		// seeded controls whether latencyByGroup is non-nil, i.e. refresh has run at least once
		seeded       bool
		lastSettings Settings
		newSettings  Settings
		expected     bool
	}{
		{
			desc:   "seeded, identical settings",
			seeded: true,
			lastSettings: Settings{
				Overall: Thresholds{
					WindowConfig: &stats.WindowConfig{WindowSize: 5 * time.Second, WindowCount: 10},
				},
			},
			newSettings: Settings{
				Overall: Thresholds{
					WindowConfig: &stats.WindowConfig{WindowSize: 5 * time.Second, WindowCount: 10},
				},
			},
			expected: false,
		},
		{
			desc:     "seeded, both empty settings",
			seeded:   true,
			expected: false,
		},
		{
			// nil latencyByGroup means refresh hasn't seeded the maps yet, so it always
			// reports changed regardless of equality
			desc:     "not seeded, identical settings",
			seeded:   false,
			expected: true,
		},
		{
			desc:   "overall window config changed",
			seeded: true,
			lastSettings: Settings{
				Overall: Thresholds{
					WindowConfig: &stats.WindowConfig{WindowSize: 5 * time.Second, WindowCount: 10},
				},
			},
			newSettings: Settings{
				Overall: Thresholds{
					WindowConfig: &stats.WindowConfig{WindowSize: 10 * time.Second, WindowCount: 10},
				},
			},
			expected: true,
		},
		{
			desc:   "overall window config added",
			seeded: true,
			newSettings: Settings{
				Overall: Thresholds{
					WindowConfig: &stats.WindowConfig{WindowSize: 5 * time.Second, WindowCount: 10},
				},
			},
			expected: true,
		},
		{
			desc:   "overall error ratio threshold changed",
			seeded: true,
			lastSettings: Settings{
				Overall: Thresholds{
					ErrorRatioThreshold: &ErrorRatioThreshold{WindowSize: 10 * time.Second, BufferSize: 5000, Threshold: 0.5},
				},
			},
			newSettings: Settings{
				Overall: Thresholds{
					ErrorRatioThreshold: &ErrorRatioThreshold{WindowSize: 10 * time.Second, BufferSize: 5000, Threshold: 0.8},
				},
			},
			expected: true,
		},
		{
			desc:   "overall enforced flag changed",
			seeded: true,
			lastSettings: Settings{
				Overall: Thresholds{Enforced: false},
			},
			newSettings: Settings{
				Overall: Thresholds{Enforced: true},
			},
			expected: true,
		},
		{
			desc:   "group added",
			seeded: true,
			newSettings: Settings{
				Groups: []Group{
					{Name: "frontend", Keys: []string{"a", "b"}},
				},
			},
			expected: true,
		},
		{
			desc:   "group keys changed",
			seeded: true,
			lastSettings: Settings{
				Groups: []Group{
					{Name: "frontend", Keys: []string{"a"}},
				},
			},
			newSettings: Settings{
				Groups: []Group{
					{Name: "frontend", Keys: []string{"a", "b"}},
				},
			},
			expected: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			s := SignalAggregator{
				lastSettings: tc.lastSettings,
			}
			if tc.seeded {
				s.latencyByGroup = map[string]stats.TimeWindowedStats{}
			}

			actual := s.settingsChanged(tc.newSettings)

			require.Equal(t, tc.expected, actual)
		})
	}
}
