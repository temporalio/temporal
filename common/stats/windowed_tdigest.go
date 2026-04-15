package stats

import (
	"errors"
	"sync"
	"time"

	"github.com/caio/go-tdigest/v5"
)

type (
	// TimeWindowedStats collects values into fixed-duration time windows,
	// each backed by a t-digest, enabling approximate quantile queries
	// over a sliding time range.
	// Windows are guaranteed to be non-overlapping.
	TimeWindowedStats interface {
		// Record records a value that occurred at the given timestamp.
		Record(value float64, timestamp time.Time)
		// RecordToLatestWindow records a value in the current time window.
		// This will call time.Now(), don't call this in a tight loop.
		RecordToLatestWindow(value float64)
		// RecordMulti records a weighted value that occurred at the given timestamp
		// with the provided count. Imagine you have 3 counts of 10, and you
		// call RecordMulti(20, t, 3). Your new mean will be 15 because you added
		// 3 counts of 20.
		RecordMulti(value float64, timestamp time.Time, count uint64)
		// RecordMultiToLatestWindow records a weighted value in the current time window.
		// This will call time.Now(), don't call this in a tight loop.
		RecordMultiToLatestWindow(value float64, count uint64)
		// Quantile returns the approximate value at quantile q (0 <= q <= 1).
		// Aggregates across all active windows.
		Quantile(q float64) float64
		// TrimmedMean returns the mean of the values within the given quantile range (0 <= q <= 1).
		// For example, TrimmedMean(0.9, 0.99) returns the average of all the values between the
		// 90th percentile and the 99th percentile.
		// Aggregates across all active windows.
		TrimmedMean(lowerQuantile, upperQuantile float64) float64
		// SubWindowForTime returns a view of the time window containing the given instant.
		// Use this to get detailed information about a specific time window.
		SubWindowForTime(instant time.Time) TimeWindowView
	}

	// TimeWindowView is a read-only view of a single time window.
	TimeWindowView interface {
		// Quantile returns the approximate value at quantile q (0 <= q <= 1)
		Quantile(q float64) float64
		// TrimmedMean returns the mean of the values within the given quantile range (0 <= q <= 1).
		// For example, TrimmedMean(0.9, 0.99) returns the average of all the values between the
		// 90th percentile and the 99th percentile.
		TrimmedMean(lowerQuantile, upperQuantile float64) float64
		Start() time.Time
		End() time.Time
	}

	// WindowConfig controls the windowing behavior.
	WindowConfig struct {
		// windowSize is the duration of each window.
		WindowSize time.Duration
		// windowCount is the maximum number of windows to retain.
		WindowCount int
		// FillBlankIntervals controls whether gaps in the windowing should be preserved.
		// This matters when windowSize is shorter than the largest gap in the measured data
		// and individual time-windows are being fetched via TimeWindowedStats.SubWindowForTime.
		FillBlankIntervals bool
	}

	timeWindowedTDigest struct {
		mu      sync.Mutex
		windows []timedWindow // ring buffer, pre-allocated to windowCount
		head    int           // index of the newest window
		cfg     WindowConfig
	}

	timedWindow struct {
		tdigest *tdigest.TDigest
		start   time.Time
		end     time.Time
	}
)

// Compile-time assertion that timeWindowedTDigest implements TimeWindowedStats.
var _ TimeWindowedStats = (*timeWindowedTDigest)(nil)

// NewWindowedTDigest creates a new TimeWindowedStats backed by per-window t-digests.
// Some guidance on sizing the windows/counts: Prefer windows containing 100-1000 datapoints.
// T-Digest is somewhat super-linear with datapoint count, and 1000 is a breakpoint with the
// cost of just creating a new window. See BenchmarkInsert in windowed_tdigest_performance_test.go for details.
func NewWindowedTDigest(cfg WindowConfig) (TimeWindowedStats, error) {
	if cfg.WindowCount <= 0 {
		return nil, errors.New("windowCount must be non-negative")
	}
	if cfg.WindowSize.Milliseconds() <= 50 {
		return nil, errors.New("probable misconfiguration detected: windowSize is too small, consider increasing it to at least 50ms")
	}
	return &timeWindowedTDigest{
		windows: make([]timedWindow, cfg.WindowCount),
		cfg:     cfg,
		// mu and head both empty
	}, nil
}

func (w *timeWindowedTDigest) Record(value float64, timestamp time.Time) {
	w.RecordMulti(value, timestamp, 1)
}

func (w *timeWindowedTDigest) RecordMulti(value float64, timestamp time.Time, count uint64) {
	window, err := w.getOrCreateWindow(timestamp)
	if err != nil {
		// Drop data for invalid timestamps
		return
	}
	_ = window.tdigest.AddWeighted(value, count)
}

func (w *timeWindowedTDigest) RecordToLatestWindow(value float64) {
	w.RecordMultiToLatestWindow(value, 1)
}

func (w *timeWindowedTDigest) RecordMultiToLatestWindow(value float64, count uint64) {
	window, err := w.getOrCreateWindow(time.Time{})
	if err != nil {
		return
	}
	_ = window.tdigest.AddWeighted(value, count)
}

func (w *timeWindowedTDigest) SubWindowForTime(instant time.Time) TimeWindowView {
	w.mu.Lock()
	defer w.mu.Unlock()
	window, _ := w.searchWindowsBackwards(instant)
	return window
}

func (w *timeWindowedTDigest) Quantile(q float64) float64 {
	return w.getMergedWindows().Quantile(q)
}

func (w *timeWindowedTDigest) TrimmedMean(lowerQuantile, upperQuantile float64) float64 {
	return w.getMergedWindows().TrimmedMean(lowerQuantile, upperQuantile)
}

// TODO: this is expensive, maybe cache everything but the latest window so we can skip N merges?
func (w *timeWindowedTDigest) getMergedWindows() *tdigest.TDigest {
	windows := w.cloneWindows()
	merged, _ := tdigest.New()
	for idx := range windows {
		if windows[idx].tdigest != nil {
			_ = merged.Merge(windows[idx].tdigest)
		}
	}
	return merged
}

// cloneWindows creates a shallow copy of the ring buffer.
// Use this to avoid holding the lock while accessing the windows for aggregated queries.
func (w *timeWindowedTDigest) cloneWindows() []timedWindow {
	w.mu.Lock()
	defer w.mu.Unlock()
	windows := make([]timedWindow, len(w.windows))
	copy(windows, w.windows)
	return windows
}

// getOrCreateWindow returns the window containing the given timestamp, creating a new one if necessary.
// 0 is a valid timestamp and will return the most recent window.
func (w *timeWindowedTDigest) getOrCreateWindow(timestamp time.Time) (*timedWindow, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if timestamp.IsZero() {
		timestamp = time.Now()
	}
	candidate, err := w.searchWindowsBackwards(timestamp)
	if err == nil {
		return candidate, nil
	}
	if errors.Is(err, errTooNew) {
		return w.advanceWindow(timestamp), nil
	}
	// err is errTooOld or errInGap
	return nil, err
}

var errTooOld = errors.New("time was older than the earliest window")
var errTooNew = errors.New("time was newer than the latest window")
var errInGap = errors.New("time was in a gap between windows")

// Precondition: w.mu is held.
// Returns the window containing the given timestamp, or error if no window exists.
func (w *timeWindowedTDigest) searchWindowsBackwards(timestamp time.Time) (*timedWindow, error) {
	latest := w.windows[w.head]
	if !timestamp.Before(latest.start) {
		// If the requested timestamp is after the latest window, no point in searching
		if !timestamp.Before(latest.end) {
			return nil, errTooNew
		}
		return &latest, nil
	}
	for idx := w.modDec(w.head); idx != w.head; idx = w.modDec(idx) {
		candidate := w.windows[idx]
		// Window start is inclusive, end is exclusive. We're iterating
		// backwards in time, so the first window that matches is the one we want.
		if !timestamp.Before(candidate.start) {
			// The first window that matches might be too short to include this timestamp.
			// Make sure the timestamp is actually in the window.
			if !timestamp.Before(w.windows[idx].end) {
				return nil, errInGap
			}
			return &candidate, nil
		}
	}
	// All the windows are newer than the requested timestamp.
	return nil, errTooOld
}

// Precondition: w.mu is held.
func (w *timeWindowedTDigest) advanceWindow(timestamp time.Time) *timedWindow {
	if w.cfg.FillBlankIntervals {
		// Fill in all the intervening blank windows.
		// The empty digests will not affect the stats, but they will drop old windows.
		lastWindow := &w.windows[w.head]
		if lastWindow.tdigest == nil {
			// Special-case, the latest window is uninitialized. Just take it.
			return w.advanceWindowSimple(timestamp)
		}
		for {
			lastWindow = w.advanceWindowSimple(lastWindow.end)
			// Need a do-while here to enforce that lastWindow contains timestamp
			if timestamp.Before(lastWindow.end) {
				break
			}
		}
		return lastWindow
	}
	return w.advanceWindowSimple(timestamp)
}

func (w *timeWindowedTDigest) advanceWindowSimple(start time.Time) *timedWindow {
	w.head = w.modInc(w.head)
	digest, _ := tdigest.New()
	window := timedWindow{
		tdigest: digest,
		start:   start,
		end:     start.Add(w.cfg.WindowSize),
	}
	w.windows[w.head] = window
	return &window
}

// modulo-increment, for wrapping around the ring buffer
func (w *timeWindowedTDigest) modInc(idx int) int {
	return (idx + 1) % w.cfg.WindowCount
}

// modulo-decrement, for wrapping around the ring buffer.
func (w *timeWindowedTDigest) modDec(idx int) int {
	return (idx - 1 + w.cfg.WindowCount) % w.cfg.WindowCount
}

func (w *timedWindow) Start() time.Time {
	return w.start
}
func (w *timedWindow) End() time.Time {
	return w.end
}

func (w *timedWindow) Quantile(q float64) float64 {
	return w.tdigest.Quantile(q)
}

func (w *timedWindow) TrimmedMean(lowerQuantile, upperQuantile float64) float64 {
	return w.tdigest.TrimmedMean(lowerQuantile, upperQuantile)
}
