package stats

import (
	"errors"
	"sync"
	"time"

	"github.com/caio/go-tdigest/v5"
	"go.temporal.io/server/common/fastrand"
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
	}

	// WindowConfig controls the windowing behavior.
	WindowConfig struct {
		// windowSize is the duration of each window.
		WindowSize time.Duration
		// windowCount is the maximum number of windows to retain.
		WindowCount int
		// FillBlankIntervals controls whether gaps in the windowing should be preserved.
		// This matters when windowSize is shorter than the largest gap in the measured data.
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
// Some guidance on sizing the windows/counts: Prefer windows containing about 10,000 datapoints.
// Insert latency increases by a factor of about 3x from a size of 1k-10k, but the stored size in RAM drops by 7x.
// So, if you want 300 seconds of history on an event that records 1k counts/sec, 3 10-second windows
// is fine.
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

func (w *timeWindowedTDigest) RecordToLatestWindow(value float64) {
	w.RecordMultiToLatestWindow(value, 1)
}

func (w *timeWindowedTDigest) RecordMultiToLatestWindow(value float64, count uint64) {
	w.RecordMulti(value, time.Now(), count)
}

func (w *timeWindowedTDigest) Record(value float64, timestamp time.Time) {
	w.RecordMulti(value, timestamp, 1)
}

func (w *timeWindowedTDigest) RecordMulti(value float64, timestamp time.Time, count uint64) {
	if err := w.addToWindow(timestamp, value, count); err != nil {
		// Data must have been too old, ignore.
		return
	}
}

func (w *timeWindowedTDigest) Quantile(q float64) float64 {
	merged := w.getMergedWindows()
	if merged == nil || merged.Count() == 0 {
		return 0
	}

	return merged.Quantile(q)
}

func (w *timeWindowedTDigest) TrimmedMean(lowerQuantile, upperQuantile float64) float64 {
	merged := w.getMergedWindows()
	if merged == nil || merged.Count() == 0 {
		return 0
	}

	return merged.TrimmedMean(lowerQuantile, upperQuantile)
}

// TODO: this is expensive, maybe cache everything but the latest window so we can skip N merges?
func (w *timeWindowedTDigest) getMergedWindows() *tdigest.TDigest {
	w.mu.Lock()
	defer w.mu.Unlock()

	var merged *tdigest.TDigest
	for idx := range w.windows {
		td := w.windows[idx].tdigest
		if td == nil {
			continue
		}

		if merged == nil {
			merged = td.Clone()
		} else {
			_ = merged.Merge(td)
		}
	}
	return merged
}

func (w *timeWindowedTDigest) addToWindow(timestamp time.Time, value float64, count uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	candidate, err := w.searchWindowsBackwards(timestamp)
	if err != nil {
		if !errors.Is(err, errTooNew) {
			// err is errTooOld or errInGap
			return err
		}

		candidate = w.advanceWindow(timestamp)
	}

	return candidate.tdigest.AddWeighted(value, count)
}

var errTooOld = errors.New("time was older than the earliest window")
var errTooNew = errors.New("time was newer than the latest window")
var errInGap = errors.New("time was in a gap between windows")

// Precondition: w.mu is held.
// Returns the window containing the given timestamp, or error if no window exists.
func (w *timeWindowedTDigest) searchWindowsBackwards(timestamp time.Time) (*timedWindow, error) {
	latest := &w.windows[w.head]
	if !timestamp.Before(latest.start) {
		// If the requested timestamp is after the latest window, no point in searching
		if !timestamp.Before(latest.end) {
			return nil, errTooNew
		}
		return latest, nil
	}
	for idx := w.modDec(w.head); idx != w.head; idx = w.modDec(idx) {
		candidate := &w.windows[idx]
		// Window start is inclusive, end is exclusive. We're iterating
		// backwards in time, so the first window that matches is the one we want.
		if !timestamp.Before(candidate.start) {
			// The first window that matches might be too short to include this timestamp.
			// Make sure the timestamp is actually in the window.
			if !timestamp.Before(w.windows[idx].end) {
				return nil, errInGap
			}
			return candidate, nil
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

// Precondition: w.mu is held.
func (w *timeWindowedTDigest) advanceWindowSimple(start time.Time) *timedWindow {
	w.head = w.modInc(w.head)
	if curr := &w.windows[w.head]; curr.tdigest != nil {
		curr.tdigest.Reset()
		curr.start = start
		curr.end = start.Add(w.cfg.WindowSize)
		return curr
	}

	digest, _ := tdigest.New(tdigest.RandomNumberGenerator(fastrand.Rand{}))
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
