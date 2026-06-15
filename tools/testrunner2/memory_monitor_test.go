package testrunner2

import (
	"bytes"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFormatRSS(t *testing.T) {
	t.Parallel()

	require.Empty(t, formatRSS(0))
	require.Equal(t, "512KiB", formatRSS(512))
	require.Equal(t, "64MiB", formatRSS(64*1024))
	require.Equal(t, "1.5GiB", formatRSS(1536*1024))
}

func TestRunnerLogMemorySummary(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	r := &runner{
		console: &consoleWriter{mu: &sync.Mutex{}, w: &buf},
	}
	r.addMemoryRecord(memoryRecord{
		displayName: "SmallSuite",
		attempt:     "1",
		runtime:     time.Second,
		peakRSSKB:   512 * 1024,
	})
	r.addMemoryRecord(memoryRecord{
		displayName: "LargeSuite",
		attempt:     "3, retry 2",
		isolated:    true,
		runtime:     2 * time.Minute,
		peakRSSKB:   2 * 1024 * 1024,
	})
	r.addMemoryRecord(memoryRecord{
		displayName: "NoSampleSuite",
		attempt:     "1",
		runtime:     time.Second,
	})

	r.logMemorySummary()

	out := buf.String()
	require.Contains(t, out, "memory peaks")
	require.Contains(t, out, "LargeSuite (attempt=3, retry 2, isolated, runtime=2m0s)")
	require.Contains(t, out, "SmallSuite (attempt=1, runtime=1s)")
	require.NotContains(t, out, "NoSampleSuite")
	require.Less(t, strings.Index(out, "LargeSuite"), strings.Index(out, "SmallSuite"))
}
