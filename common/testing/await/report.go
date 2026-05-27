package await

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

// reportAttemptErrors emits the collected attempt failures. When there are
// many, only the first and the last few are shown — long polls would
// otherwise produce hundreds of duplicate lines.
const (
	reportHeadAttempts = 1
	reportTailAttempts = 3
)

type attemptFailure struct {
	attempt int
	errors  []string
}

type awaitStats struct {
	attempts         []attemptTiming
	sleeps           []time.Duration
	failedAttempts   int
	stoppedAttempts  int
	deadlockAttempts int
}

type attemptTiming struct {
	attempt  int
	duration time.Duration
}

func (s *awaitStats) recordAttempt(attempt int, duration time.Duration, failed, stopped, deadlocked bool) {
	s.attempts = append(s.attempts, attemptTiming{
		attempt:  attempt,
		duration: duration,
	})
	if failed {
		s.failedAttempts++
	}
	if stopped {
		s.stoppedAttempts++
	}
	if deadlocked {
		s.deadlockAttempts++
	}
}

func (s *awaitStats) recordSleep(duration time.Duration) {
	s.sleeps = append(s.sleeps, duration)
}

// reportTimeout reports the timeout failure plus collected attempt errors.
func reportTimeout(
	tb testing.TB,
	failures []attemptFailure,
	stats awaitStats,
	parentErr error,
	awaitErr error,
	deadlineRemaining time.Duration,
	funcName string,
	timeoutMsg string,
	effectiveTimeout time.Duration,
	polls int,
) {
	var sections []string
	sections = append(sections, formatAwaitStats(stats, parentErr, awaitErr, deadlineRemaining, polls))
	if s := formatFinalAttemptContext(failures); s != "" {
		sections = append(sections, s)
	}
	if s := formatAttemptErrors(failures); s != "" {
		sections = append(sections, s)
	}
	diagnostics := strings.Join(sections, "\n\n")
	if timeoutMsg != "" {
		tb.Fatalf("%s\n\n%s: %s (not satisfied after %v, %d polls)", diagnostics, funcName, timeoutMsg, effectiveTimeout, polls)
	} else {
		tb.Fatalf("%s\n\n%s: condition not satisfied after %v (%d polls)", diagnostics, funcName, effectiveTimeout, polls)
	}
}

func formatAwaitStats(stats awaitStats, parentErr error, awaitErr error, deadlineRemaining time.Duration, polls int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "await stats: polls=%d failed_attempts=%d stopped_attempts=%d deadlock_attempts=%d",
		polls, stats.failedAttempts, stats.stoppedAttempts, stats.deadlockAttempts)
	writeDurationSummary(&b, "attempt_duration", attemptDurations(stats.attempts))
	writeDurationSummary(&b, "sleep_duration", stats.sleeps)
	writeSlowestAttempts(&b, stats.attempts)
	fmt.Fprintf(&b, "\ncontext at timeout: parent_err=%v await_err=%v deadline_remaining=%v",
		parentErr, awaitErr, deadlineRemaining)
	return b.String()
}

func formatFinalAttemptContext(failures []attemptFailure) string {
	if len(failures) == 0 {
		return ""
	}

	var b strings.Builder
	last := failures[len(failures)-1]
	b.WriteString("last failed attempt before timeout:")
	writeAttemptFailure(&b, last)

	if previous, ok := previousNonDeadlineFailure(failures); ok && previous.attempt != last.attempt {
		b.WriteString("\n\nlast non-deadline failed attempt:")
		writeAttemptFailure(&b, previous)
	} else if isDeadlineOnlyFailure(last) {
		if previous, ok := previousDistinctFailure(failures, last); ok {
			b.WriteString("\n\nprevious distinct failed attempt:")
			writeAttemptFailure(&b, previous)
		}
	}

	return b.String()
}

func reportAttemptErrors(tb testing.TB, failures []attemptFailure) {
	if s := formatAttemptErrors(failures); s != "" {
		tb.Errorf("%s", s)
	}
}

func formatAttemptErrors(failures []attemptFailure) string {
	if len(failures) == 0 {
		return ""
	}

	var b strings.Builder
	b.WriteString("attempt errors:")
	if len(failures) <= reportHeadAttempts+reportTailAttempts {
		for _, f := range failures {
			writeAttemptFailure(&b, f)
		}
	} else {
		for _, f := range failures[:reportHeadAttempts] {
			writeAttemptFailure(&b, f)
		}
		omitted := len(failures) - reportHeadAttempts - reportTailAttempts
		fmt.Fprintf(&b, "\n  ... %d attempts omitted ...", omitted)
		for _, f := range failures[len(failures)-reportTailAttempts:] {
			writeAttemptFailure(&b, f)
		}
	}
	return b.String()
}

func previousDistinctFailure(failures []attemptFailure, last attemptFailure) (attemptFailure, bool) {
	lastText := attemptFailureText(last)
	for i := len(failures) - 2; i >= 0; i-- {
		if attemptFailureText(failures[i]) != lastText {
			return failures[i], true
		}
	}
	return attemptFailure{}, false
}

func previousNonDeadlineFailure(failures []attemptFailure) (attemptFailure, bool) {
	for i := len(failures) - 1; i >= 0; i-- {
		if !hasContextDeadlineFailure(failures[i]) {
			return failures[i], true
		}
	}
	return attemptFailure{}, false
}

func isDeadlineOnlyFailure(f attemptFailure) bool {
	if len(f.errors) == 0 {
		return false
	}
	text := strings.ToLower(attemptFailureText(f))
	if !hasContextDeadlineText(text) {
		return false
	}
	withoutDeadline := strings.ReplaceAll(text, "context deadline exceeded", "")
	withoutDeadline = strings.ReplaceAll(withoutDeadline, "context canceled", "")
	withoutDeadline = strings.TrimSpace(withoutDeadline)
	return withoutDeadline == "" ||
		strings.Contains(withoutDeadline, "error trace:") ||
		strings.Contains(withoutDeadline, "error:")
}

func hasContextDeadlineFailure(f attemptFailure) bool {
	return hasContextDeadlineText(strings.ToLower(attemptFailureText(f)))
}

func hasContextDeadlineText(text string) bool {
	return strings.Contains(text, "context deadline exceeded") ||
		strings.Contains(text, "context canceled")
}

func attemptFailureText(f attemptFailure) string {
	return strings.Join(f.errors, "\n")
}

func writeDurationSummary(b *strings.Builder, label string, durations []time.Duration) {
	if len(durations) == 0 {
		fmt.Fprintf(b, " %s=(none)", label)
		return
	}
	minDuration, maxDuration, totalDuration := durations[0], durations[0], time.Duration(0)
	for _, duration := range durations {
		minDuration = min(minDuration, duration)
		maxDuration = max(maxDuration, duration)
		totalDuration += duration
	}
	fmt.Fprintf(b, " %s min=%v avg=%v max=%v last=%v",
		label, minDuration, totalDuration/time.Duration(len(durations)), maxDuration, durations[len(durations)-1])
}

func writeSlowestAttempts(b *strings.Builder, timings []attemptTiming) {
	if len(timings) == 0 {
		b.WriteString("\nslowest attempts: (none)")
		return
	}
	slowest := slices.Clone(timings)
	slices.SortFunc(slowest, func(a, b attemptTiming) int {
		return cmp.Compare(b.duration, a.duration)
	})
	if len(slowest) > 3 {
		slowest = slowest[:3]
	}
	b.WriteString("\nslowest attempts:")
	for _, timing := range slowest {
		fmt.Fprintf(b, " #%d=%v", timing.attempt, timing.duration)
	}
}

func attemptDurations(timings []attemptTiming) []time.Duration {
	durations := make([]time.Duration, 0, len(timings))
	for _, timing := range timings {
		durations = append(durations, timing.duration)
	}
	return durations
}

func writeAttemptFailure(b *strings.Builder, f attemptFailure) {
	fmt.Fprintf(b, "\n\n  --- attempt %d ---", f.attempt)
	if len(f.errors) == 0 {
		b.WriteString("\n    (attempt failed without recorded assertion output)")
		return
	}
	for _, e := range f.errors {
		for line := range strings.SplitSeq(e, "\n") {
			b.WriteString("\n    ")
			b.WriteString(line)
		}
	}
}
