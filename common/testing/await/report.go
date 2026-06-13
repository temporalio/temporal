package await

import (
	"fmt"
	"strings"
	"testing"
	"text/tabwriter"
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

type timeoutReport struct {
	effectiveTimeout    time.Duration
	configuredTimeout   time.Duration
	attemptTimeout      time.Duration
	testExtensionReport string
	deadlineCause       string
	attempts            int
	attemptTimeouts     int
	attemptDurationSum  time.Duration
	attemptDurationMax  time.Duration
	failures            []attemptFailure
}

func (r *timeoutReport) nextPoll() {
	r.attempts++
}

func (r *timeoutReport) recordErrors(errors []string) {
	if len(errors) > 0 {
		r.failures = append(r.failures, attemptFailure{attempt: r.attempts, errors: errors})
	}
}

func (r *timeoutReport) recordAttemptTimeout() {
	r.attemptTimeouts++
}

func (r *timeoutReport) recordAttemptDuration(d time.Duration) {
	r.attemptDurationSum += d
	r.attemptDurationMax = max(r.attemptDurationMax, d)
}

func (r timeoutReport) reportAttemptErrors(tb testing.TB) {
	reportAttemptErrors(tb, r.failures)
}

func (r timeoutReport) reportTimeout(tb testing.TB, funcName, timeoutMsg string) {
	r.reportAttemptErrors(tb)
	message := fmt.Sprintf("condition not satisfied after %v", reportDuration(r.effectiveTimeout))
	if timeoutMsg != "" {
		message = fmt.Sprintf("%s (not satisfied after %v)", timeoutMsg, reportDuration(r.effectiveTimeout))
	}
	var details strings.Builder
	detailWriter := tabwriter.NewWriter(&details, 0, 0, 1, ' ', 0)
	writeDetail := func(label, value string) {
		fmt.Fprintf(detailWriter, "  %s\t= %s\n", label, value)
	}

	hasAttemptFailures := len(r.failures) > 0 || r.attemptTimeouts > 0
	shortenedTimeout := r.configuredTimeout-r.effectiveTimeout > time.Millisecond
	if r.configuredTimeout > 0 && (!hasAttemptFailures || shortenedTimeout) {
		value := reportDuration(r.effectiveTimeout)
		if shortenedTimeout {
			value += fmt.Sprintf(" (configured %v", reportDuration(r.configuredTimeout))
			if r.deadlineCause != "" {
				value += "; limited by " + r.deadlineCause
			}
			value += ")"
		}
		writeDetail("await timeout", value)
	}
	writeDetail("attempts", fmt.Sprintf("%d", r.attempts))
	if r.attemptTimeouts > 0 {
		writeDetail("attempt timeout", fmt.Sprintf("%d (configured as %v)", r.attemptTimeouts, reportDuration(r.attemptTimeout)))
	}
	if r.attempts > 0 {
		writeDetail(
			"attempt duration",
			fmt.Sprintf(
				"avg %v, max %v",
				reportDuration(r.attemptDurationSum/time.Duration(r.attempts)),
				reportDuration(r.attemptDurationMax),
			),
		)
	}
	if r.attemptTimeouts == 0 && r.deadlineCause != "" {
		writeDetail("last failure", r.deadlineCause)
	}
	if r.testExtensionReport != "" {
		fmt.Fprintln(detailWriter, indentDetail(r.testExtensionReport))
	}
	if err := detailWriter.Flush(); err != nil {
		tb.Fatalf("%s: failed to render timeout report: %v", funcName, err)
		return
	}
	tb.Fatalf("%s: %s\ndetails:\n%s", funcName, message, strings.TrimSuffix(details.String(), "\n"))
}

func reportDuration(d time.Duration) string {
	if d > -time.Millisecond && d < time.Millisecond {
		rounded := d.Round(time.Microsecond)
		if rounded == 0 {
			return "0µs"
		}
		return rounded.String()
	}
	return d.Round(time.Millisecond).String()
}

func indentDetail(s string) string {
	var b strings.Builder
	for line := range strings.SplitSeq(s, "\n") {
		if b.Len() > 0 {
			b.WriteByte('\n')
		}
		b.WriteString("  ")
		b.WriteString(line)
	}
	return b.String()
}

func reportAttemptErrors(tb testing.TB, failures []attemptFailure) {
	if len(failures) == 0 {
		return
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
	tb.Errorf("%s", b.String())
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
