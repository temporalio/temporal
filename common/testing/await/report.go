package await

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.temporal.io/server/common/testing/testcontext"
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
	effectiveTimeout   time.Duration
	configuredTimeout  time.Duration
	attemptTimeout     time.Duration
	testContext        context.Context
	deadlineCause      string
	attempts           int
	attemptTimeouts    int
	attemptDurationSum time.Duration
	attemptDurationMax time.Duration
	failures           []attemptFailure
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
	message := fmt.Sprintf("condition not satisfied after %v", reportDuration(r.effectiveTimeout))
	if timeoutMsg != "" {
		message = fmt.Sprintf("%s (not satisfied after %v)", timeoutMsg, reportDuration(r.effectiveTimeout))
	}
	details := r.renderDetails()
	if audit := testcontext.ExtensionAudit(r.testContext); audit != "" {
		if details != "" {
			details += "\n"
		}
		details += audit
	}
	tb.Fatalf("%s: %s\ndetails:\n%s", funcName, message, indentReportDetails(details))
}

func (r timeoutReport) renderSupplementalDetails(funcName, timeoutMsg string) string {
	var details strings.Builder
	writeReportDetail(&details, "operation", funcName)
	if timeoutMsg != "" {
		writeReportDetail(&details, "condition", timeoutMsg)
	}
	details.WriteString(r.renderDetails())
	return strings.TrimSuffix(details.String(), "\n")
}

func (r timeoutReport) renderDetails() string {
	var details strings.Builder
	if r.deadlineCause != "" && r.configuredTimeout > 0 {
		writeReportDetail(&details, "await timeout", fmt.Sprintf(
			"%v (configured %v; limited by %s)",
			reportDuration(r.effectiveTimeout), reportDuration(r.configuredTimeout), r.deadlineCause,
		))
	}
	writeReportDetail(&details, "attempts", fmt.Sprintf("%d", r.attempts))
	if r.attemptTimeouts > 0 {
		writeReportDetail(&details, "attempt timeouts", fmt.Sprintf("%d (attempt timeout %v)", r.attemptTimeouts, reportDuration(r.attemptTimeout)))
	}
	if r.attempts > 0 {
		writeReportDetail(
			&details,
			"attempt duration",
			fmt.Sprintf(
				"avg %v, max %v",
				reportDuration(r.attemptDurationSum/time.Duration(r.attempts)),
				reportDuration(r.attemptDurationMax),
			),
		)
	}
	if failures := renderAttemptFailures(r.failures); failures != "" {
		details.WriteString(failures)
		details.WriteByte('\n')
	}
	return strings.TrimSuffix(details.String(), "\n")
}

// Keep the 16-character label column aligned with the testcontext audit.
func writeReportDetail(details *strings.Builder, label, value string) {
	fmt.Fprintf(details, "%-16s = %s\n", label, value)
}

func indentReportDetails(details string) string {
	lines := strings.Split(details, "\n")
	for i, line := range lines {
		if line != "" {
			lines[i] = "  " + line
		}
	}
	return strings.Join(lines, "\n")
}

// Keep this formatting consistent with testcontext reports embedded above.
func reportDuration(d time.Duration) string {
	if d > -time.Millisecond && d < time.Millisecond {
		rounded := d.Round(time.Microsecond)
		if rounded != 0 {
			return rounded.String()
		}
	}
	return d.Round(time.Millisecond).String()
}

func reportAttemptErrors(tb testing.TB, failures []attemptFailure) {
	message := renderAttemptFailures(failures)
	if message == "" {
		return
	}
	tb.Errorf("%s", message)
}

func renderAttemptFailures(failures []attemptFailure) string {
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
