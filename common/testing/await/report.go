package await

import (
	"fmt"
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

type timeoutReport struct {
	attempts   int
	failures   []attemptFailure
	derivation timeoutDerivation
}

type timeoutDerivation struct {
	cfgTotalTimeout   time.Duration
	hasParentDeadline bool
	parentRemaining   time.Duration
	hasTestCap        bool
	testCapHit        bool
	testCapRemaining  time.Duration
	effectiveTimeout  time.Duration
	attempts          int
	attemptTimeout    time.Duration
	attemptTimeouts   int
	minPollInterval   time.Duration
	maxPollInterval   time.Duration
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
	r.derivation.attemptTimeouts++
}

func (r timeoutReport) reportAttemptErrors(tb testing.TB) {
	reportAttemptErrors(tb, r.failures)
}

func (r timeoutReport) reportTimeout(tb testing.TB, funcName, timeoutMsg string) {
	r.reportAttemptErrors(tb)
	derivation := r.derivation
	derivation.attempts = r.attempts
	detail := derivation.String()
	message := fmt.Sprintf("condition not satisfied after %v", r.derivation.effectiveTimeout)
	if timeoutMsg != "" {
		message = fmt.Sprintf("%s (not satisfied after %v)", timeoutMsg, r.derivation.effectiveTimeout)
	}
	tb.Fatalf("%s: %s\n%s", funcName, message, detail)
}

func (d timeoutDerivation) String() string {
	var b strings.Builder
	b.WriteString("details:")
	fmt.Fprintf(&b, "\n  cfg.totalTimeout      = %v", d.cfgTotalTimeout)
	if d.hasParentDeadline {
		fmt.Fprintf(&b, "\n  parent ctx deadline   = +%v", d.parentRemaining.Round(time.Millisecond))
	} else {
		b.WriteString("\n  parent ctx deadline   = (none)")
	}
	if d.hasTestCap {
		hit := "not hit"
		if d.testCapHit {
			hit = "HIT"
		}
		fmt.Fprintf(&b, "\n  test 2min cap         = +%v remaining (%s)", d.testCapRemaining.Round(time.Millisecond), hit)
	}
	fmt.Fprintf(&b, "\n  effective deadline    = %v from start", d.effectiveTimeout)
	fmt.Fprintf(&b, "\n  attempts              = %d", d.attempts)
	fmt.Fprintf(&b, "\n  per-attempt timeout   = %v", d.attemptTimeout)
	fmt.Fprintf(&b, "\n  attempt timeouts      = %d", d.attemptTimeouts)
	if d.minPollInterval == d.maxPollInterval {
		fmt.Fprintf(&b, "\n  poll interval         = %v (fixed)", d.minPollInterval)
	} else {
		fmt.Fprintf(&b, "\n  poll interval         = %v -> %v (exp backoff)", d.minPollInterval, d.maxPollInterval)
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
	fmt.Fprintf(b, "\n  attempt %d:", f.attempt)
	for _, e := range f.errors {
		for line := range strings.SplitSeq(e, "\n") {
			b.WriteString("\n    ")
			b.WriteString(line)
		}
	}
}
