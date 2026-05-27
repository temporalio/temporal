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

// reportTimeout reports the timeout failure plus collected attempt errors.
func reportTimeout(tb testing.TB, failures []attemptFailure, funcName, timeoutMsg string, effectiveTimeout time.Duration, polls int) {
	reportFinalAttemptContext(tb, failures)
	reportAttemptErrors(tb, failures)
	if timeoutMsg != "" {
		tb.Fatalf("%s: %s (not satisfied after %v, %d polls)", funcName, timeoutMsg, effectiveTimeout, polls)
	} else {
		tb.Fatalf("%s: condition not satisfied after %v (%d polls)", funcName, effectiveTimeout, polls)
	}
}

func reportFinalAttemptContext(tb testing.TB, failures []attemptFailure) {
	if len(failures) == 0 {
		return
	}

	var b strings.Builder
	last := failures[len(failures)-1]
	b.WriteString("last failed attempt before timeout:")
	writeAttemptFailure(&b, last)

	if isDeadlineOnlyFailure(last) {
		if previous, ok := previousDistinctFailure(failures, last); ok {
			b.WriteString("\n\nprevious distinct failed attempt:")
			writeAttemptFailure(&b, previous)
		}
	}

	tb.Errorf("%s", b.String())
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

func previousDistinctFailure(failures []attemptFailure, last attemptFailure) (attemptFailure, bool) {
	lastText := attemptFailureText(last)
	for i := len(failures) - 2; i >= 0; i-- {
		if attemptFailureText(failures[i]) != lastText {
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
	hasDeadline := strings.Contains(text, "context deadline exceeded") ||
		strings.Contains(text, "context canceled")
	if !hasDeadline {
		return false
	}
	withoutDeadline := strings.ReplaceAll(text, "context deadline exceeded", "")
	withoutDeadline = strings.ReplaceAll(withoutDeadline, "context canceled", "")
	withoutDeadline = strings.TrimSpace(withoutDeadline)
	return withoutDeadline == "" ||
		strings.Contains(withoutDeadline, "error trace:") ||
		strings.Contains(withoutDeadline, "error:")
}

func attemptFailureText(f attemptFailure) string {
	return strings.Join(f.errors, "\n")
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
