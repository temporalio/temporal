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
	reportAttemptErrors(tb, failures)
	if timeoutMsg != "" {
		tb.Fatalf("%s: %s (not satisfied after %v, %d polls)", funcName, timeoutMsg, effectiveTimeout, polls)
	} else {
		tb.Fatalf("%s: condition not satisfied after %v (%d polls)", funcName, effectiveTimeout, polls)
	}
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
