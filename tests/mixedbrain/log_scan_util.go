package mixedbrain

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// logLineValidator inspects a single server log line and returns a non-nil
// error if the line indicates a problem. Validators are run against every line
// of each server log file after the servers stop. To add a new scan, implement
// this interface (or use substringValidator / validatorFunc) and append it to
// serverLogValidators.
type logLineValidator interface {
	Validate(line string) error
}

// validatorFunc adapts a plain function into a logLineValidator, for ad-hoc
// checks that don't fit the substring model (regex, JSON field parsing, etc.).
type validatorFunc func(line string) error

func (f validatorFunc) Validate(line string) error { return f(line) }

// substringValidator fails a line that contains `include` unless the line also
// contains one of `exclude` (known-good lines to skip). This mirrors the
// |= / != operators in saas-cicd oss-cicd/e2e/workflows/generic/activities/check_logs.go;
// keep the two in sync when adjusting the exclude lists.
type substringValidator struct {
	name    string
	include string
	exclude []string
}

func (v substringValidator) Validate(line string) error {
	if !strings.Contains(line, v.include) {
		return nil
	}
	for _, e := range v.exclude {
		if strings.Contains(line, e) {
			return nil
		}
	}
	return fmt.Errorf("%s: %s", v.name, truncate(line, 500))
}

// serverLogValidators is the registry of checks applied to server logs.
// Append an entry to add a new scan.
var serverLogValidators = []logLineValidator{
	substringValidator{
		name:    "soft assertion",
		include: "failed assertion:",
		// per saas-cicd check_logs.go assertionQueryTemplate
		exclude: []string{"found otherHasTasks in classic metadata"},
	},
	substringValidator{
		name:    "panic",
		include: "panic",
		exclude: []string{
			"Potential deadlock detected", // expected under resource-constrained load
			"[TMPRL1100]",                 // invalid state transition, sdk-side (per saas-cicd)
		},
	},
	substringValidator{
		name:    "inconsistent metric descriptor",
		include: "a previously registered descriptor with the same fully-qualified name as",
	},
}

// scanServerLogs runs every validator against every line of each log file and
// fails the test (without stopping at the first hit) on any validation error.
func scanServerLogs(t testing.TB, validators []logLineValidator, logPaths ...string) {
	t.Helper()
	for _, path := range logPaths {
		scanLogFile(t, validators, path)
	}
}

func scanLogFile(t testing.TB, validators []logLineValidator, path string) {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err, "open server log %s", path)
	defer func() { _ = f.Close() }()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024) // server JSON lines can be long
	for n := 1; sc.Scan(); n++ {
		line := sc.Text()
		for _, v := range validators {
			if err := v.Validate(line); err != nil {
				t.Errorf("%s:%d %v", filepath.Base(path), n, err)
			}
		}
	}
	require.NoError(t, sc.Err(), "read server log %s", path)
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}
