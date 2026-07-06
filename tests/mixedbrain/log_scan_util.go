package mixedbrain

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"go.temporal.io/server/common/util"
)

// logLineValidator inspects a single server log line and returns a non-nil
// error if the line indicates a problem. Validators are run against every line
// of each server log file after the servers stop. To add a new scan, implement
// this interface (or use substringValidator) and append it to serverLogValidators.
type logLineValidator interface {
	Validate(line string) error
}

// substringValidator fails a line that contains `include` unless the line also
// contains one of `exclude` (known-good lines to skip).
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
	return fmt.Errorf("%s: %s", v.name, util.TruncateUTF8(line, 500))
}

// serverLogValidators is the registry of checks applied to server logs.
// Append an entry to add a new scan.
var serverLogValidators = []logLineValidator{
	substringValidator{
		name:    "soft assertion",
		include: "failed assertion:",
		exclude: []string{"found otherHasTasks in classic metadata"},
	},
	substringValidator{
		name:    "panic",
		include: "panic",
		exclude: []string{
			"Potential deadlock detected", // expected under resource-constrained load
			"[TMPRL1100]",                 // invalid state transition, sdk-side
		},
	},
}

// scanServerLogs runs every validator against every line of each log file and
// returns a message for each validation failure (it does not stop at the first).
func scanServerLogs(validators []logLineValidator, logPaths ...string) ([]string, error) {
	var problems []string
	for _, path := range logPaths {
		f, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("open server log %s: %w", path, err)
		}

		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024) // server JSON lines can be long
		for n := 1; sc.Scan(); n++ {
			line := sc.Text()
			for _, v := range validators {
				if err := v.Validate(line); err != nil {
					problems = append(problems, fmt.Sprintf("%s:%d %v", filepath.Base(path), n, err))
				}
			}
		}
		err = sc.Err()
		_ = f.Close()
		if err != nil {
			return nil, fmt.Errorf("read server log %s: %w", path, err)
		}
	}
	return problems, nil
}
