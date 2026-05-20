package flakereport

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

// commitOrder returns commit SHAs between oldSHA (exclusive) and HEAD (inclusive),
// in chronological order (oldest first).
// Shells out to: git log --reverse --format=%H <oldSHA>..HEAD
func commitOrder(ctx context.Context, oldSHA string) ([]string, error) {
	ctxTimeout, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctxTimeout, "git", "log",
		"--reverse",
		"--format=%H",
		fmt.Sprintf("%s..HEAD", oldSHA),
	)

	output, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("git log failed: %w\nstderr: %s", err, string(exitErr.Stderr))
		}
		return nil, fmt.Errorf("failed to run git log: %w", err)
	}

	var commits []string
	for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			commits = append(commits, line)
		}
	}
	return commits, nil
}
