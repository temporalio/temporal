package github

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

const defaultTimeout = 30 * time.Second

var fallbackToken struct {
	sync.Once
	value string
	err   error
}

func apiToken(ctx context.Context) (string, error) {
	if token := os.Getenv("GH_TOKEN"); token != "" {
		return token, nil
	}
	if token := os.Getenv("GITHUB_TOKEN"); token != "" {
		return token, nil
	}

	fallbackToken.Do(func() {
		output, err := commandOutput(ctx, defaultTimeout, "auth", "token")
		fallbackToken.value = strings.TrimSpace(string(output))
		fallbackToken.err = err
	})
	if fallbackToken.err != nil {
		return "", fmt.Errorf("failed to get GitHub token: %w", fallbackToken.err)
	}
	if fallbackToken.value == "" {
		return "", errors.New("GitHub token is empty")
	}
	return fallbackToken.value, nil
}

func commandOutput(ctx context.Context, timeout time.Duration, args ...string) ([]byte, error) {
	ctxTimeout, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(ctxTimeout, "gh", args...)
	output, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("gh %s failed: %w\nstdout: %s\nstderr: %s", strings.Join(args, " "), err, string(output), string(exitErr.Stderr))
		}
		return nil, fmt.Errorf("failed to execute gh %s: %w", strings.Join(args, " "), err)
	}
	return output, nil
}

func commandRun(ctx context.Context, timeout time.Duration, args ...string) error {
	ctxTimeout, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(ctxTimeout, "gh", args...)
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("gh %s failed: %w", strings.Join(args, " "), err)
	}
	return nil
}
