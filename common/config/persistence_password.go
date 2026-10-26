//go:build !gomad

package config

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

const (
	defaultPasswordCommandTimeout = 30 * time.Second
	passwordCommandWaitDelay      = 5 * time.Second
)

// ResolvePassword returns the database password, either from the static Password
// field or by executing PasswordCommand. If neither is set, it returns an empty string.
func (c *SQL) ResolvePassword() (string, error) {
	if c.PasswordCommand == nil {
		return c.Password, nil
	}
	timeout := c.PasswordCommand.Timeout
	if timeout == 0 {
		timeout = defaultPasswordCommandTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, c.PasswordCommand.Command, c.PasswordCommand.Args...) //nolint:gosec
	// WaitDelay caps how long we block on the stdout pipe after the process is killed.
	// Without it, a subprocess that inherits the pipe could keep it open indefinitely.
	cmd.WaitDelay = passwordCommandWaitDelay
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("passwordCommand %q %v failed: %w (stderr: %s)",
			c.PasswordCommand.Command, c.PasswordCommand.Args, err, stderr.String())
	}
	return strings.TrimRight(string(out), "\n\r"), nil
}
