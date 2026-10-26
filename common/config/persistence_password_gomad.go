//go:build gomad

package config

import "fmt"

// ResolvePassword returns the static Password. Under Gomad the deterministic
// runtime forbids os/exec, so a configured PasswordCommand is an error rather
// than a subprocess.
func (c *SQL) ResolvePassword() (string, error) {
	if c.PasswordCommand == nil {
		return c.Password, nil
	}
	return "", fmt.Errorf("%w: passwordCommand cannot run under Gomad", ErrPersistenceConfig)
}
