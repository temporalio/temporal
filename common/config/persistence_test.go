package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSQLValidate_MutualExclusivity(t *testing.T) {
	cfg := &SQL{
		Password: "static",
		PasswordCommand: &PasswordCommandConfig{
			Command: "echo",
			Args:    []string{"dynamic"},
		},
	}
	err := cfg.validate()
	require.ErrorContains(t, err, "mutually exclusive")
}

func TestSQLValidate_PasswordOnly(t *testing.T) {
	cfg := &SQL{Password: "static"}
	err := cfg.validate()
	require.NoError(t, err)
}

func TestSQLValidate_PasswordCommandOnly(t *testing.T) {
	cfg := &SQL{
		PasswordCommand: &PasswordCommandConfig{
			Command: "echo",
			Args:    []string{"dynamic"},
		},
	}
	err := cfg.validate()
	require.NoError(t, err)
}

func TestSQLValidate_PasswordCommandEmptyCommand(t *testing.T) {
	cfg := &SQL{
		PasswordCommand: &PasswordCommandConfig{},
	}
	err := cfg.validate()
	require.ErrorContains(t, err, "passwordCommand.command must not be empty")
}

func TestSQLResolvePassword_Static(t *testing.T) {
	cfg := &SQL{Password: "static-pass"}
	pw, err := cfg.ResolvePassword()
	require.NoError(t, err)
	require.Equal(t, "static-pass", pw)
}

func TestSQLResolvePassword_EmptyWhenNothingSet(t *testing.T) {
	cfg := &SQL{}
	pw, err := cfg.ResolvePassword()
	require.NoError(t, err)
	require.Empty(t, pw)
}

func TestSQLResolvePassword_Command(t *testing.T) {
	cfg := &SQL{
		PasswordCommand: &PasswordCommandConfig{
			Command: "echo",
			Args:    []string{"hello"},
		},
	}
	pw, err := cfg.ResolvePassword()
	require.NoError(t, err)
	require.Equal(t, "hello", pw)
}

func TestSQLResolvePassword_CommandTrimsTrailingNewline(t *testing.T) {
	cfg := &SQL{
		PasswordCommand: &PasswordCommandConfig{
			Command: "printf",
			Args:    []string{"hello\n\n"},
		},
	}
	pw, err := cfg.ResolvePassword()
	require.NoError(t, err)
	require.Equal(t, "hello", pw)
}

func TestSQLResolvePassword_CommandFailure(t *testing.T) {
	cfg := &SQL{
		PasswordCommand: &PasswordCommandConfig{
			Command: "false",
		},
	}
	_, err := cfg.ResolvePassword()
	require.ErrorContains(t, err, "passwordCommand")
}

func TestSQLResolvePassword_CommandTimeout(t *testing.T) {
	cfg := &SQL{
		PasswordCommand: &PasswordCommandConfig{
			Command: "sleep",
			Args:    []string{"10"},
			Timeout: 10 * time.Millisecond,
		},
	}
	start := time.Now()
	_, err := cfg.ResolvePassword()
	elapsed := time.Since(start)
	require.ErrorContains(t, err, "passwordCommand")
	require.Less(t, elapsed, 5*time.Second, "command should have been killed by timeout")
}
