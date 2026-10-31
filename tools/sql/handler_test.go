package sql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli"
	"go.temporal.io/server/common/config"
)

func TestParseConnectConfigPasswordCommand(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		args     []string
		expected *config.PasswordCommandConfig
	}{
		{
			name:     "no password command",
			args:     nil,
			expected: nil,
		},
		{
			name: "command only",
			args: []string{"--password-command", "/bin/token"},
			expected: &config.PasswordCommandConfig{
				Command: "/bin/token",
				Args:    []string{},
			},
		},
		{
			name: "command with args and timeout",
			args: []string{
				"--password-command", "aws",
				"--password-command-arg", "rds",
				"--password-command-arg", "generate-db-auth-token",
				"--password-command-timeout", "10s",
			},
			expected: &config.PasswordCommandConfig{
				Command: "aws",
				Args:    []string{"rds", "generate-db-auth-token"},
				Timeout: 10 * time.Second,
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := runParseConnectConfig(t, tc.args)
			require.NoError(t, err)
			require.Equal(t, tc.expected, cfg.PasswordCommand)
		})
	}
}

func TestParseConnectConfigPasswordCommandInvalid(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		args        []string
		expectedErr string
	}{
		{
			name:        "password and command",
			args:        []string{"--password", "secret", "--password-command", "aws"},
			expectedErr: "mutually exclusive",
		},
		{
			name:        "arg without command",
			args:        []string{"--password-command-arg", "rds"},
			expectedErr: "require (-password-command)",
		},
		{
			name:        "timeout without command",
			args:        []string{"--password-command-timeout", "10s"},
			expectedErr: "require (-password-command)",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := runParseConnectConfig(t, tc.args)
			require.ErrorContains(t, err, tc.expectedErr)
		})
	}
}

//parallelize:ignore
func TestParseConnectConfigPasswordCommandFromEnv(t *testing.T) {
	t.Setenv("SQL_PASSWORD_COMMAND", "aws")
	t.Setenv("SQL_PASSWORD_COMMAND_ARGS", "rds,generate-db-auth-token")
	t.Setenv("SQL_PASSWORD_COMMAND_TIMEOUT", "10s")

	cfg, err := runParseConnectConfig(t, nil)
	require.NoError(t, err)
	require.Equal(t, &config.PasswordCommandConfig{
		Command: "aws",
		Args:    []string{"rds", "generate-db-auth-token"},
		Timeout: 10 * time.Second,
	}, cfg.PasswordCommand)
}

func runParseConnectConfig(t *testing.T, args []string) (*config.SQL, error) {
	var cfg *config.SQL
	var parseErr error
	app := BuildCLIOptions()
	app.Action = func(c *cli.Context) error {
		cfg, parseErr = parseConnectConfig(c)
		return nil
	}
	baseArgs := []string{"temporal-sql-tool", "--endpoint", "127.0.0.1", "--port", "3306", "--database", "temporal"}
	require.NoError(t, app.Run(append(baseArgs, args...)))
	return cfg, parseErr
}
