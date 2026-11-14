package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMaxTerms(t *testing.T) {
	t.Parallel()

	binary := filepath.Join(t.TempDir(), "lintconditions")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, ".")
	output, err := build.CombinedOutput()
	require.NoError(t, err, "%s", output)

	for _, tc := range []struct {
		name       string
		limit      string
		expression string
		wantError  string
	}{
		{
			name:       "higher limit",
			limit:      "4",
			expression: "a && b && c && d",
		},
		{
			name:       "lower limit",
			limit:      "2",
			expression: "a && b && c",
			wantError:  "boolean chain has 3 terms (limit 2)",
		},
		{
			name:       "minimum limit",
			limit:      "1",
			expression: "a == b",
		},
		{
			name:       "zero limit",
			limit:      "0",
			expression: "a",
			wantError:  "max-terms must be at least 1",
		},
		{
			name:       "negative limit",
			limit:      "-1",
			expression: "a",
			wantError:  "max-terms must be at least 1",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module example.com/conditions\n"), 0o600))
			source := "package conditions\nfunc Check(a, b, c, d bool) bool { return " + tc.expression + " }\n"
			require.NoError(t, os.WriteFile(filepath.Join(dir, "conditions.go"), []byte(source), 0o600))

			command := exec.CommandContext(t.Context(), binary, "-max-terms="+tc.limit, "./...")
			command.Dir = dir
			output, err := command.CombinedOutput()
			if tc.wantError != "" {
				require.Error(t, err)
				require.Contains(t, string(output), tc.wantError)
				return
			}
			require.NoError(t, err, "%s", output)
			require.Empty(t, string(output))
		})
	}
}
