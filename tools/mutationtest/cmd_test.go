package mutationtest

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/await"
)

func TestLabeledCommandStopsDescendantsWhenContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	readyPath := filepath.Join(t.TempDir(), "ready")
	cmd := labeledCommand(ctx, "", os.Args[0], "-test.run=^TestLabeledCommandHelper$")
	cmd.Env = append(os.Environ(),
		"MUTATION_TEST_COMMAND_HELPER=parent",
		"MUTATION_TEST_COMMAND_READY="+readyPath,
	)
	done := make(chan error, 1)
	go func() {
		_, err := cmd.CombinedOutput()
		done <- err
	}()
	await.RequireTrue(t, func() bool {
		_, err := os.Stat(readyPath)
		return err == nil
	}, time.Second, 10*time.Millisecond)

	canceledAt := time.Now()
	cancel()
	select {
	case err := <-done:
		require.Error(t, err)
		require.Less(t, time.Since(canceledAt), time.Second)
	case <-time.After(time.Second):
		require.Fail(t, "command did not stop within the cancellation bound")
	}
}

func TestLabeledCommandHelper(t *testing.T) {
	switch os.Getenv("MUTATION_TEST_COMMAND_HELPER") {
	case "parent":
		child := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestLabeledCommandHelper$")
		child.Env = append(commandHelperEnv(), "MUTATION_TEST_COMMAND_HELPER=child")
		child.Stdout = os.Stdout
		child.Stderr = os.Stderr
		require.NoError(t, child.Start())
		require.NoError(t, os.WriteFile(os.Getenv("MUTATION_TEST_COMMAND_READY"), nil, 0o644))
		<-time.After(3 * time.Second)
	case "child":
		<-time.After(3 * time.Second)
	default:
		return
	}
}

func commandHelperEnv() []string {
	env := make([]string, 0, len(os.Environ()))
	for _, variable := range os.Environ() {
		if strings.HasPrefix(variable, "MUTATION_TEST_COMMAND_HELPER=") {
			continue
		}
		env = append(env, variable)
	}
	return env
}
