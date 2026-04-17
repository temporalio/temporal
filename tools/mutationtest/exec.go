package mutationtest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

func runMutationExec(ctx context.Context) int {
	outputRoot, err := parseExecOutputRoot(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitMutationSkipped
	}
	mutatedFile, err := requiredEnv("MUTATE_CHANGED")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitMutationSurvived
	}
	originalFile, err := requiredEnv("MUTATE_ORIGINAL")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitMutationSurvived
	}
	if _, err := requiredEnv("MUTATE_PACKAGE"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitMutationSurvived
	}
	logMutationDebugState("start", originalFile, mutatedFile)

	repoRoot, err := gitRepoRoot(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitMutationSkipped
	}

	originalContents, err := os.ReadFile(originalFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitMutationSkipped
	}
	diffOutput := gitDiffNoIndex(context.WithoutCancel(ctx), repoRoot, originalFile, mutatedFile)
	defer func() {
		restoreFile(originalFile, originalContents)
		logMutationDebugState("restored", originalFile, mutatedFile)
	}()

	if err := copyFile(mutatedFile, originalFile); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitMutationSurvived
	}
	logMutationDebugState("mutated", originalFile, mutatedFile)

	timeoutSeconds := getEnvDefault("MUTATE_TIMEOUT", "10")
	args := []string{
		"test",
		"-count=1",
		"-timeout", timeoutSeconds + "s",
		"-tags", getEnvDefault("MUTATION_TEST_TAGS", "test_dep"),
		"./...",
	}

	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir = repoRoot
	cmd.Env = append(os.Environ(), "GOFLAGS=")
	output, err := cmd.CombinedOutput()
	exitCode := 0
	if err != nil {
		exitErr, ok := errors.AsType[*exec.ExitError](err)
		if !ok {
			fmt.Fprintln(os.Stderr, err)
			return exitMutationSkipped
		}
		exitCode = exitErr.ExitCode()
	}

	if mutateDebug() {
		fmt.Print(string(output))
	}
	if isBuildFailureOutput(output) {
		if mutateVerbose() {
			fmt.Println("Mutation did not compile")
		}
		if mutateDebug() {
			fmt.Print(diffOutput)
		}
		return exitMutationSkipped
	}

	switch exitCode {
	case 0:
		if err := appendSurvivorDiff(filepath.Join(outputRoot, "survivors.diff"), originalFile, diffOutput); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
		fmt.Print(diffOutput)
		return exitMutationSurvived
	case 1:
		if mutateDebug() {
			fmt.Print(diffOutput)
		}
		return exitMutationKilled
	case 2:
		if mutateVerbose() {
			fmt.Println("Mutation did not compile")
		}
		if mutateDebug() {
			fmt.Print(diffOutput)
		}
		return exitMutationSkipped
	default:
		fmt.Println("Unknown exit code")
		fmt.Print(diffOutput)
		return exitCode
	}
}

func appendSurvivorDiff(path string, originalFile string, diffOutput string) error {
	if strings.TrimSpace(path) == "" || strings.TrimSpace(diffOutput) == "" {
		return nil
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := fmt.Fprintf(file, "=== %s ===\n", filepath.ToSlash(originalFile)); err != nil {
		return err
	}
	if _, err := file.WriteString(diffOutput); err != nil {
		return err
	}
	if !strings.HasSuffix(diffOutput, "\n") {
		if _, err := file.WriteString("\n"); err != nil {
			return err
		}
	}
	_, err = file.WriteString("\n")
	return err
}

func isBuildFailureOutput(output []byte) bool {
	text := string(output)
	return strings.Contains(text, "[build failed]") ||
		strings.Contains(text, "undefined:") ||
		strings.Contains(text, "syntax error:")
}

func restoreFile(path string, contents []byte) {
	_ = os.WriteFile(path, contents, 0o644)
}

func logMutationDebugState(stage string, originalFile string, mutatedFile string) {
	if !mutateDebug() {
		return
	}

	cwd, err := os.Getwd()
	if err != nil {
		cwd = "<error: " + err.Error() + ">"
	}
	fmt.Fprintf(
		os.Stderr,
		"[exec] stage=%s cwd=%s original=%s original_stat=%s mutated=%s mutated_stat=%s\n",
		stage,
		cwd,
		originalFile,
		describePath(originalFile),
		mutatedFile,
		describePath(mutatedFile),
	)
}

func describePath(path string) string {
	info, err := os.Stat(path)
	if err != nil {
		return err.Error()
	}
	return fmt.Sprintf("mode=%s size=%d", info.Mode(), info.Size())
}

func requiredEnv(name string) (string, error) {
	value, ok := os.LookupEnv(name)
	if !ok || strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("%s is not set", name)
	}
	return value, nil
}

func copyFile(src string, dst string) error {
	source, err := os.Open(src)
	if err != nil {
		return err
	}
	defer source.Close()

	destination, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destination.Close()

	if _, err := io.Copy(destination, source); err != nil {
		return err
	}
	return destination.Close()
}

func mutateDebug() bool {
	return parseBoolEnv("MUTATION_HOOK_DEBUG") || parseBoolEnv("MUTATE_DEBUG")
}

func mutateVerbose() bool {
	return parseBoolEnv("MUTATE_VERBOSE")
}

func parseBoolEnv(name string) bool {
	value := os.Getenv(name)
	ok, err := strconv.ParseBool(value)
	return err == nil && ok
}

func getEnvDefault(name string, defaultValue string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return defaultValue
}
