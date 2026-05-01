package testrunner2

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
)

type packageAttemptResult struct {
	exitCode int
	logPath  string
	report   *junitReport
	alerts   alerts
}

func (r *runner) runPackageTests(ctx context.Context, args []string) error {
	args = stripArgSeparator(args)
	testDirs, _, _ := parseTestArgs(args)
	if len(testDirs) == 0 {
		return errors.New("no test directories specified")
	}

	if r.totalShards > 1 {
		args = replaceTestDirs(args, shardPackageDirs(testDirs, r.totalShards, r.shardIndex))
		testDirs, _, _ = parseTestArgs(args)
		r.log("shard %d/%d (group-by=%s)", r.shardIndex+1, r.totalShards, r.groupBy)
	}
	if len(testDirs) == 0 {
		return fmt.Errorf("no test packages selected for shard %d/%d", r.shardIndex+1, r.totalShards)
	}

	r.junitReports = nil
	r.alerts = nil
	r.errors = nil
	r.tracker.reset()
	r.resetRetryOrdinals()

	if err := os.MkdirAll(r.logDir, 0755); err != nil {
		return fmt.Errorf("failed to create log directory: %w", err)
	}
	r.log("test packages: %v", testDirs)
	r.log("log directory: %s", r.logDir)

	currentArgs := args
	var lastExitCode int
	var attemptsRun int
	for attempt := 1; attempt <= r.maxAttempts; attempt++ {
		attemptsRun = attempt
		result, err := r.runPackageAttempt(ctx, currentArgs, attempt)
		if err != nil {
			return err
		}
		lastExitCode = result.exitCode
		r.addReport(result.report)
		r.addAlerts(result.alerts)

		if result.exitCode == 0 {
			_ = os.Remove(result.logPath)
			break
		}
		if ctx.Err() != nil {
			r.addError(fmt.Errorf("go test timed out on attempt %d", attempt))
			break
		}
		if attempt >= r.maxAttempts {
			r.addError(fmt.Errorf("go test failed on attempt %d", attempt))
			break
		}

		failures := result.report.collectTestCaseFailureRefs()
		if len(failures) == 0 {
			r.addError(fmt.Errorf("go test failed on attempt %d without reported test failures", attempt))
			break
		}

		currentArgs = packageRetryArgs(args, failures)
		r.log("🔄 scheduling package retry: %s in %v (attempt %d)",
			packageRetryRunPattern(failures), packageRetryDirs(failures), attempt+1)
	}

	if len(r.alerts) > 0 {
		alertsReport := &junitReport{}
		alertsReport.appendAlerts(r.alerts)
		r.junitReports = append(r.junitReports, alertsReport)
	}

	if err := r.finalizeReport(r.junitReports); err != nil {
		return err
	}
	if len(r.errors) > 0 {
		return errors.Join(r.errors...)
	}
	if lastExitCode != 0 {
		return fmt.Errorf("go test failed after %d attempt(s)", attemptsRun)
	}
	r.log("test run completed")
	return nil
}

func (r *runner) runPackageAttempt(ctx context.Context, args []string, attempt int) (packageAttemptResult, error) {
	start := time.Now()
	attemptArgs := replaceCoverProfile(args, packageAttemptCoverProfile(r.coverProfilePath, attempt))
	goArgs := r.goTestArgs(attemptArgs)
	logPath, _ := filepath.Abs(buildLogFilename(r.logDir, "go-test-packages", attempt))
	junitPath := filepath.Join(os.TempDir(), fmt.Sprintf("temporalio-temporal-%s-junit.xml", uuid.NewString()))

	lc, err := newLogCapture(logCaptureConfig{
		LogPath: logPath,
		Header: &logFileHeader{
			Package: "go test",
			Attempt: fmt.Sprintf("%d", attempt),
			Started: time.Now(),
			Command: "go " + strings.Join(goArgs, " "),
		},
	})
	if err != nil {
		return packageAttemptResult{}, fmt.Errorf("failed to create log file: %w", err)
	}

	writer := newGoTestJSONWriter(lc, os.Stdout)
	r.console.WriteGrouped(
		fmt.Sprintf("%s%s 🚀 go test packages (attempt %d)", logPrefix, time.Now().Format("15:04:05"), attempt),
		"$ go "+strings.Join(goArgs, " ")+"\n",
	)
	exitCode := r.exec(ctx, "", "go", goArgs, append(r.env, fmt.Sprintf("TEMPORAL_TEST_ATTEMPT=%d", attempt)), writer)
	writer.Close()
	_ = lc.Close()

	results := newJUnitReport(logPath, junitPath)
	report := &junitReport{path: junitPath, attempt: attempt}
	if err := report.read(); err != nil {
		return packageAttemptResult{}, fmt.Errorf("failed to read junit report for package attempt %d: %w", attempt, err)
	}

	status := "✅"
	if exitCode != 0 || report.Failures > 0 || report.Errors > 0 {
		status = "❌️"
	}
	r.log("%s package attempt %d (passed=%d/%d, failed=%d, errors=%d, runtime=%v)",
		status,
		attempt,
		report.Tests-report.Failures-report.Errors,
		report.Tests,
		report.Failures,
		report.Errors,
		time.Since(start).Round(time.Second),
	)

	if exitCode != 0 && len(results.failures) > 0 {
		writePackageFailures(r, results)
	}

	return packageAttemptResult{
		exitCode: exitCode,
		logPath:  logPath,
		report:   report,
		alerts:   parseAlerts(writer.PlainOutput()),
	}, nil
}

func (r *runner) goTestArgs(args []string) []string {
	testDirs, baseArgs, testBinaryArgs := parseTestArgs(args)
	baseArgs = slices.Clone(baseArgs)
	if !hasFlag(baseArgs, "-json") {
		baseArgs = append([]string{"-json"}, baseArgs...)
	}
	if r.timeout > 0 && !hasFlag(baseArgs, timeoutFlag) {
		baseArgs = append(baseArgs, timeoutFlag+"="+r.timeout.String())
	}
	if r.buildTags != "" && !hasFlag(baseArgs, tagsFlag) {
		baseArgs = append(baseArgs, tagsFlag+"="+r.buildTags)
	}
	if r.runFilter != "" && !hasFlag(baseArgs, runFlag) {
		baseArgs = append(baseArgs, runFlag, r.runFilter)
	}

	goArgs := []string{"test"}
	goArgs = append(goArgs, baseArgs...)
	goArgs = append(goArgs, testDirs...)
	if len(testBinaryArgs) > 0 {
		goArgs = append(goArgs, "-args")
		goArgs = append(goArgs, testBinaryArgs...)
	}
	return goArgs
}

func writePackageFailures(r *runner, results testResults) {
	var body strings.Builder
	for _, failure := range results.failures {
		fmt.Fprintf(&body, "--- %s\n", failure.Name)
		if failure.ErrorTrace != "" {
			body.WriteString(failure.ErrorTrace)
			if !strings.HasSuffix(failure.ErrorTrace, "\n") {
				body.WriteByte('\n')
			}
		}
	}
	r.console.WriteGrouped(
		fmt.Sprintf("%s%s package failures", logPrefix, time.Now().Format("15:04:05")),
		body.String(),
	)
}

func packageAttemptCoverProfile(path string, attempt int) string {
	return fmt.Sprintf("%s_%d%s",
		strings.TrimSuffix(path, codeCoverageExtension),
		attempt-1,
		codeCoverageExtension)
}

func replaceCoverProfile(args []string, coverProfile string) []string {
	out := slices.Clone(args)
	for i := 0; i < len(out); i++ {
		switch {
		case out[i] == coverProfileFlag && i+1 < len(out):
			i++
			out[i] = coverProfile
		case strings.HasPrefix(out[i], coverProfileFlag+"="):
			out[i] = coverProfileFlag + "=" + coverProfile
		default:
		}
	}
	return out
}

func packageRetryArgs(originalArgs []string, failures []testFailureRef) []string {
	testDirs, baseArgs, testBinaryArgs := parseTestArgs(originalArgs)
	baseArgs = stripRunFromArgs(baseArgs)
	baseArgs = append(baseArgs, runFlag, packageRetryRunPattern(failures))

	args := append([]string{}, baseArgs...)
	dirs := packageRetryDirs(failures)
	if len(dirs) == 0 {
		dirs = testDirs
	}
	args = append(args, dirs...)
	if len(testBinaryArgs) > 0 {
		args = append(args, "-args")
		args = append(args, testBinaryArgs...)
	}
	return args
}

func packageRetryRunPattern(failures []testFailureRef) string {
	var names []string
	seen := make(map[string]bool)
	for _, failure := range failures {
		name := failure.name
		if idx := strings.IndexByte(name, '/'); idx >= 0 {
			name = name[:idx]
		}
		if !seen[name] {
			seen[name] = true
			names = append(names, name)
		}
	}
	return regexAlternation(names)
}

func packageRetryDirs(failures []testFailureRef) []string {
	var dirs []string
	seen := make(map[string]bool)
	for _, failure := range failures {
		if failure.pkg == "" || seen[failure.pkg] {
			continue
		}
		seen[failure.pkg] = true
		dirs = append(dirs, failure.pkg)
	}
	slices.Sort(dirs)
	return dirs
}

func shardPackageDirs(dirs []string, totalShards, shardIndex int) []string {
	var selected []string
	for _, dir := range dirs {
		if getShardForKey(dir, totalShards) == shardIndex {
			selected = append(selected, dir)
		}
	}
	return selected
}

func replaceTestDirs(args []string, dirs []string) []string {
	_, baseArgs, testBinaryArgs := parseTestArgs(args)
	out := append([]string{}, baseArgs...)
	out = append(out, dirs...)
	if len(testBinaryArgs) > 0 {
		out = append(out, "-args")
		out = append(out, testBinaryArgs...)
	}
	return out
}

func stripArgSeparator(args []string) []string {
	out := make([]string, 0, len(args))
	for _, arg := range args {
		if arg != "--" {
			out = append(out, arg)
		}
	}
	return out
}

func stripRunFromArgs(args []string) []string {
	var out []string
	var skipNext bool
	for _, arg := range args {
		if skipNext {
			skipNext = false
			continue
		}
		if arg == runFlag {
			skipNext = true
			continue
		}
		if strings.HasPrefix(arg, runFlag+"=") {
			continue
		}
		out = append(out, arg)
	}
	return out
}

func hasFlag(args []string, flag string) bool {
	for _, arg := range args {
		if arg == flag || strings.HasPrefix(arg, flag+"=") {
			return true
		}
	}
	return false
}

type goTestJSONWriter struct {
	raw     io.Writer
	console io.Writer
	plain   bytes.Buffer
	lineBuf []byte
}

func newGoTestJSONWriter(raw, console io.Writer) *goTestJSONWriter {
	return &goTestJSONWriter{raw: raw, console: console}
}

func (w *goTestJSONWriter) Write(p []byte) (int, error) {
	if _, err := w.raw.Write(p); err != nil {
		return 0, err
	}
	w.lineBuf = append(w.lineBuf, p...)
	w.processLines(false)
	return len(p), nil
}

func (w *goTestJSONWriter) Close() {
	w.processLines(true)
}

func (w *goTestJSONWriter) PlainOutput() string {
	return w.plain.String()
}

func (w *goTestJSONWriter) processLines(flush bool) {
	for {
		idx := bytes.IndexByte(w.lineBuf, '\n')
		if idx < 0 {
			if flush && len(w.lineBuf) > 0 {
				w.writePlainLine(w.lineBuf)
				w.lineBuf = nil
			}
			return
		}
		line := w.lineBuf[:idx+1]
		w.lineBuf = w.lineBuf[idx+1:]
		w.writePlainLine(line)
	}
}

func (w *goTestJSONWriter) writePlainLine(line []byte) {
	var ev struct {
		Output string `json:"Output"`
	}
	output := string(line)
	if err := json.Unmarshal(bytes.TrimSpace(line), &ev); err == nil {
		output = ev.Output
	}
	if output == "" {
		return
	}
	w.plain.WriteString(output)
	_, _ = io.WriteString(w.console, output)
}
