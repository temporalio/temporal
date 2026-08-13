package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.temporal.io/server/common/testing/umpire/verify"
)

type Backend string

const (
	SANY          Backend = "sany"
	TLC           Backend = "tlc"
	Apalache      Backend = "apalache"
	ApalacheProof Backend = "apalache-proof"
	P             Backend = "p"
	PEx           Backend = "pex"
	Ivy           Backend = "ivy"
)

type Request struct {
	Backend       Backend
	Target        string
	Profile       string
	ToolPath      string
	JavaPath      string
	ToolVersion   string
	ModelDir      string
	ArtifactDir   string
	Config        string
	Timeout       time.Duration
	Bounds        verify.Bounds
	ActionNames   map[string]string
	PropertyNames map[string]string
	Fairness      []string
	Abstractions  []verify.Abstraction
	Unsupported   []verify.Unsupported
}

type command struct {
	path string
	args []string
	dir  string
	env  []string
}

type execution struct {
	output string
	stdout string
	stderr string
	err    error
}

type executor interface {
	run(context.Context, command) execution
}

type osExecutor struct{}

func (osExecutor) run(ctx context.Context, specification command) execution {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	process := exec.CommandContext(ctx, specification.path, specification.args...)
	process.Dir = specification.dir
	process.Env = append(os.Environ(), specification.env...)
	process.Stdout = &stdout
	process.Stderr = &stderr
	err := process.Run()
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	return execution{
		output: stdout.String() + stderr.String(),
		stdout: stdout.String(),
		stderr: stderr.String(),
		err:    err,
	}
}

func Check(ctx context.Context, request Request) (verify.Result, error) {
	modelDirectory, err := filepath.Abs(request.ModelDir)
	if err != nil {
		return verify.Result{}, err
	}
	request.ModelDir = modelDirectory
	if request.ArtifactDir != "" {
		artifactDirectory, err := filepath.Abs(request.ArtifactDir)
		if err != nil {
			return verify.Result{}, err
		}
		request.ArtifactDir = artifactDirectory
	}
	return check(ctx, osExecutor{}, request)
}

func check(ctx context.Context, executor executor, request Request) (verify.Result, error) {
	if request.ToolPath == "" {
		return verify.Result{}, errors.New("verification tool path is empty")
	}
	if request.ModelDir == "" {
		return verify.Result{}, errors.New("verification model directory is empty")
	}
	if request.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, request.Timeout)
		defer cancel()
	}
	execution, replay, err := execute(ctx, executor, request)
	if err != nil {
		return verify.Result{}, err
	}
	result := classify(request, execution)
	if len(replay) == 1 {
		result.ReplayCommand = replay[0]
	} else {
		result.ReplayCommands = replay
	}
	result.Bounds = request.Bounds
	result.StandardOutput = execution.stdout
	result.StandardError = execution.stderr
	if request.ArtifactDir != "" {
		result.Artifacts = append(result.Artifacts, nativeArtifacts(request)...)
		artifacts, err := writeArtifacts(request.ArtifactDir, request.Backend, result)
		if err != nil {
			return verify.Result{}, err
		}
		result.Artifacts = append(result.Artifacts, artifacts...)
	}
	if err := verify.ValidateResult(result); err != nil {
		return verify.Result{}, err
	}
	return result, nil
}

func execute(ctx context.Context, executor executor, request Request) (execution, [][]string, error) {
	switch request.Backend {
	case SANY:
		java := request.JavaPath
		if java == "" {
			java = "java"
		}
		args := []string{"-cp", request.ToolPath, "tla2sany.SANY", "Umpire.tla"}
		return executor.run(ctx, command{path: java, args: args, dir: request.ModelDir}), [][]string{append([]string{java}, args...)}, nil
	case TLC:
		java := request.JavaPath
		if java == "" {
			java = "java"
		}
		config := request.Config
		if config == "" {
			config = "Umpire-smoke.cfg"
		}
		args := []string{"-cp", request.ToolPath, "tlc2.TLC", "-workers", "1", "-config", config}
		if request.ArtifactDir != "" {
			nativeOutput := filepath.Join(request.ArtifactDir, "tlc-native")
			if err := os.RemoveAll(nativeOutput); err != nil {
				return execution{}, nil, err
			}
			if err := os.MkdirAll(nativeOutput, 0o755); err != nil {
				return execution{}, nil, err
			}
			args = append(args, "-metadir", nativeOutput)
		}
		args = append(args, "Umpire.tla")
		return executor.run(ctx, command{path: java, args: args, dir: request.ModelDir}), [][]string{append([]string{java}, args...)}, nil
	case Apalache:
		config := request.Config
		if config == "" {
			config = "Umpire-smoke.cfg"
		}
		var args []string
		if request.ArtifactDir != "" {
			nativeOutput := filepath.Join(request.ArtifactDir, "apalache-native")
			if err := os.RemoveAll(nativeOutput); err != nil {
				return execution{}, nil, err
			}
			if err := os.MkdirAll(nativeOutput, 0o755); err != nil {
				return execution{}, nil, err
			}
			args = append(args, "--out-dir="+nativeOutput)
		}
		args = append(args, "check", "--config="+config, "--inv=Safety,QuiescentSafety", "--no-deadlock", "--output-traces=true")
		if request.Bounds.MaxDepth != 0 {
			args = append(args, "--length="+strconv.FormatUint(request.Bounds.MaxDepth, 10))
		}
		args = append(args, "Umpire.tla")
		environment := javaEnvironment(request.JavaPath)
		return executor.run(ctx, command{path: request.ToolPath, args: args, dir: request.ModelDir, env: environment}), [][]string{replayCommand(environment, request.ToolPath, args)}, nil
	case ApalacheProof:
		return executeApalacheProof(ctx, executor, request)
	case P, PEx:
		return executeP(ctx, executor, request)
	case Ivy:
		args := []string{filepath.Join(request.ModelDir, "Umpire.ivy")}
		return executor.run(ctx, command{path: request.ToolPath, args: args, dir: request.ModelDir}), [][]string{append([]string{request.ToolPath}, args...)}, nil
	default:
		return execution{}, nil, fmt.Errorf("unknown verification backend %q", request.Backend)
	}
}

func executeApalacheProof(ctx context.Context, executor executor, request Request) (execution, [][]string, error) {
	config := request.Config
	if config == "" {
		config = "Umpire-smoke.cfg"
	}
	type obligation struct {
		name string
		args []string
	}
	obligations := []obligation{
		{name: "init", args: []string{"--init=Init", "--inv=InductiveInvariant", "--length=0"}},
		{name: "consecution", args: []string{"--init=InductiveInvariant", "--next=Next", "--inv=InductiveInvariant", "--length=1"}},
		{name: "safety", args: []string{"--init=InductiveInvariant", "--inv=DeclaredSafety", "--length=0"}},
	}
	environment := javaEnvironment(request.JavaPath)
	if request.ArtifactDir != "" {
		nativeOutput := filepath.Join(request.ArtifactDir, "apalache-proof-native")
		if err := os.RemoveAll(nativeOutput); err != nil {
			return execution{}, nil, err
		}
	}
	var combined execution
	var replay [][]string
	for _, proof := range obligations {
		args := []string{}
		if request.ArtifactDir != "" {
			nativeOutput := filepath.Join(request.ArtifactDir, "apalache-proof-native", proof.name)
			if err := os.RemoveAll(nativeOutput); err != nil {
				return execution{}, nil, err
			}
			if err := os.MkdirAll(nativeOutput, 0o755); err != nil {
				return execution{}, nil, err
			}
			args = append(args, "--out-dir="+nativeOutput)
		}
		args = append(args, "check", "--config="+config)
		args = append(args, proof.args...)
		args = append(args, "--no-deadlock", "--output-traces=true", "Umpire.tla")
		marker := "UMPIRE_PROOF_OBLIGATION " + proof.name + "\n"
		actual := executor.run(ctx, command{path: request.ToolPath, args: args, dir: request.ModelDir, env: environment})
		combined.output += marker + actual.output
		combined.stdout += marker + actual.stdout
		combined.stderr += actual.stderr
		replay = append(replay, replayCommand(environment, request.ToolPath, args))
		if actual.err != nil {
			combined.err = actual.err
			break
		}
	}
	return combined, replay, nil
}

func executeP(ctx context.Context, executor executor, request Request) (_ execution, _ [][]string, retErr error) {
	mode := "bugfinding"
	if request.Backend == PEx {
		mode = "pex"
	}
	workDirectory, err := os.MkdirTemp("", "umpire-p-run-")
	if err != nil {
		return execution{}, nil, err
	}
	defer func() {
		retErr = errors.Join(retErr, os.RemoveAll(workDirectory))
	}()
	for _, name := range []string{"Umpire.p", "Umpire.pproj"} {
		contents, err := os.ReadFile(filepath.Join(request.ModelDir, name))
		if err != nil {
			return execution{}, nil, err
		}
		if err := os.WriteFile(filepath.Join(workDirectory, name), contents, 0o600); err != nil {
			return execution{}, nil, err
		}
	}
	project := filepath.Join(workDirectory, "Umpire.pproj")
	compileArgs := []string{"compile", "--pproj", project, "--mode", mode}
	environment := javaEnvironment(request.JavaPath)
	compiled := executor.run(ctx, command{path: request.ToolPath, args: compileArgs, dir: workDirectory, env: environment})
	if compiled.err != nil {
		return compiled, [][]string{replayCommand(environment, request.ToolPath, compileArgs)}, nil
	}
	dll, err := findPAssembly(workDirectory, mode)
	if err != nil {
		compiled.err = err
		compiled.output += "\n" + err.Error()
		return compiled, [][]string{replayCommand(environment, request.ToolPath, compileArgs)}, nil
	}
	checkArgs := pCheckArgs(dll, mode, request)
	nativeOutput := filepath.Join(workDirectory, "PCheckerOutput")
	if request.ArtifactDir != "" {
		nativeOutput = filepath.Join(request.ArtifactDir, strings.ToLower(mode)+"-native")
	}
	if err := os.RemoveAll(nativeOutput); err != nil {
		return execution{}, nil, err
	}
	if err := os.MkdirAll(nativeOutput, 0o755); err != nil {
		return execution{}, nil, err
	}
	if request.ArtifactDir != "" {
		persistentAssembly := filepath.Join(nativeOutput, filepath.Base(dll))
		if err := copyFile(dll, persistentAssembly); err != nil {
			return execution{}, nil, err
		}
		checkArgs[1] = persistentAssembly
	}
	checkArgs = append(checkArgs, "--outdir", nativeOutput)
	checked := executor.run(ctx, command{path: request.ToolPath, args: checkArgs, dir: workDirectory, env: environment})
	evidence, evidenceErr := collectPTraceEvidence(nativeOutput)
	if evidenceErr != nil {
		return execution{}, nil, evidenceErr
	}
	if evidence != "" {
		checked.stdout += "\n" + evidence
		checked.output += "\n" + evidence
	}
	checked.stdout = compiled.stdout + checked.stdout
	checked.stderr = compiled.stderr + checked.stderr
	checked.output = compiled.output + checked.output
	return checked, [][]string{replayCommand(environment, request.ToolPath, checkArgs)}, nil
}

func pCheckArgs(assembly, mode string, request Request) []string {
	result := []string{"check", assembly, "--mode", mode, "--testcase", "tcUmpire"}
	if request.Bounds.Schedules != 0 {
		result = append(result, "--schedules", strconv.FormatUint(request.Bounds.Schedules, 10))
	}
	if request.Bounds.MaxDepth != 0 {
		result = append(result, "--max-steps", strconv.FormatUint(request.Bounds.MaxDepth, 10))
	}
	return result
}

func copyFile(source, target string) error {
	contents, err := os.ReadFile(source)
	if err != nil {
		return err
	}
	return os.WriteFile(target, contents, 0o600)
}

func collectPTraceEvidence(directory string) (string, error) {
	var paths []string
	err := filepath.WalkDir(directory, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		extension := filepath.Ext(entry.Name())
		if extension == ".log" || extension == ".txt" {
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	slices.Sort(paths)
	var result strings.Builder
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			return "", err
		}
		if info.Size() > 4<<20 {
			continue
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&result, "--- %s ---\n%s\n", filepath.Base(path), contents)
	}
	return result.String(), nil
}

func javaEnvironment(javaPath string) []string {
	if javaPath == "" {
		return nil
	}
	bin := filepath.Dir(javaPath)
	return []string{
		"JAVA_HOME=" + filepath.Dir(bin),
		"PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"),
	}
}

func replayCommand(environment []string, path string, args []string) []string {
	if len(environment) == 0 {
		return append([]string{path}, args...)
	}
	result := append([]string{"env"}, environment...)
	result = append(result, path)
	return append(result, args...)
}

func findPAssembly(directory, mode string) (string, error) {
	requested := "PChecker"
	artifact := "Umpire.dll"
	if mode == "pex" {
		requested = "PEx"
		artifact = "Umpire-jar-with-dependencies.jar"
	}
	var candidates []string
	err := filepath.WalkDir(directory, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && entry.Name() == artifact && strings.Contains(path, requested) && !strings.Contains(path, string(filepath.Separator)+"obj"+string(filepath.Separator)) {
			candidates = append(candidates, path)
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	if len(candidates) != 1 {
		return "", fmt.Errorf("expected one generated %s %s, found %d", requested, artifact, len(candidates))
	}
	return candidates[0], nil
}

var (
	stateCountsPattern = regexp.MustCompile(`(?m)(\d+) states generated, (\d+) distinct states found`)
	propertyPattern    = regexp.MustCompile(`(?i)(?:property|invariant)[ :\[]+([A-Za-z0-9_.-]+)`)
	pPropertyPattern   = regexp.MustCompile(`(?i)property\s+([A-Za-z0-9_.-]+)\s+failed`)
	actionPattern      = regexp.MustCompile(`(?m)UMPIRE_ACTION\s+(\S+)([^\r\n]*)`)
	bindingPattern     = regexp.MustCompile(`(\S+)=([^ ]+)`)
	tlaActionPattern   = regexp.MustCompile(`(?m)^State \d+: <([A-Za-z0-9_]+)(?:\(([^)]*)\))?`)
	tlaBindingPattern  = regexp.MustCompile(`([A-Za-z0-9_]+)\s*=\s*"([^"]*)"`)
	invariantViolation = regexp.MustCompile(`(?i)invariant\s+[A-Za-z0-9_.-]+\s+is violated`)
	timeoutPattern     = regexp.MustCompile(`(?i)(timed out|timeout (?:after|reached|exceeded)|time limit)`)
	ivySuccessPattern  = regexp.MustCompile(`(?m)^OK\s*$`)
	ivyActionPattern   = regexp.MustCompile(`^\s*\(internal\)\s+([A-Za-z0-9_]+)\s*$`)
	ivyFailurePattern  = regexp.MustCompile(`line \d+:\s+([A-Za-z0-9_]+)\s+\.\.\.\s+FAIL`)
)

func classify(request Request, execution execution) verify.Result {
	output := execution.output
	result := verify.Result{
		Backend:      string(request.Backend),
		Target:       request.Target,
		Profile:      request.Profile,
		ToolVersion:  request.ToolVersion,
		Termination:  verify.Completed,
		Fairness:     slices.Clone(request.Fairness),
		Abstractions: slices.Clone(request.Abstractions),
		Unsupported:  slices.Clone(request.Unsupported),
	}
	if errors.Is(execution.err, context.DeadlineExceeded) || errors.Is(execution.err, context.Canceled) {
		result.Status = verify.Inconclusive
		if errors.Is(execution.err, context.DeadlineExceeded) {
			result.Termination = verify.Timeout
		} else {
			result.Termination = verify.Interrupted
		}
		return result
	}
	lower := strings.ToLower(output)
	switch {
	case strings.Contains(lower, "max scheduling steps") || strings.Contains(lower, "maximum number of steps"):
		result.Status = verify.Inconclusive
		result.Termination = verify.StepLimit
	case timeoutPattern.MatchString(output):
		result.Status = verify.Inconclusive
		result.Termination = verify.Timeout
	case strings.Contains(lower, "memory limit") || strings.Contains(lower, "out of memory"):
		result.Status = verify.Inconclusive
		result.Termination = verify.MemoryLimit
	case request.Backend == PEx && strings.Contains(lower, "cycle detected: infinite loop"):
		result.Status = verify.Inconclusive
		result.Termination = verify.ToolError
		result.Diagnostic = "PEx reported an implicit cycle outside the generated Umpire properties"
	case request.Backend == PEx && strings.Contains(lower, "toomanychoicesexception"):
		result.Status = verify.Inconclusive
		result.Termination = verify.StepLimit
		result.Diagnostic = "PEx reached its native per-statement choice limit"
	case isCounterexample(lower):
		result.Status = verify.Counterexample
		result.FailedProperty = failedProperty(request, output)
		if result.FailedProperty == "" {
			result.FailedProperty = "unknown"
		}
		result.Trace = normalizeActions(request, output)
	case execution.err != nil:
		result.Status = verify.Inconclusive
		result.Termination = verify.ToolError
		result.Diagnostic = execution.err.Error()
	case request.Backend == SANY && strings.Contains(output, "Semantic processing of module"):
		result.Status = verify.Generated
	case request.Backend == TLC && strings.Contains(output, "Model checking completed. No error has been found"):
		result.Status = verify.FiniteExhaustive
	case request.Backend == ApalacheProof && strings.Count(lower, "checker reports no error") == 3:
		result.Status = verify.InvariantProved
	case request.Backend == Apalache && (strings.Contains(output, "NoError") || strings.Contains(lower, "no counterexample") || strings.Contains(lower, "checker reports no error")):
		result.Status = verify.BoundedNoCounterexample
	case (request.Backend == P || request.Backend == PEx) && strings.Contains(lower, "found 0 bugs"):
		result.Status = verify.BoundedNoCounterexample
	case request.Backend == Ivy && (ivySuccessPattern.MatchString(output) || strings.Contains(lower, "finished with 0 errors")):
		result.Status = verify.InvariantProved
	default:
		result.Status = verify.Inconclusive
		result.Termination = verify.ParseFailure
		result.Diagnostic = "tool output did not contain a recognized completion marker"
	}
	if matches := stateCountsPattern.FindStringSubmatch(output); len(matches) == 3 {
		result.GeneratedStates, _ = strconv.ParseUint(matches[1], 10, 64)
		result.DistinctStates, _ = strconv.ParseUint(matches[2], 10, 64)
	}
	return result
}

func isCounterexample(lower string) bool {
	return strings.Contains(lower, "assertion failed") ||
		strings.Contains(lower, "invariant is violated") ||
		strings.Contains(lower, "invariant violation") ||
		strings.Contains(lower, "counterexample found") ||
		strings.Contains(lower, "checker found a bug") ||
		strings.Contains(lower, "found 1 bug") ||
		strings.Contains(lower, "property violated") ||
		strings.Contains(lower, "error: failed checks:") ||
		invariantViolation.MatchString(lower)
}

func failedProperty(request Request, output string) string {
	name := ""
	if request.Backend == P || request.Backend == PEx {
		if matches := pPropertyPattern.FindStringSubmatch(output); len(matches) == 2 {
			name = matches[1]
		}
	}
	if request.Backend == Ivy {
		if matches := ivyFailurePattern.FindStringSubmatch(output); len(matches) == 2 {
			name = matches[1]
		}
	}
	if name == "" {
		matches := propertyPattern.FindStringSubmatch(output)
		if len(matches) == 2 {
			name = strings.Trim(matches[1], "]")
		}
	}
	if source := request.PropertyNames[name]; source != "" {
		return source
	}
	return name
}

func normalizeActions(request Request, output string) []verify.TraceStep {
	var result []verify.TraceStep
	for _, match := range actionPattern.FindAllStringSubmatch(output, -1) {
		bindings := verify.Bindings{}
		for _, binding := range bindingPattern.FindAllStringSubmatch(match[2], -1) {
			bindings[binding[1]] = binding[2]
		}
		result = append(result, verify.TraceStep{Action: match[1], Bindings: bindings})
	}
	if len(result) != 0 || request.Backend != TLC {
		if len(result) != 0 || request.Backend != Ivy {
			return result
		}
		return normalizeIvyActions(request, output)
	}
	for _, match := range tlaActionPattern.FindAllStringSubmatch(output, -1) {
		name := match[1]
		sourceName, known := request.ActionNames[name]
		if len(request.ActionNames) != 0 && !known {
			continue
		}
		if sourceName != "" {
			name = sourceName
		}
		bindings := verify.Bindings{}
		for _, binding := range tlaBindingPattern.FindAllStringSubmatch(match[2], -1) {
			bindings[binding[1]] = binding[2]
		}
		result = append(result, verify.TraceStep{Action: name, Bindings: bindings})
	}
	return result
}

func normalizeIvyActions(request Request, output string) []verify.TraceStep {
	var result []verify.TraceStep
	current := ""
	seen := map[string]struct{}{}
	for _, line := range strings.Split(output, "\n") {
		if match := ivyActionPattern.FindStringSubmatch(line); len(match) == 2 {
			current = match[1]
			continue
		}
		if current == "" || !ivyFailurePattern.MatchString(line) {
			continue
		}
		name := current
		if source := request.ActionNames[name]; source != "" {
			name = source
		}
		if _, duplicate := seen[name]; !duplicate {
			seen[name] = struct{}{}
			result = append(result, verify.TraceStep{Action: name})
		}
	}
	return result
}

func writeArtifacts(directory string, backend Backend, result verify.Result) ([]string, error) {
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return nil, err
	}
	prefix := string(backend)
	stdoutPath := filepath.Join(directory, prefix+".stdout.log")
	stderrPath := filepath.Join(directory, prefix+".stderr.log")
	resultPath := filepath.Join(directory, prefix+".result.json")
	paths := []string{stdoutPath, stderrPath, resultPath}
	result.Artifacts = append(result.Artifacts, paths...)
	if err := os.WriteFile(stdoutPath, []byte(result.StandardOutput), 0o600); err != nil {
		return nil, err
	}
	if err := os.WriteFile(stderrPath, []byte(result.StandardError), 0o600); err != nil {
		return nil, err
	}
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(resultPath, append(encoded, '\n'), 0o600); err != nil {
		return nil, err
	}
	return paths, nil
}

func nativeArtifacts(request Request) []string {
	var path string
	switch request.Backend {
	case TLC:
		path = filepath.Join(request.ArtifactDir, "tlc-native")
	case Apalache:
		path = filepath.Join(request.ArtifactDir, "apalache-native")
	case ApalacheProof:
		path = filepath.Join(request.ArtifactDir, "apalache-proof-native")
	case P:
		path = filepath.Join(request.ArtifactDir, "bugfinding-native")
	case PEx:
		path = filepath.Join(request.ArtifactDir, "pex-native")
	default:
		return nil
	}
	if path == "" {
		return nil
	}
	if _, err := os.Stat(path); err != nil {
		return nil
	}
	return []string{path}
}
