package runner

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"go.temporal.io/server/common/testing/umpire/verify"
)

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
		args := []string{"-cp", request.ToolPath, "tlc2.TLC", "-workers", "auto", "-config", config}
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
		nativeOutput := ""
		temporaryOutput := false
		if request.ArtifactDir != "" {
			nativeOutput = filepath.Join(request.ArtifactDir, "apalache-native")
			if err := os.RemoveAll(nativeOutput); err != nil {
				return execution{}, nil, err
			}
			if err := os.MkdirAll(nativeOutput, 0o755); err != nil {
				return execution{}, nil, err
			}
		} else {
			var err error
			nativeOutput, err = os.MkdirTemp("", "umpire-apalache-run-")
			if err != nil {
				return execution{}, nil, err
			}
			temporaryOutput = true
		}
		args := []string{"--out-dir=" + nativeOutput}
		args = append(args, "check", "--config="+config, "--inv=Safety,QuiescentSafety", "--no-deadlock", "--output-traces=true")
		if request.Bounds.MaxDepth != 0 {
			args = append(args, "--length="+strconv.FormatUint(request.Bounds.MaxDepth, 10))
		}
		args = append(args, "Umpire.tla")
		environment := javaEnvironment(request.JavaPath)
		actual := executor.run(ctx, command{path: request.ToolPath, args: args, dir: request.ModelDir, env: environment})
		evidence, evidenceErr := collectApalacheTraceEvidence(nativeOutput)
		if temporaryOutput {
			if cleanupErr := os.RemoveAll(nativeOutput); cleanupErr != nil {
				return execution{}, nil, errors.Join(evidenceErr, cleanupErr)
			}
		}
		if errors.Is(evidenceErr, errNativeTraceTooLarge) {
			actual.nativeTraceErr = fmt.Errorf("native-trace-too-large: %w", evidenceErr)
		} else if evidenceErr != nil {
			return execution{}, nil, evidenceErr
		} else {
			actual.nativeTrace = evidence
		}
		return actual, [][]string{replayCommand(environment, request.ToolPath, args)}, nil
	case ApalacheProof:
		return executeApalacheProof(ctx, executor, request)
	case P, PEx:
		return executeP(ctx, executor, request)
	case Ivy:
		args := []string{"trace=true", filepath.Join(request.ModelDir, "Umpire.ivy")}
		return executor.run(ctx, command{path: request.ToolPath, args: args, dir: request.ModelDir}), [][]string{append([]string{request.ToolPath}, args...)}, nil
	case Fizz:
		return executeFizz(ctx, executor, request)
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
		nativeOutput := ""
		temporaryOutput := false
		if request.ArtifactDir != "" {
			nativeOutput = filepath.Join(request.ArtifactDir, "apalache-proof-native", proof.name)
			if err := os.RemoveAll(nativeOutput); err != nil {
				return execution{}, nil, err
			}
			if err := os.MkdirAll(nativeOutput, 0o755); err != nil {
				return execution{}, nil, err
			}
		} else {
			var err error
			nativeOutput, err = os.MkdirTemp("", "umpire-apalache-proof-"+proof.name+"-")
			if err != nil {
				return execution{}, nil, err
			}
			temporaryOutput = true
		}
		args := []string{"--out-dir=" + nativeOutput}
		args = append(args, "check", "--config="+config)
		args = append(args, proof.args...)
		args = append(args, "--no-deadlock", "--output-traces=true", "Umpire.tla")
		marker := "UMPIRE_PROOF_OBLIGATION " + proof.name + "\n"
		actual := executor.run(ctx, command{path: request.ToolPath, args: args, dir: request.ModelDir, env: environment})
		var collectionErr error
		if actual.err != nil {
			evidence, err := collectApalacheTraceEvidence(nativeOutput)
			if errors.Is(err, errNativeTraceTooLarge) {
				actual.nativeTraceErr = fmt.Errorf("native-trace-too-large: %w", err)
			} else if err != nil {
				collectionErr = err
			} else {
				actual.nativeTrace = evidence
			}
		}
		if temporaryOutput {
			collectionErr = errors.Join(collectionErr, os.RemoveAll(nativeOutput))
		}
		if collectionErr != nil {
			return execution{}, nil, collectionErr
		}
		combined.output += marker + actual.output
		combined.stdout += marker + actual.stdout
		combined.stderr += actual.stderr
		replay = append(replay, replayCommand(environment, request.ToolPath, args))
		if actual.err != nil {
			combined.err = actual.err
			combined.nativeTrace = actual.nativeTrace
			combined.nativeTraceErr = actual.nativeTraceErr
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
	if errors.Is(evidenceErr, errNativeTraceTooLarge) {
		checked.nativeTraceErr = fmt.Errorf("native-trace-too-large: %w", evidenceErr)
	} else if evidenceErr != nil {
		return execution{}, nil, evidenceErr
	} else if evidence != "" {
		checked.nativeTrace = evidence
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
		header := fmt.Sprintf("--- %s ---\n", filepath.Base(path))
		if info.Size() > maxNativeTraceBytes || int64(result.Len()+len(header)+1)+info.Size() > maxNativeTraceBytes {
			return "", fmt.Errorf("%w: P checker trace exceeds 4 MiB", errNativeTraceTooLarge)
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		result.WriteString(header)
		result.Write(contents)
		result.WriteByte('\n')
	}
	return result.String(), nil
}

var errNativeTraceTooLarge = errors.New("native trace is too large")

func collectApalacheTraceEvidence(directory string) (string, error) {
	var examplePaths []string
	var violationPaths []string
	var numberedViolationPaths []string
	err := filepath.WalkDir(directory, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if entry.Name() == "example.itf.json" {
			examplePaths = append(examplePaths, path)
		}
		if entry.Name() == "violation.itf.json" {
			violationPaths = append(violationPaths, path)
		}
		if apalacheTracePattern.MatchString(entry.Name()) {
			numberedViolationPaths = append(numberedViolationPaths, path)
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	paths := numberedViolationPaths
	if len(paths) == 0 {
		paths = violationPaths
	}
	if len(paths) == 0 {
		paths = examplePaths
	}
	if len(paths) == 0 {
		return "", nil
	}
	if len(paths) != 1 {
		return "", fmt.Errorf("expected at most one Apalache ITF trace, found %d: %v", len(paths), paths)
	}
	info, err := os.Stat(paths[0])
	if err != nil {
		return "", err
	}
	if info.Size() > maxNativeTraceBytes {
		return "", fmt.Errorf("%w: Apalache ITF trace exceeds 4 MiB", errNativeTraceTooLarge)
	}
	contents, err := os.ReadFile(paths[0])
	if err != nil {
		return "", err
	}
	return string(contents), nil
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
	stateCountsPattern       = regexp.MustCompile(`(?m)(\d+) states generated, (\d+) distinct states found`)
	propertyPattern          = regexp.MustCompile(`(?i)(?:property|invariant)[ :\[]+([A-Za-z0-9_.-]+)`)
	pPropertyPattern         = regexp.MustCompile(`(?i)property\s+([A-Za-z0-9_.-]+)\s+failed`)
	actionPattern            = regexp.MustCompile(`(?m)UMPIRE_ACTION\s+(\S+)([^\r\n]*)`)
	bindingPattern           = regexp.MustCompile(`(\S+)=([^ ]+)`)
	tlaActionPattern         = regexp.MustCompile(`(?m)^State \d+: <([A-Za-z0-9_]+)(?:\(([^)]*)\))?`)
	tlaBindingPattern        = regexp.MustCompile(`([A-Za-z0-9_]+)\s*=\s*"([^"]*)"`)
	invariantViolation       = regexp.MustCompile(`(?i)invariant\s+[A-Za-z0-9_.-]+\s+is violated`)
	timeoutPattern           = regexp.MustCompile(`(?i)(timed out|timeout (?:after|reached|exceeded)|time limit)`)
	apalacheTimeout          = regexp.MustCompile(`(?i)(?:=>|reports?)\s*timeout\b`)
	depthLimitPattern        = regexp.MustCompile(`(?im)^Result:\s+(?:partially\s+)?correct up to step\s+[\d,]+\b`)
	stateLimitPattern        = regexp.MustCompile(`(?i)(?:state limit (?:reached|exceeded)|maximum (?:number of )?states (?:reached|exceeded))`)
	stepLimitPattern         = regexp.MustCompile(`(?i)(?:max(?:imum)? scheduling steps|maximum number of steps|scheduling steps bound of [\d,]+ reached|exceeded the max-steps bound)`)
	scheduleLimit            = regexp.MustCompile(`(?im)^Finished [\d,]+ search tasks \([1-9][\d,]* pending\)\s*$`)
	ivySuccessPattern        = regexp.MustCompile(`(?m)^OK\s*$`)
	ivyActionPattern         = regexp.MustCompile(`^\s*\(internal\)\s+([A-Za-z0-9_]+)\s*$`)
	ivyFailurePattern        = regexp.MustCompile(`line \d+:\s+([A-Za-z0-9_]+)\s+\.\.\.\s+FAIL`)
	apalacheInvariantFailure = regexp.MustCompile(`(?i)state\s+\d+:\s+(?:state|action|trace)\s+invariant\s+\d+\s+\[([A-Za-z0-9_.-]+)\]\s+violated`)
	apalacheNumberedFailure  = regexp.MustCompile(`(?i)state\s+\d+:\s+(?:state|action|trace)\s+invariant\s+\d+\s+violated`)
	apalacheTracePattern     = regexp.MustCompile(`^violation\d+\.itf\.json$`)
	pRelationPropertyPattern = regexp.MustCompile(`(?i)relation\s+([A-Za-z0-9_.-]+)\s+(has an absent endpoint|exceeds source cardinality|exceeds target cardinality)`)
	fizzFailurePattern       = regexp.MustCompile(`(?m)^FAILED: Model checker failed\. (?:Transition )?Invariant:\s+([A-Za-z0-9_.-]+)\s*$`)
	fizzStateCountsPattern   = regexp.MustCompile(`(?m)Valid Nodes:\s+(\d+)\s+Unique states:\s+(\d+)`)
)
