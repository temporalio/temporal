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

	maxNativeTraceBytes = 4 << 20
)

type Request struct {
	Backend         Backend
	Model           verify.Model
	TraceVocabulary verify.TraceVocabulary
	Target          string
	Profile         string
	ToolPath        string
	JavaPath        string
	ToolVersion     string
	ModelDir        string
	ArtifactDir     string
	Config          string
	Timeout         time.Duration
	Bounds          verify.Bounds
	ActionNames     map[string]string
	PropertyNames   map[string]string
	Fairness        []string
	Abstractions    []verify.Abstraction
	Unsupported     []verify.Unsupported
}

type command struct {
	path string
	args []string
	dir  string
	env  []string
}

type execution struct {
	output         string
	stdout         string
	stderr         string
	nativeTrace    string
	nativeTraceErr error
	err            error
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
	var paths []string
	err := filepath.WalkDir(directory, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && entry.Name() == "example.itf.json" {
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	if len(paths) == 0 {
		return "", nil
	}
	if len(paths) != 1 {
		return "", fmt.Errorf("expected at most one Apalache example.itf.json, found %d", len(paths))
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
	stepLimitPattern         = regexp.MustCompile(`(?i)(?:max(?:imum)? scheduling steps|maximum number of steps|scheduling steps bound of [\d,]+ reached)`)
	scheduleLimit            = regexp.MustCompile(`(?im)^Finished [\d,]+ search tasks \([1-9][\d,]* pending\)\s*$`)
	ivySuccessPattern        = regexp.MustCompile(`(?m)^OK\s*$`)
	ivyActionPattern         = regexp.MustCompile(`^\s*\(internal\)\s+([A-Za-z0-9_]+)\s*$`)
	ivyFailurePattern        = regexp.MustCompile(`line \d+:\s+([A-Za-z0-9_]+)\s+\.\.\.\s+FAIL`)
	apalacheInvariantFailure = regexp.MustCompile(`(?i)state\s+\d+:\s+(?:state|action|trace)\s+invariant\s+\d+\s+\[([A-Za-z0-9_.-]+)\]\s+violated`)
)

func classify(request Request, execution execution) verify.Result {
	output := execution.output
	unsupported := unsupportedForBackend(request.Backend, request.Unsupported)
	result := verify.Result{
		Backend:      string(request.Backend),
		Target:       request.Target,
		Profile:      request.Profile,
		ToolVersion:  request.ToolVersion,
		Termination:  verify.Completed,
		Fairness:     slices.Clone(request.Fairness),
		Abstractions: slices.Clone(request.Abstractions),
		Unsupported:  unsupported,
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
	limit := reportedLimit(request.Backend, output)
	switch {
	case limit != "":
		result.Status = verify.Inconclusive
		result.Termination = limit
		if limit == verify.ToolLimit && strings.Contains(lower, "toomanychoicesexception") {
			result.Diagnostic = "PEx reached its native per-statement choice limit"
		}
	case request.Backend == PEx && strings.Contains(lower, "cycle detected: infinite loop"):
		result.Status = verify.Inconclusive
		result.Termination = verify.ToolError
		result.Diagnostic = "PEx reported an implicit cycle outside the generated Umpire properties"
	case isCounterexample(lower):
		result.Status = verify.Counterexample
		result.NativeTrace = output
		semanticBackend := request.Backend == P || request.Backend == PEx || request.Backend == TLC || request.Backend == Apalache || request.Backend == ApalacheProof || request.Backend == Ivy
		if semanticBackend && request.Model.Version == "" {
			result.Status = verify.Inconclusive
			result.Termination = verify.EvidenceFailure
			result.Diagnostic = "native-trace-malformed: canonical model is unavailable"
			break
		}
		nativeProperty := nativeFailedProperty(request, output)
		properties := failedPropertyCandidates(request, nativeProperty)
		if len(properties) == 0 {
			result.Status = verify.Inconclusive
			result.Termination = verify.EvidenceFailure
			result.Diagnostic = "property-unmapped: counterexample has no recognized failed property"
			break
		}
		if semanticBackend {
			var evidence verify.TraceEvidence
			var evidenceErr error
			switch request.Backend {
			case P, PEx:
				if execution.nativeTraceErr != nil {
					evidenceErr = execution.nativeTraceErr
					break
				}
				if len(output) > maxNativeTraceBytes {
					evidenceErr = errors.New("native-trace-too-large: P counterexample trace exceeds 4 MiB")
					break
				}
				parsed := normalizeActions(request, output)
				if len(parsed) == 0 {
					evidenceErr = errors.New("native-trace-missing: P counterexample has no Umpire action records")
					break
				}
				evidence.Steps = make([]verify.ObservedTraceStep, len(parsed))
				for index, step := range parsed {
					evidence.Steps[index] = verify.ObservedTraceStep{Action: step.Action, Bindings: step.Bindings}
				}
			case TLC:
				if len(output) > maxNativeTraceBytes {
					evidenceErr = errors.New("native-trace-too-large: TLC textual trace exceeds 4 MiB")
					break
				}
				evidence, evidenceErr = decodeTLCTrace(request, output)
			case Apalache, ApalacheProof:
				result.NativeTrace = execution.nativeTrace
				if execution.nativeTraceErr != nil {
					evidenceErr = execution.nativeTraceErr
					break
				}
				if execution.nativeTrace == "" {
					evidenceErr = errors.New("native-trace-missing: Apalache counterexample has no ITF trace")
					break
				}
				if len(execution.nativeTrace) > maxNativeTraceBytes {
					evidenceErr = errors.New("native-trace-too-large: Apalache ITF trace exceeds 4 MiB")
					break
				}
				evidence, evidenceErr = decodeITFTrace(request, execution.nativeTrace)
			case Ivy:
				if len(output) > maxNativeTraceBytes {
					evidenceErr = errors.New("native-trace-too-large: Ivy textual trace exceeds 4 MiB")
					break
				}
				evidence, evidenceErr = decodeIvyTrace(request, output)
			default:
			}
			if evidenceErr != nil {
				result.Status = verify.Inconclusive
				result.Termination = verify.EvidenceFailure
				result.Diagnostic = evidenceErr.Error()
				break
			}
			property, normalized, normalizationErr := normalizeEvidence(request.Model, properties, evidence)
			if normalizationErr != nil {
				result.Status = verify.Inconclusive
				result.Termination = verify.EvidenceFailure
				result.Diagnostic = normalizationErr.Error()
				break
			}
			result.FailedProperty = property
			result.Trace = normalized
		} else {
			result.FailedProperty = properties[0]
			result.Trace = normalizeActions(request, output)
		}
	case execution.err != nil:
		result.Status = verify.Inconclusive
		result.Termination = verify.ToolError
		result.Diagnostic = execution.err.Error()
	case len(unsupported) > 0:
		result.Status = verify.UnsupportedStatus
		result.Diagnostic = fmt.Sprintf("%s: %s", unsupported[0].Construct, unsupported[0].Reason)
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

func reportedLimit(backend Backend, output string) verify.TerminationReason {
	lower := strings.ToLower(output)
	switch {
	case timeoutPattern.MatchString(output) || (backend == Apalache || backend == ApalacheProof) && apalacheTimeout.MatchString(output):
		return verify.Timeout
	case strings.Contains(lower, "memory limit") || strings.Contains(lower, "out of memory"):
		return verify.MemoryLimit
	case backend == PEx && depthLimitPattern.MatchString(output):
		return verify.DepthLimit
	case stateLimitPattern.MatchString(output):
		return verify.StateLimit
	case stepLimitPattern.MatchString(output):
		return verify.StepLimit
	case backend == PEx && scheduleLimit.MatchString(output):
		return verify.ScheduleLimit
	case backend == PEx && strings.Contains(lower, "toomanychoicesexception"):
		return verify.ToolLimit
	default:
		return ""
	}
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
		apalacheInvariantFailure.MatchString(lower) ||
		invariantViolation.MatchString(lower)
}

func nativeFailedProperty(request Request, output string) string {
	name := ""
	if request.Backend == Apalache || request.Backend == ApalacheProof {
		if matches := apalacheInvariantFailure.FindStringSubmatch(output); len(matches) == 2 {
			name = matches[1]
		}
	}
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
	return name
}

func failedPropertyCandidates(request Request, name string) []string {
	if name == "" {
		return nil
	}
	if properties := request.TraceVocabulary.Properties[name]; len(properties) != 0 {
		return slices.Clone(properties)
	}
	if source := request.PropertyNames[name]; source != "" {
		return []string{source}
	}
	for _, property := range request.Model.Properties {
		if property.Name == name {
			return []string{name}
		}
	}
	for _, relation := range request.Model.Relations {
		properties := []string{"relation " + relation.Name + " endpoints"}
		if relation.SourceCardinality == verify.One {
			properties = append(properties, "relation "+relation.Name+" source cardinality")
		}
		if relation.TargetCardinality == verify.One {
			properties = append(properties, "relation "+relation.Name+" target cardinality")
		}
		for _, property := range properties {
			if property == name {
				return []string{name}
			}
		}
	}
	return nil
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
