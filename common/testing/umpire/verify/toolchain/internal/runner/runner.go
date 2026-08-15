package runner

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
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
	Fizz          Backend = "fizz"

	maxNativeTraceBytes = 4 << 20
)

// EquivalenceEvidence records which backend results can be compared with the canonical interpreter.
type EquivalenceEvidence struct {
	StateCountComparable       bool
	SemanticMutationComparable bool
	Reason                     string
}

// BackendEquivalenceEvidence prevents a clean but incomparable run from being treated as equivalence.
func BackendEquivalenceEvidence(backend Backend) EquivalenceEvidence {
	switch backend {
	case TLC, Fizz:
		return EquivalenceEvidence{StateCountComparable: true, SemanticMutationComparable: true}
	case Apalache:
		return EquivalenceEvidence{SemanticMutationComparable: true, Reason: "the pinned symbolic checker does not expose a stable distinct-state count"}
	case P, PEx:
		return EquivalenceEvidence{SemanticMutationComparable: true, Reason: "schedule exploration does not expose a canonical distinct-state count"}
	case Ivy:
		return EquivalenceEvidence{SemanticMutationComparable: true, Reason: "inductiveness checking is not reachable-state enumeration"}
	default:
		return EquivalenceEvidence{Reason: "backend has no declared equivalence evidence"}
	}
}

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
	configureProcessCancellation(process)
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
