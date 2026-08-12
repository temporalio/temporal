package regress

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
)

// PathArtifact is the incrementally flushed diagnostic state of one path execution.
type PathArtifact struct {
	Index          int               `json:"index"`
	ActionsBegun   []string          `json:"actionsBegun,omitempty"`
	Bindings       Bindings          `json:"bindings,omitempty"`
	ActivePolicies []string          `json:"activePolicies,omitempty"`
	Observations   []string          `json:"observations,omitempty"`
	Verdicts       []string          `json:"verdicts,omitempty"`
	Facts          []json.RawMessage `json:"facts,omitempty"`
	Complete       bool              `json:"complete"`
	Error          string            `json:"error,omitempty"`
}

// Artifact records sparse intent, selected completed paths, and incremental execution evidence.
type Artifact struct {
	Name         string         `json:"name,omitempty"`
	ModelVersion string         `json:"modelVersion"`
	Profile      Profile        `json:"profile"`
	IR           IR             `json:"ir"`
	Completed    Suite          `json:"completed"`
	Paths        []PathArtifact `json:"paths"`
	Complete     bool           `json:"complete"`
}

// ArtifactSink durably records successive snapshots. Implementations should flush each write.
type ArtifactSink interface {
	WriteArtifact(context.Context, Artifact) error
}

// JSONFileSink atomically replaces one durable JSON artifact after every execution event.
type JSONFileSink struct {
	path string
	mu   sync.Mutex
}

func NewJSONFileSink(path string) (*JSONFileSink, error) {
	if path == "" {
		return nil, errors.New("artifact path is empty")
	}
	return &JSONFileSink{path: filepath.Clean(path)}, nil
}

func (s *JSONFileSink) WriteArtifact(_ context.Context, artifact Artifact) (resultErr error) {
	if s == nil || s.path == "" {
		return errors.New("artifact sink is not configured")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	directory := filepath.Dir(s.path)
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return fmt.Errorf("create artifact directory: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".umpire-regress-*.json")
	if err != nil {
		return fmt.Errorf("create temporary artifact: %w", err)
	}
	temporaryPath := temporary.Name()
	temporaryOpen := true
	removeTemporary := true
	defer func() {
		if temporaryOpen {
			if err := temporary.Close(); err != nil {
				resultErr = errors.Join(resultErr, fmt.Errorf("close temporary artifact: %w", err))
			}
		}
		if removeTemporary {
			if err := os.Remove(temporaryPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				resultErr = errors.Join(resultErr, fmt.Errorf("remove temporary artifact: %w", err))
			}
		}
	}()
	encoder := json.NewEncoder(temporary)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(artifact); err != nil {
		return fmt.Errorf("encode artifact: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("flush artifact: %w", err)
	}
	if err := temporary.Close(); err != nil {
		temporaryOpen = false
		return fmt.Errorf("close artifact: %w", err)
	}
	temporaryOpen = false
	if err := os.Rename(temporaryPath, s.path); err != nil {
		return fmt.Errorf("replace artifact: %w", err)
	}
	removeTemporary = false
	return nil
}

// ArtifactHarness optionally exposes an incremental artifact sink to Run.
type ArtifactHarness interface {
	ArtifactSink() ArtifactSink
}

// ArtifactFactProvider exposes the facts observed by one live path for incremental diagnostics.
type ArtifactFactProvider interface {
	ArtifactFacts(context.Context) ([]json.RawMessage, error)
}

var ErrReplayMismatch = errors.New("completed regression replay does not match model or profile")

// Replay executes a completed artifact only against the same model and environment profile.
func Replay(ctx context.Context, artifact Artifact, domain *Domain, profile Profile, harness Harness) error {
	if domain == nil || artifact.ModelVersion != domain.Version() || !sameProfile(artifact.Profile, profile) {
		return fmt.Errorf("%w: artifact model=%q profile=%q, current model=%q profile=%q", ErrReplayMismatch, artifact.ModelVersion, artifact.Profile.Name, domainVersion(domain), profile.Name)
	}
	if artifact.Completed.ModelVersion != artifact.ModelVersion || !sameProfile(artifact.Completed.Profile, artifact.Profile) {
		return fmt.Errorf("%w: completed suite identity differs from artifact", ErrReplayMismatch)
	}
	return Run(ctx, artifact.Completed, harness)
}

func domainVersion(domain *Domain) string {
	if domain == nil {
		return ""
	}
	return domain.Version()
}

func sameProfile(left, right Profile) bool {
	left.Capabilities = slices.Clone(left.Capabilities)
	right.Capabilities = slices.Clone(right.Capabilities)
	slices.Sort(left.Capabilities)
	slices.Sort(right.Capabilities)
	return reflect.DeepEqual(left, right)
}

type artifactRecorder struct {
	sink     ArtifactSink
	artifact Artifact
	mu       sync.Mutex
}

func newArtifactRecorder(suite Suite, sink ArtifactSink) *artifactRecorder {
	paths := make([]PathArtifact, len(suite.Paths))
	for index := range paths {
		paths[index] = PathArtifact{Index: index, Bindings: Bindings{}}
	}
	return &artifactRecorder{
		sink: sink,
		artifact: Artifact{
			Name:         suite.Name,
			ModelVersion: suite.ModelVersion,
			Profile:      suite.Profile,
			IR:           suite.IR,
			Completed:    suite,
			Paths:        paths,
		},
	}
}

func (r *artifactRecorder) flush(ctx context.Context) error {
	if r == nil || r.sink == nil {
		return nil
	}
	return r.sink.WriteArtifact(context.WithoutCancel(ctx), r.snapshot())
}

func (r *artifactRecorder) actionBegun(ctx context.Context, path int, action string, bindings Bindings) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.artifact.Paths[path].ActionsBegun = append(r.artifact.Paths[path].ActionsBegun, action)
	r.artifact.Paths[path].Bindings = cloneRuntimeBindings(bindings)
	return r.flush(ctx)
}

func (r *artifactRecorder) policy(ctx context.Context, path int, policy string, active bool, bindings Bindings) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.artifact.Paths[path].ActivePolicies
	if active {
		if !containsString(current, policy) {
			current = append(current, policy)
		}
	} else {
		for index, name := range current {
			if name == policy {
				current = append(current[:index], current[index+1:]...)
				break
			}
		}
	}
	r.artifact.Paths[path].ActivePolicies = current
	r.artifact.Paths[path].Bindings = cloneRuntimeBindings(bindings)
	return r.flush(ctx)
}

func (r *artifactRecorder) observation(ctx context.Context, path int, name string, bindings Bindings) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.artifact.Paths[path].Observations = append(r.artifact.Paths[path].Observations, name)
	r.artifact.Paths[path].Bindings = cloneRuntimeBindings(bindings)
	return r.flush(ctx)
}

func (r *artifactRecorder) verdict(ctx context.Context, path int, checkpoint Checkpoint, bindings Bindings) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.artifact.Paths[path].Verdicts = append(r.artifact.Paths[path].Verdicts, checkpoint.String()+":pass")
	r.artifact.Paths[path].Bindings = cloneRuntimeBindings(bindings)
	return r.flush(ctx)
}

func (r *artifactRecorder) facts(ctx context.Context, path int, provider ArtifactFactProvider) error {
	if r == nil || provider == nil {
		return nil
	}
	facts, err := provider.ArtifactFacts(ctx)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.artifact.Paths[path].Facts = slices.Clone(facts)
	return r.flush(ctx)
}

func (r *artifactRecorder) finish(ctx context.Context, path int, bindings Bindings, err error) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	pathArtifact := &r.artifact.Paths[path]
	pathArtifact.Bindings = cloneRuntimeBindings(bindings)
	pathArtifact.ActivePolicies = nil
	pathArtifact.Complete = err == nil
	if err != nil {
		pathArtifact.Error = err.Error()
	}
	r.artifact.Completed.Paths[path].Bindings = cloneRuntimeBindings(bindings)
	return r.flush(ctx)
}

func (r *artifactRecorder) complete(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.artifact.Complete = true
	return r.flush(ctx)
}

func (r *artifactRecorder) snapshot() Artifact {
	result := r.artifact
	result.Paths = make([]PathArtifact, len(r.artifact.Paths))
	for index, path := range r.artifact.Paths {
		result.Paths[index] = path
		result.Paths[index].ActionsBegun = slices.Clone(path.ActionsBegun)
		result.Paths[index].Bindings = cloneRuntimeBindings(path.Bindings)
		result.Paths[index].ActivePolicies = slices.Clone(path.ActivePolicies)
		result.Paths[index].Observations = slices.Clone(path.Observations)
		result.Paths[index].Verdicts = slices.Clone(path.Verdicts)
		result.Paths[index].Facts = slices.Clone(path.Facts)
	}
	result.Completed.Paths = slices.Clone(r.artifact.Completed.Paths)
	for index := range result.Completed.Paths {
		result.Completed.Paths[index].Bindings = cloneRuntimeBindings(r.artifact.Completed.Paths[index].Bindings)
	}
	return result
}

func cloneRuntimeBindings(source Bindings) Bindings {
	result := make(Bindings, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
