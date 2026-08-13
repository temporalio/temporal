package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"go.temporal.io/server/common/testing/umpire/verify"
	"go.temporal.io/server/common/testing/umpire/verify/ivy"
	pgenerator "go.temporal.io/server/common/testing/umpire/verify/p"
	"go.temporal.io/server/common/testing/umpire/verify/runner"
	"go.temporal.io/server/common/testing/umpire/verify/tla"
	"go.temporal.io/server/tests/umpire2/protocol"
)

const generatorVersion = "umpire-genmodels/v1"

var pinnedTools = []verify.ToolVersion{
	{Name: "apalache", Version: "0.61.0", SHA256: "f2d761315667f977c7c33792d95167f12e83b8a775909180886bcb67660470c5"},
	{Name: "ivy", Version: "1.8.26", Artifacts: []verify.ToolArtifact{
		{Platform: "darwin-universal2", SHA256: "d2f8df47e4731f2e23f7b5ab0852662e871217a9506c36310d75d81a9f09219c"},
		{Platform: "linux-x86_64", SHA256: "2a71da0bb2ce6314ddb40b6d76c6d734b8102db51c158477c2ef85b45da65dc1"},
	}},
	{Name: "p", Version: "3.1.0", SHA256: "b2a212e3b1af1bf2fdc9b80899da2901d6625d1a2e478d478e30028872a4bdc1"},
	{Name: "tla2tools", Version: "1.7.4", SHA256: "936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"},
}

func main() {
	var (
		mode         = flag.String("mode", "generate", "generate, check-generated, or verify")
		output       = flag.String("out", "tests/umpire2/genmodels", "generated model directory")
		artifacts    = flag.String("artifacts", "tests/umpire2/genmodels-results", "verification result directory")
		backend      = flag.String("backend", "all", "sany, tlc, apalache, apalache-proof, p, pex, ivy, or all")
		profile      = flag.String("profile", "smoke", "smoke or nightly")
		defaultBound = flag.Int("default-bound", 1, "finite identity pool size per entity type")
		timeout      = flag.Duration("timeout", 10*time.Minute, "per-tool verification timeout")
		tlaJar       = flag.String("tla-jar", os.Getenv("UMPIRE_TLA_JAR"), "pinned tla2tools.jar")
		javaTool     = flag.String("java-tool", os.Getenv("UMPIRE_JAVA_TOOL"), "Java executable")
		pTool        = flag.String("p-tool", os.Getenv("UMPIRE_P_TOOL"), "P executable")
		apalacheTool = flag.String("apalache-tool", os.Getenv("UMPIRE_APALACHE_TOOL"), "Apalache executable")
		ivyTool      = flag.String("ivy-tool", os.Getenv("UMPIRE_IVY_TOOL"), "ivy_check executable")
	)
	flag.Parse()

	var err error
	switch *mode {
	case "generate":
		err = generate(*output, *defaultBound)
	case "check-generated":
		err = checkGenerated(*output, *defaultBound)
	case "verify":
		err = checkModels(context.Background(), checkOptions{
			output: *output, artifacts: *artifacts, backend: *backend, profile: *profile,
			timeout: *timeout, tlaJar: *tlaJar, javaTool: *javaTool, pTool: *pTool,
			apalacheTool: *apalacheTool, ivyTool: *ivyTool, defaultBound: *defaultBound,
		})
	default:
		err = fmt.Errorf("unknown mode %q", *mode)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func generate(output string, defaultBound int) error {
	model, err := verificationModel(defaultBound)
	if err != nil {
		return err
	}
	tlaFiles, err := tla.Generate(model)
	if err != nil {
		return err
	}
	pFiles, err := pgenerator.Generate(model)
	if err != nil {
		return err
	}
	ivyFiles, ivyDiagnostics, err := ivy.Generate(model)
	if err != nil {
		return err
	}
	unsupported := make([]verify.Unsupported, len(ivyDiagnostics))
	for index, diagnostic := range ivyDiagnostics {
		unsupported[index] = verify.Unsupported{Backend: "ivy", Construct: diagnostic.Construct, Reason: diagnostic.Reason}
	}
	manifest, err := verify.NewManifest(model, verify.ManifestOptions{
		GeneratorVersion: generatorVersion,
		Guarantee:        verify.FiniteExhaustive,
		Tools:            pinnedTools,
		Unsupported:      unsupported,
	})
	if err != nil {
		return err
	}
	manifestJSON, err := verify.MarshalManifest(manifest)
	if err != nil {
		return err
	}
	modelJSON, err := verify.MarshalModel(model)
	if err != nil {
		return err
	}
	files := map[string][]byte{
		"manifest.json": manifestJSON,
		"model.ir.json": modelJSON,
	}
	mergeFiles(files, "tla", tlaFiles)
	mergeFiles(files, "p", pFiles)
	mergeFiles(files, "ivy", ivyFiles)
	return writeFiles(output, files)
}

func verificationModel(defaultBound int) (verify.Model, error) {
	compiled, err := protocol.Default()
	if err != nil {
		return verify.Model{}, fmt.Errorf("compile default Umpire protocol: %w", err)
	}
	return compiled.VerificationModel(protocol.VerificationOptions{DefaultBound: defaultBound})
}

func mergeFiles(destination map[string][]byte, directory string, files map[string][]byte) {
	for name, contents := range files {
		destination[filepath.Join(directory, name)] = contents
	}
}

func writeFiles(root string, files map[string][]byte) error {
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(path, files[name], 0o644); err != nil {
			return err
		}
	}
	return nil
}

func checkGenerated(output string, defaultBound int) (retErr error) {
	temporary, err := os.MkdirTemp("", "umpire-genmodels-check-")
	if err != nil {
		return err
	}
	defer func() {
		retErr = errors.Join(retErr, os.RemoveAll(temporary))
	}()
	if err := generate(temporary, defaultBound); err != nil {
		return err
	}
	var differences []string
	expected := map[string]struct{}{}
	err = filepath.WalkDir(temporary, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() {
			return walkErr
		}
		relative, err := filepath.Rel(temporary, path)
		if err != nil {
			return err
		}
		expected[relative] = struct{}{}
		generated, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		checkedIn, err := os.ReadFile(filepath.Join(output, relative))
		if err != nil || !slices.Equal(generated, checkedIn) {
			differences = append(differences, "changed "+relative)
		}
		return nil
	})
	if err != nil {
		return err
	}
	err = filepath.WalkDir(output, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() {
			return walkErr
		}
		relative, err := filepath.Rel(output, path)
		if err != nil {
			return err
		}
		if _, ok := expected[relative]; !ok {
			differences = append(differences, "unexpected "+relative)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(differences) != 0 {
		slices.Sort(differences)
		return fmt.Errorf("generated Umpire models differ: %s", strings.Join(differences, ", "))
	}
	return nil
}

type checkOptions struct {
	output       string
	artifacts    string
	backend      string
	profile      string
	timeout      time.Duration
	tlaJar       string
	javaTool     string
	pTool        string
	apalacheTool string
	ivyTool      string
	defaultBound int
}

func checkModels(ctx context.Context, options checkOptions) error {
	backends, err := requestedBackends(options.backend, options.profile)
	if err != nil {
		return err
	}
	bounds, err := profileBounds(options.profile)
	if err != nil {
		return err
	}
	model, err := verificationModel(options.defaultBound)
	if err != nil {
		return err
	}
	bounds.Identities = make(map[string]int, len(model.Entities))
	for _, entity := range model.Entities {
		bounds.Identities[entity.Name] = len(entity.IDs)
	}
	for _, backend := range backends {
		request, err := runnerRequest(backend, options, bounds, model)
		if err != nil {
			return err
		}
		result, err := runner.Check(ctx, request)
		if err != nil {
			return err
		}
		fmt.Printf("%s: %s (%s)\n", backend, result.Status, result.Termination)
		if result.Status == verify.Counterexample || result.Status == verify.Inconclusive || result.Status == verify.UnsupportedStatus {
			return fmt.Errorf("%s verification returned %s: %s", backend, result.Status, result.Diagnostic)
		}
	}
	return nil
}

func requestedBackends(value, profile string) ([]runner.Backend, error) {
	if value == "all" {
		result := []runner.Backend{runner.SANY, runner.TLC, runner.Apalache, runner.P, runner.PEx, runner.Ivy}
		if profile == "nightly" {
			result = append(result, runner.ApalacheProof)
		}
		return result, nil
	}
	var result []runner.Backend
	for _, name := range strings.Split(value, ",") {
		backend := runner.Backend(strings.TrimSpace(name))
		switch backend {
		case runner.SANY, runner.TLC, runner.Apalache, runner.ApalacheProof, runner.P, runner.PEx, runner.Ivy:
			result = append(result, backend)
		default:
			return nil, fmt.Errorf("unknown verification backend %q", name)
		}
	}
	return result, nil
}

func profileBounds(profile string) (verify.Bounds, error) {
	switch profile {
	case "smoke":
		return verify.Bounds{MaxDepth: 100, Schedules: 100}, nil
	case "nightly":
		return verify.Bounds{MaxDepth: 1_000, Schedules: 10_000}, nil
	default:
		return verify.Bounds{}, fmt.Errorf("unknown verification profile %q", profile)
	}
}

func runnerRequest(
	backend runner.Backend,
	options checkOptions,
	bounds verify.Bounds,
	model verify.Model,
) (runner.Request, error) {
	actionNames := make(map[string]string, len(model.Actions))
	propertyNames := make(map[string]string, len(model.Properties))
	fairnessSet := map[string]struct{}{}
	var unsupported []verify.Unsupported
	for _, action := range model.Actions {
		actionNames[tla.ActionIdentifier(action.Name)] = action.Name
		actionNames[ivy.ActionIdentifier(action.Name)] = action.Name
	}
	for _, property := range model.Properties {
		propertyNames[tla.PropertyIdentifier(property.Name)] = property.Name
		propertyNames[ivy.PropertyIdentifier(property.Name)] = property.Name
		for _, assumption := range property.Fairness {
			fairnessSet[assumption] = struct{}{}
		}
		if property.Kind != verify.SafetyProperty {
			unsupported = append(unsupported, verify.Unsupported{
				Backend: "ivy", Construct: "property " + property.Name,
				Reason: "Ivy generation supports inductive safety properties only", Source: property.Source,
			})
		}
	}
	var fairness []string
	for assumption := range fairnessSet {
		fairness = append(fairness, assumption)
	}
	slices.Sort(fairness)
	request := runner.Request{
		Backend: backend, ArtifactDir: options.artifacts, Timeout: options.timeout, Bounds: bounds,
		JavaPath: options.javaTool, ActionNames: actionNames, PropertyNames: propertyNames,
		Fairness: fairness, Abstractions: model.Abstractions, Unsupported: unsupported,
	}
	switch backend {
	case runner.SANY, runner.TLC:
		request.ModelDir = filepath.Join(options.output, "tla")
		request.ToolPath = options.tlaJar
		request.ToolVersion = "1.7.4"
		if request.ToolPath == "" {
			return request, errors.New("TLA+ verification requires -tla-jar or UMPIRE_TLA_JAR")
		}
	case runner.Apalache, runner.ApalacheProof:
		request.ModelDir = filepath.Join(options.output, "tla")
		request.ToolPath = options.apalacheTool
		request.ToolVersion = "0.61.0"
		if backend == runner.Apalache {
			if options.profile == "nightly" {
				request.Bounds.MaxDepth = 20
			} else {
				request.Bounds.MaxDepth = 5
			}
		}
		if request.ToolPath == "" {
			return request, errors.New("apalache verification requires -apalache-tool or UMPIRE_APALACHE_TOOL")
		}
	case runner.P, runner.PEx:
		request.ModelDir = filepath.Join(options.output, "p")
		request.ToolPath = options.pTool
		request.ToolVersion = "3.1.0"
		if backend == runner.PEx && request.Bounds.MaxDepth > 100 {
			request.Bounds.MaxDepth = 100
		}
		if request.ToolPath == "" {
			return request, errors.New("p verification requires -p-tool or UMPIRE_P_TOOL")
		}
	case runner.Ivy:
		request.ModelDir = filepath.Join(options.output, "ivy")
		request.ToolPath = options.ivyTool
		request.ToolVersion = "1.8.26"
		if request.ToolPath == "" {
			return request, errors.New("ivy verification requires -ivy-tool or UMPIRE_IVY_TOOL")
		}
	default:
		return request, fmt.Errorf("unknown verification backend %q", backend)
	}
	if options.profile == "nightly" {
		request.Config = "Umpire-nightly.cfg"
	}
	return request, nil
}
