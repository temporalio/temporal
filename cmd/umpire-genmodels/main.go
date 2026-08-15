package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"go.temporal.io/server/common/testing/umpire/verify"
	"go.temporal.io/server/common/testing/umpire/verify/fizz"
	"go.temporal.io/server/common/testing/umpire/verify/ivy"
	pgenerator "go.temporal.io/server/common/testing/umpire/verify/p"
	"go.temporal.io/server/common/testing/umpire/verify/runner"
	"go.temporal.io/server/common/testing/umpire/verify/tla"
	"go.temporal.io/server/tests/umpire2/assurance"
	"go.temporal.io/server/tests/umpire2/protocol"
)

const generatorVersion = "umpire-genmodels/v1"

func main() {
	var (
		mode         = flag.String("mode", "generate", "generate, check-generated, or verify")
		output       = flag.String("out", "tests/umpire2/genmodels", "generated model directory")
		artifacts    = flag.String("artifacts", "tests/umpire2/genmodels-results", "verification result directory")
		target       = flag.String("target", "all", "verification target name or all")
		backend      = flag.String("backend", "all", "sany, tlc, apalache, apalache-proof, p, pex, ivy, fizz, or all")
		profile      = flag.String("profile", "smoke", "smoke or nightly")
		defaultBound = flag.Int("default-bound", 1, "finite identity pool size per entity type")
		timeout      = flag.Duration("timeout", 10*time.Minute, "per-tool verification timeout")
		tlaJar       = flag.String("tla-jar", os.Getenv("UMPIRE_TLA_JAR"), "pinned tla2tools.jar")
		javaTool     = flag.String("java-tool", os.Getenv("UMPIRE_JAVA_TOOL"), "Java executable")
		pTool        = flag.String("p-tool", os.Getenv("UMPIRE_P_TOOL"), "P executable")
		apalacheTool = flag.String("apalache-tool", os.Getenv("UMPIRE_APALACHE_TOOL"), "Apalache executable")
		ivyTool      = flag.String("ivy-tool", os.Getenv("UMPIRE_IVY_TOOL"), "ivy_check executable")
		fizzTool     = flag.String("fizz-tool", os.Getenv("UMPIRE_FIZZ_TOOL"), "FizzBee executable")
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
			output: *output, artifacts: *artifacts, target: *target, backend: *backend, profile: *profile,
			timeout: *timeout, tlaJar: *tlaJar, javaTool: *javaTool, pTool: *pTool,
			apalacheTool: *apalacheTool, ivyTool: *ivyTool, fizzTool: *fizzTool, defaultBound: *defaultBound,
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
	family, err := verificationFamily(defaultBound)
	if err != nil {
		return err
	}
	familyHash, err := verify.HashModelFamily(family)
	if err != nil {
		return err
	}
	index := targetIndex{
		SchemaVersion:    "umpire-verification-target-index/v1",
		GeneratorVersion: generatorVersion,
		ModelFamily:      family.Version,
		ModelFamilyHash:  familyHash,
	}
	files := map[string][]byte{}
	targets := slices.Clone(family.Targets)
	slices.SortFunc(targets, func(left, right verify.VerificationTarget) int {
		return strings.Compare(left.Name, right.Name)
	})
	for _, target := range targets {
		projection, err := verify.Project(family, target.Name)
		if err != nil {
			return err
		}
		targetFiles, entry, err := generateTarget(familyHash, projection)
		if err != nil {
			return err
		}
		mergeFiles(files, target.Name, targetFiles)
		index.Targets = append(index.Targets, entry)
	}
	indexJSON, err := json.MarshalIndent(index, "", "  ")
	if err != nil {
		return err
	}
	files["manifest.json"] = append(indexJSON, '\n')
	toolEnvironment, err := renderToolEnvironment(runner.ToolVersions(), "linux-x86_64")
	if err != nil {
		return err
	}
	files["tools.env"] = toolEnvironment
	return writeFiles(output, files)
}

type targetIndex struct {
	SchemaVersion    string             `json:"schemaVersion"`
	GeneratorVersion string             `json:"generatorVersion"`
	ModelFamily      string             `json:"modelFamilyVersion"`
	ModelFamilyHash  string             `json:"modelFamilyHash"`
	Targets          []targetIndexEntry `json:"targets"`
}

type targetIndexEntry struct {
	Name                string                   `json:"name"`
	ModelHash           string                   `json:"modelHash"`
	Owners              []verify.CapabilityOwner `json:"owners"`
	BackendRequirements []string                 `json:"backendRequirements,omitempty"`
}

func generateTarget(
	familyHash string,
	projection verify.ProjectedTarget,
) (map[string][]byte, targetIndexEntry, error) {
	target, model := projection.Target, projection.Model
	tlaFiles, err := tla.Generate(model)
	if err != nil {
		return nil, targetIndexEntry{}, err
	}
	pFiles, err := pgenerator.Generate(model)
	if err != nil {
		return nil, targetIndexEntry{}, err
	}
	ivyFiles, ivyDiagnostics, err := ivy.Generate(model)
	if err != nil {
		return nil, targetIndexEntry{}, err
	}
	fizzFiles, fizzDiagnostics, err := fizz.Generate(model)
	if err != nil {
		return nil, targetIndexEntry{}, err
	}
	smokeBounds, err := runner.ProfileBounds("smoke")
	if err != nil {
		return nil, targetIndexEntry{}, err
	}
	fizzConfig, err := fizz.RenderConfig(runner.FizzBounds(smokeBounds))
	if err != nil {
		return nil, targetIndexEntry{}, err
	}
	fizzFiles["fizz.yaml"] = fizzConfig
	unsupported := make([]verify.Unsupported, 0, len(ivyDiagnostics)+len(fizzDiagnostics))
	for _, diagnostic := range ivyDiagnostics {
		unsupported = append(unsupported, verify.Unsupported{Backend: "ivy", Construct: diagnostic.Construct, Reason: diagnostic.Reason})
	}
	for _, diagnostic := range fizzDiagnostics {
		unsupported = append(unsupported, verify.Unsupported{
			Backend: "fizz", Construct: diagnostic.Construct, Reason: diagnostic.Reason, Source: diagnostic.Source,
		})
	}
	manifest, err := verify.NewManifest(model, verify.ManifestOptions{
		GeneratorVersion:    generatorVersion,
		Target:              target.Name,
		TargetOwners:        target.Owners,
		TargetModules:       projection.Modules,
		TargetCompositions:  target.Compositions,
		TargetProperties:    projection.Properties,
		ModelFamilyVersion:  projection.ModelFamilyVersion,
		ModelFamilyHash:     familyHash,
		BackendRequirements: target.BackendRequirements,
		MinimumBounds:       target.MinimumBounds,
		FailurePolicy:       target.FailurePolicy,
		Interfaces:          projection.Interfaces,
		Guarantee:           verify.FiniteExhaustive,
		Tools:               runner.ToolVersions(),
		Unsupported:         unsupported,
		Omitted:             target.Omissions,
	})
	if err != nil {
		return nil, targetIndexEntry{}, err
	}
	manifestJSON, err := verify.MarshalManifest(manifest)
	if err != nil {
		return nil, targetIndexEntry{}, err
	}
	modelJSON, err := verify.MarshalModel(model)
	if err != nil {
		return nil, targetIndexEntry{}, err
	}
	closureJSON, err := verify.MarshalClosureReport(projection.Closure)
	if err != nil {
		return nil, targetIndexEntry{}, err
	}
	modelHash, err := verify.HashModel(model)
	if err != nil {
		return nil, targetIndexEntry{}, err
	}
	files := map[string][]byte{
		"closure.json":  closureJSON,
		"manifest.json": manifestJSON,
		"model.ir.json": modelJSON,
	}
	mergeFiles(files, "tla", tlaFiles)
	mergeFiles(files, "p", pFiles)
	mergeFiles(files, "ivy", ivyFiles)
	mergeFiles(files, "fizz", fizzFiles)
	entry := targetIndexEntry{
		Name:                target.Name,
		ModelHash:           modelHash,
		Owners:              slices.Clone(target.Owners),
		BackendRequirements: slices.Clone(target.BackendRequirements),
	}
	slices.Sort(entry.Owners)
	slices.Sort(entry.BackendRequirements)
	return files, entry, nil
}

func verificationModel(defaultBound int) (verify.Model, error) {
	family, err := verificationFamily(defaultBound)
	if err != nil {
		return verify.Model{}, err
	}
	projection, err := verify.Project(family, protocol.ProtocolAtomicTarget)
	return projection.Model, err
}

func verificationFamily(defaultBound int) (verify.ModelFamily, error) {
	compiled, err := protocol.Default()
	if err != nil {
		return verify.ModelFamily{}, fmt.Errorf("compile default Umpire protocol: %w", err)
	}
	catalog, err := assurance.Default()
	if err != nil {
		return verify.ModelFamily{}, fmt.Errorf("compile default Umpire assurance catalog: %w", err)
	}
	return compiled.VerificationFamily(protocol.VerificationOptions{
		DefaultBound:  defaultBound,
		RuleInventory: catalog.VerificationInventory(),
	})
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
	target       string
	backend      string
	profile      string
	timeout      time.Duration
	tlaJar       string
	javaTool     string
	pTool        string
	apalacheTool string
	ivyTool      string
	fizzTool     string
	defaultBound int
}

func checkModels(ctx context.Context, options checkOptions) error {
	family, err := verificationFamily(options.defaultBound)
	if err != nil {
		return err
	}
	targets, err := requestedVerificationTargets(family, options.target)
	if err != nil {
		return err
	}
	toolchain := runner.Toolchain{
		TLAJarPath: options.tlaJar, JavaPath: options.javaTool, PPath: options.pTool,
		ApalachePath: options.apalacheTool, IvyPath: options.ivyTool, FizzPath: options.fizzTool,
	}
	for _, target := range targets {
		projection, err := verify.Project(family, target.Name)
		if err != nil {
			return err
		}
		requests, err := toolchain.Plan(projection.Model, runner.PlanOptions{
			ModelRoot: options.output, ArtifactRoot: options.artifacts, Target: target.Name,
			Backends: options.backend, Profile: options.profile, Requirements: target.BackendRequirements,
			Timeout: options.timeout,
		})
		if err != nil {
			return fmt.Errorf("verification target %q: %w", target.Name, err)
		}
		for _, request := range requests {
			result, err := runner.Check(ctx, request)
			if err != nil {
				return err
			}
			fmt.Printf("%s/%s: %s (%s)\n", target.Name, request.Backend, result.Status, result.Termination)
			if result.Status == verify.Counterexample || result.Status == verify.Inconclusive || result.Status == verify.UnsupportedStatus {
				return fmt.Errorf("%s/%s verification returned %s: %s", target.Name, request.Backend, result.Status, result.Diagnostic)
			}
		}
	}
	return nil
}

func requestedVerificationTargets(family verify.ModelFamily, value string) ([]verify.VerificationTarget, error) {
	if value == "all" {
		result := slices.Clone(family.Targets)
		slices.SortFunc(result, func(left, right verify.VerificationTarget) int {
			return strings.Compare(left.Name, right.Name)
		})
		return result, nil
	}
	target, found := targetByName(family.Targets, value)
	if !found {
		return nil, fmt.Errorf("unknown verification target %q", value)
	}
	return []verify.VerificationTarget{target}, nil
}

func targetByName(targets []verify.VerificationTarget, name string) (verify.VerificationTarget, bool) {
	for _, target := range targets {
		if target.Name == name {
			return target, true
		}
	}
	return verify.VerificationTarget{}, false
}
