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
		target       = flag.String("target", "all", "verification target name or all")
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
			output: *output, artifacts: *artifacts, target: *target, backend: *backend, profile: *profile,
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
		model, report, err := verify.Project(family, target.Name)
		if err != nil {
			return err
		}
		targetFiles, entry, err := generateTarget(family, familyHash, target, model, report)
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
	family verify.ModelFamily,
	familyHash string,
	target verify.VerificationTarget,
	model verify.Model,
	report verify.ClosureReport,
) (map[string][]byte, targetIndexEntry, error) {
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
	unsupported := make([]verify.Unsupported, len(ivyDiagnostics))
	for index, diagnostic := range ivyDiagnostics {
		unsupported[index] = verify.Unsupported{Backend: "ivy", Construct: diagnostic.Construct, Reason: diagnostic.Reason}
	}
	manifest, err := verify.NewManifest(model, verify.ManifestOptions{
		GeneratorVersion:    generatorVersion,
		Target:              target.Name,
		TargetOwners:        target.Owners,
		TargetModules:       targetModuleNames(family, target),
		TargetCompositions:  target.Compositions,
		TargetProperties:    targetPropertyNames(family, target),
		ModelFamilyVersion:  family.Version,
		ModelFamilyHash:     familyHash,
		BackendRequirements: target.BackendRequirements,
		MinimumBounds:       target.MinimumBounds,
		FailurePolicy:       target.FailurePolicy,
		Interfaces:          targetManifestInterfaces(family, target),
		Guarantee:           verify.FiniteExhaustive,
		Tools:               pinnedTools,
		Unsupported:         unsupported,
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
	closureJSON, err := verify.MarshalClosureReport(report)
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

func targetModuleNames(family verify.ModelFamily, target verify.VerificationTarget) []string {
	modules := slices.Clone(target.Modules)
	for _, selected := range target.Compositions {
		for _, composition := range family.Compositions {
			if composition.Name == selected {
				modules = append(modules, composition.Modules...)
				break
			}
		}
	}
	slices.Sort(modules)
	return slices.Compact(modules)
}

func targetPropertyNames(family verify.ModelFamily, target verify.VerificationTarget) []string {
	properties := slices.Clone(target.Properties)
	for _, selected := range target.Compositions {
		for _, composition := range family.Compositions {
			if composition.Name == selected {
				properties = append(properties, composition.Properties...)
				break
			}
		}
	}
	slices.Sort(properties)
	return slices.Compact(properties)
}

func targetManifestInterfaces(family verify.ModelFamily, target verify.VerificationTarget) []verify.ManifestInterface {
	selected := make(map[string]struct{})
	for _, module := range targetModuleNames(family, target) {
		selected[module] = struct{}{}
	}
	owners := make(map[string]verify.CapabilityOwner, len(family.Modules))
	for _, module := range family.Modules {
		owners[module.Name] = module.Owner
	}
	var result []verify.ManifestInterface
	for _, declared := range family.Interfaces {
		_, providerSelected := selected[declared.Provider]
		var consumers []verify.ManifestModuleRef
		for _, consumer := range declared.Consumers {
			if _, consumerSelected := selected[consumer]; consumerSelected {
				consumers = append(consumers, verify.ManifestModuleRef{Module: consumer, Owner: owners[consumer]})
			}
		}
		if !providerSelected && len(consumers) == 0 {
			continue
		}
		manifestInterface := verify.ManifestInterface{
			Name:       declared.Name,
			Provider:   verify.ManifestModuleRef{Module: declared.Provider, Owner: owners[declared.Provider]},
			Consumers:  consumers,
			Identities: slices.Clone(declared.Identities),
		}
		for _, obligation := range declared.Obligations {
			manifestInterface.Obligations = append(manifestInterface.Obligations, obligation.Name)
		}
		result = append(result, manifestInterface)
	}
	return result
}

func verificationModel(defaultBound int) (verify.Model, error) {
	family, err := verificationFamily(defaultBound)
	if err != nil {
		return verify.Model{}, err
	}
	model, _, err := verify.Project(family, protocol.ProtocolAtomicTarget)
	return model, err
}

func verificationFamily(defaultBound int) (verify.ModelFamily, error) {
	compiled, err := protocol.Default()
	if err != nil {
		return verify.ModelFamily{}, fmt.Errorf("compile default Umpire protocol: %w", err)
	}
	return compiled.VerificationFamily(protocol.VerificationOptions{DefaultBound: defaultBound})
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
	family, err := verificationFamily(options.defaultBound)
	if err != nil {
		return err
	}
	targets, err := requestedVerificationTargets(family, options.target)
	if err != nil {
		return err
	}
	for _, target := range targets {
		model, _, err := verify.Project(family, target.Name)
		if err != nil {
			return err
		}
		targetBounds := bounds
		targetBounds.Identities = make(map[string]int, len(model.Entities))
		for _, entity := range model.Entities {
			targetBounds.Identities[entity.Name] = len(entity.IDs)
		}
		options.target = target.Name
		selectedBackends, err := targetBackends(backends, target.BackendRequirements)
		if err != nil {
			return fmt.Errorf("verification target %q: %w", target.Name, err)
		}
		for _, backend := range selectedBackends {
			request, err := runnerRequest(backend, options, targetBounds, model)
			if err != nil {
				return err
			}
			result, err := runner.Check(ctx, request)
			if err != nil {
				return err
			}
			fmt.Printf("%s/%s: %s (%s)\n", target.Name, backend, result.Status, result.Termination)
			if result.Status == verify.Counterexample || result.Status == verify.Inconclusive || result.Status == verify.UnsupportedStatus {
				return fmt.Errorf("%s/%s verification returned %s: %s", target.Name, backend, result.Status, result.Diagnostic)
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

func targetBackends(backends []runner.Backend, requirements []string) ([]runner.Backend, error) {
	if len(requirements) == 0 {
		return slices.Clone(backends), nil
	}
	allowed := make(map[string]struct{}, len(requirements))
	for _, requirement := range requirements {
		allowed[requirement] = struct{}{}
	}
	result := make([]runner.Backend, 0, len(backends))
	for _, backend := range backends {
		family := string(backend)
		switch backend {
		case runner.SANY, runner.TLC, runner.Apalache, runner.ApalacheProof:
			family = "tla"
		case runner.P, runner.PEx:
			family = "p"
		case runner.Ivy:
			family = "ivy"
		default:
		}
		_, exact := allowed[string(backend)]
		_, familyAllowed := allowed[family]
		if exact || familyAllowed {
			result = append(result, backend)
		}
	}
	if len(result) == 0 {
		return nil, errors.New("none of the requested backends satisfy target requirements")
	}
	return result, nil
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
		Backend: backend, Target: options.target, Profile: options.profile,
		ArtifactDir: filepath.Join(options.artifacts, options.target, options.profile), Timeout: options.timeout, Bounds: bounds,
		JavaPath: options.javaTool, ActionNames: actionNames, PropertyNames: propertyNames,
		Fairness: fairness, Abstractions: model.Abstractions, Unsupported: unsupported,
	}
	switch backend {
	case runner.SANY, runner.TLC:
		request.ModelDir = filepath.Join(options.output, options.target, "tla")
		request.ToolPath = options.tlaJar
		request.ToolVersion = "1.7.4"
		if request.ToolPath == "" {
			return request, errors.New("TLA+ verification requires -tla-jar or UMPIRE_TLA_JAR")
		}
	case runner.Apalache, runner.ApalacheProof:
		request.ModelDir = filepath.Join(options.output, options.target, "tla")
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
		request.ModelDir = filepath.Join(options.output, options.target, "p")
		request.ToolPath = options.pTool
		request.ToolVersion = "3.1.0"
		if backend == runner.PEx && request.Bounds.MaxDepth > 100 {
			request.Bounds.MaxDepth = 100
		}
		if request.ToolPath == "" {
			return request, errors.New("p verification requires -p-tool or UMPIRE_P_TOOL")
		}
	case runner.Ivy:
		request.ModelDir = filepath.Join(options.output, options.target, "ivy")
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
