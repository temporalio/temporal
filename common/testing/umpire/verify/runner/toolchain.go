package runner

import (
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"go.temporal.io/server/common/testing/umpire/verify"
	"go.temporal.io/server/common/testing/umpire/verify/ivy"
	"go.temporal.io/server/common/testing/umpire/verify/tla"
)

var pinnedToolVersions = []verify.ToolVersion{
	{Name: "apalache", Version: "0.61.0", SHA256: "f2d761315667f977c7c33792d95167f12e83b8a775909180886bcb67660470c5"},
	{Name: "ivy", Version: "1.8.26", Artifacts: []verify.ToolArtifact{
		{Platform: "darwin-universal2", SHA256: "d2f8df47e4731f2e23f7b5ab0852662e871217a9506c36310d75d81a9f09219c"},
		{Platform: "linux-x86_64", SHA256: "2a71da0bb2ce6314ddb40b6d76c6d734b8102db51c158477c2ef85b45da65dc1"},
	}},
	{Name: "p", Version: "3.1.0", SHA256: "b2a212e3b1af1bf2fdc9b80899da2901d6625d1a2e478d478e30028872a4bdc1"},
	{Name: "tla2tools", Version: "1.7.4", SHA256: "936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"},
}

type Toolchain struct {
	TLAJarPath   string
	JavaPath     string
	PPath        string
	ApalachePath string
	IvyPath      string
}

type PlanOptions struct {
	ModelRoot    string
	ArtifactRoot string
	Target       string
	Backends     string
	Profile      string
	Requirements []string
	Timeout      time.Duration
}

func ToolVersions() []verify.ToolVersion {
	result := slices.Clone(pinnedToolVersions)
	for index := range result {
		result[index].Artifacts = slices.Clone(result[index].Artifacts)
	}
	return result
}

func (t Toolchain) Plan(model verify.Model, options PlanOptions) ([]Request, error) {
	bounds, err := profileBounds(options.Profile)
	if err != nil {
		return nil, err
	}
	if len(model.Entities) != 0 {
		bounds.Identities = make(map[string]int, len(model.Entities))
		for _, entity := range model.Entities {
			bounds.Identities[entity.Name] = len(entity.IDs)
		}
	}
	backends, err := requestedBackends(options.Backends, options.Profile)
	if err != nil {
		return nil, err
	}
	backends, err = targetBackends(backends, options.Requirements)
	if err != nil {
		return nil, err
	}
	metadata, err := planMetadataFor(model)
	if err != nil {
		return nil, err
	}
	requests := make([]Request, 0, len(backends))
	for _, backend := range backends {
		request, err := t.planRequest(backend, options, bounds, metadata)
		if err != nil {
			return nil, err
		}
		requests = append(requests, request)
	}
	return requests, nil
}

type planMetadata struct {
	model         verify.Model
	tlaVocabulary verify.TraceVocabulary
	ivyVocabulary verify.TraceVocabulary
	actionNames   map[string]string
	propertyNames map[string]string
	fairness      []string
	abstractions  []verify.Abstraction
	unsupported   []verify.Unsupported
}

func planMetadataFor(model verify.Model) (planMetadata, error) {
	result := planMetadata{
		model:         model,
		actionNames:   make(map[string]string, len(model.Actions)),
		propertyNames: make(map[string]string, len(model.Properties)),
		abstractions:  slices.Clone(model.Abstractions),
	}
	fairness := make(map[string]struct{})
	for _, action := range model.Actions {
		result.actionNames[tla.ActionIdentifier(action.Name)] = action.Name
		result.actionNames[ivy.ActionIdentifier(action.Name)] = action.Name
	}
	for _, property := range model.Properties {
		result.propertyNames[tla.PropertyIdentifier(property.Name)] = property.Name
		result.propertyNames[ivy.PropertyIdentifier(property.Name)] = property.Name
		for _, assumption := range property.Fairness {
			fairness[assumption] = struct{}{}
		}
		if property.Kind != verify.SafetyProperty {
			result.unsupported = append(result.unsupported, verify.Unsupported{
				Backend: "ivy", Construct: "property " + property.Name,
				Reason: "Ivy generation supports inductive safety properties only", Source: property.Source,
			})
		}
	}
	for assumption := range fairness {
		result.fairness = append(result.fairness, assumption)
	}
	slices.Sort(result.fairness)
	if model.Version != "" {
		var err error
		result.tlaVocabulary, err = tla.TraceVocabulary(model)
		if err != nil {
			return planMetadata{}, err
		}
		result.ivyVocabulary, err = ivy.TraceVocabulary(model)
		if err != nil {
			return planMetadata{}, err
		}
	}
	return result, nil
}

func (t Toolchain) planRequest(
	backend Backend,
	options PlanOptions,
	bounds verify.Bounds,
	metadata planMetadata,
) (Request, error) {
	request := Request{
		Backend: backend, Model: metadata.model, Target: options.Target, Profile: options.Profile,
		ArtifactDir: filepath.Join(options.ArtifactRoot, options.Target, options.Profile), Timeout: options.Timeout, Bounds: bounds,
		JavaPath: t.JavaPath, ActionNames: cloneNames(metadata.actionNames), PropertyNames: cloneNames(metadata.propertyNames),
		Fairness: slices.Clone(metadata.fairness), Abstractions: slices.Clone(metadata.abstractions), Unsupported: unsupportedForBackend(backend, metadata.unsupported),
	}
	switch backendFamily(backend) {
	case "tla":
		request.TraceVocabulary = metadata.tlaVocabulary
	case "ivy":
		request.TraceVocabulary = metadata.ivyVocabulary
	default:
	}
	switch backend {
	case SANY, TLC:
		request.ModelDir = filepath.Join(options.ModelRoot, options.Target, "tla")
		request.ToolPath = t.TLAJarPath
		request.ToolVersion = pinnedToolVersion("tla2tools")
		if request.ToolPath == "" {
			return request, errors.New("TLA+ verification requires -tla-jar or UMPIRE_TLA_JAR")
		}
	case Apalache, ApalacheProof:
		request.ModelDir = filepath.Join(options.ModelRoot, options.Target, "tla")
		request.ToolPath = t.ApalachePath
		request.ToolVersion = pinnedToolVersion("apalache")
		if backend == Apalache {
			if options.Profile == "nightly" {
				request.Bounds.MaxDepth = 20
			} else {
				request.Bounds.MaxDepth = 5
			}
		}
		if request.ToolPath == "" {
			return request, errors.New("apalache verification requires -apalache-tool or UMPIRE_APALACHE_TOOL")
		}
	case P, PEx:
		request.ModelDir = filepath.Join(options.ModelRoot, options.Target, "p")
		request.ToolPath = t.PPath
		request.ToolVersion = pinnedToolVersion("p")
		if backend == PEx && request.Bounds.MaxDepth > 100 {
			request.Bounds.MaxDepth = 100
		}
		if request.ToolPath == "" {
			return request, errors.New("p verification requires -p-tool or UMPIRE_P_TOOL")
		}
	case Ivy:
		request.ModelDir = filepath.Join(options.ModelRoot, options.Target, "ivy")
		request.ToolPath = t.IvyPath
		request.ToolVersion = pinnedToolVersion("ivy")
		if request.ToolPath == "" {
			return request, errors.New("ivy verification requires -ivy-tool or UMPIRE_IVY_TOOL")
		}
	default:
		return request, fmt.Errorf("unknown verification backend %q", backend)
	}
	if options.Profile == "nightly" {
		request.Config = "Umpire-nightly.cfg"
	}
	return request, nil
}

func requestedBackends(value, profile string) ([]Backend, error) {
	if value == "all" {
		result := []Backend{SANY, TLC, Apalache, P, PEx, Ivy}
		if profile == "nightly" {
			result = append(result, ApalacheProof)
		}
		return result, nil
	}
	var result []Backend
	for _, name := range strings.Split(value, ",") {
		backend := Backend(strings.TrimSpace(name))
		switch backend {
		case SANY, TLC, Apalache, ApalacheProof, P, PEx, Ivy:
			result = append(result, backend)
		default:
			return nil, fmt.Errorf("unknown verification backend %q", name)
		}
	}
	return result, nil
}

func targetBackends(backends []Backend, requirements []string) ([]Backend, error) {
	if len(requirements) == 0 {
		return slices.Clone(backends), nil
	}
	allowed := make(map[string]struct{}, len(requirements))
	for _, requirement := range requirements {
		allowed[requirement] = struct{}{}
	}
	result := make([]Backend, 0, len(backends))
	for _, backend := range backends {
		family := backendFamily(backend)
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

func backendFamily(backend Backend) string {
	switch backend {
	case SANY, TLC, Apalache, ApalacheProof:
		return "tla"
	case P, PEx:
		return "p"
	default:
		return string(backend)
	}
}

func unsupportedForBackend(backend Backend, unsupported []verify.Unsupported) []verify.Unsupported {
	var result []verify.Unsupported
	family := backendFamily(backend)
	for _, item := range unsupported {
		if item.Backend == string(backend) || item.Backend == family {
			result = append(result, item)
		}
	}
	return result
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

func pinnedToolVersion(name string) string {
	for _, tool := range pinnedToolVersions {
		if tool.Name == name {
			return tool.Version
		}
	}
	return ""
}

func cloneNames(names map[string]string) map[string]string {
	result := make(map[string]string, len(names))
	for identifier, name := range names {
		result[identifier] = name
	}
	return result
}
