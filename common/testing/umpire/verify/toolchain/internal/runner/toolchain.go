package runner

import (
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"go.temporal.io/server/common/testing/umpire/verify"
	"go.temporal.io/server/common/testing/umpire/verify/toolchain/internal/fizz"
	"go.temporal.io/server/common/testing/umpire/verify/toolchain/internal/ivy"
	"go.temporal.io/server/common/testing/umpire/verify/toolchain/internal/tla"
)

var pinnedToolVersions = []verify.ToolVersion{
	{Name: "apalache", Version: "0.61.0", URL: "https://github.com/apalache-mc/apalache/releases/download/v0.61.0/apalache-0.61.0.zip", SHA256: "f2d761315667f977c7c33792d95167f12e83b8a775909180886bcb67660470c5", Archive: "apalache.zip", ArchiveType: "zip", ExtractRoot: "apalache", Executable: "apalache/apalache-0.61.0/bin/apalache-mc"},
	{Name: "fizzbee", Version: "0.5.2", Artifacts: []verify.ToolArtifact{
		{Platform: "darwin-arm64", URL: "https://github.com/fizzbee-io/fizzbee/releases/download/v0.5.2/fizzbee-v0.5.2-macos_arm.tar.gz", SHA256: "aab223e0bac8f0c052cf774dc25872f72c138da30f4079b914bb9c8921910904", Archive: "fizzbee.tar.gz", ArchiveType: "tar.gz", ExtractRoot: "fizz", Executable: "fizz/fizz"},
		{Platform: "darwin-x86_64", URL: "https://github.com/fizzbee-io/fizzbee/releases/download/v0.5.2/fizzbee-v0.5.2-macos_x86.tar.gz", SHA256: "6293bd7ab90c79b8607dc9fb2f09407fde0e11ac6596e884bef7f660178597fa", Archive: "fizzbee.tar.gz", ArchiveType: "tar.gz", ExtractRoot: "fizz", Executable: "fizz/fizz"},
		{Platform: "linux-arm64", URL: "https://github.com/fizzbee-io/fizzbee/releases/download/v0.5.2/fizzbee-v0.5.2-linux_arm.tar.gz", SHA256: "00011bbfe9bf4c7bcb03a5bf1f5b7fe7390111ad6f0611c6be71e8692504da4e", Archive: "fizzbee.tar.gz", ArchiveType: "tar.gz", ExtractRoot: "fizz", Executable: "fizz/fizz"},
		{Platform: "linux-x86_64", URL: "https://github.com/fizzbee-io/fizzbee/releases/download/v0.5.2/fizzbee-v0.5.2-linux_x86.tar.gz", SHA256: "f494b7b2afcc7ce24575ed91a389b46bbbbe5976f9e4b5cd717327012f5e0395", Archive: "fizzbee.tar.gz", ArchiveType: "tar.gz", ExtractRoot: "fizz", Executable: "fizz/fizz"},
	}},
	{Name: "ivy", Version: "1.8.26", Artifacts: []verify.ToolArtifact{
		{Platform: "darwin-arm64", URL: "https://files.pythonhosted.org/packages/c8/38/f829838dc68e5e5aad7babd8273f253e424eb399731b34f432ea66b16647/ms_ivy-1.8.26-cp310-cp310-macosx_10_9_universal2.whl", SHA256: "d2f8df47e4731f2e23f7b5ab0852662e871217a9506c36310d75d81a9f09219c", Archive: "ms_ivy-1.8.26-cp310-cp310-macosx_10_9_universal2.whl", ArchiveType: "wheel", ExtractRoot: "ivy", Package: "ms-ivy", Executable: "ivy/bin/ivy_check"},
		{Platform: "darwin-x86_64", URL: "https://files.pythonhosted.org/packages/c8/38/f829838dc68e5e5aad7babd8273f253e424eb399731b34f432ea66b16647/ms_ivy-1.8.26-cp310-cp310-macosx_10_9_universal2.whl", SHA256: "d2f8df47e4731f2e23f7b5ab0852662e871217a9506c36310d75d81a9f09219c", Archive: "ms_ivy-1.8.26-cp310-cp310-macosx_10_9_universal2.whl", ArchiveType: "wheel", ExtractRoot: "ivy", Package: "ms-ivy", Executable: "ivy/bin/ivy_check"},
		{Platform: "linux-x86_64", URL: "https://files.pythonhosted.org/packages/0a/f7/c8f9264bae27f2c56c2d02630c94a0fa400c66bc6772bf6ff049fdcd8101/ms_ivy-1.8.26-cp310-cp310-manylinux1_x86_64.whl", SHA256: "2a71da0bb2ce6314ddb40b6d76c6d734b8102db51c158477c2ef85b45da65dc1", Archive: "ms_ivy-1.8.26-cp310-cp310-manylinux1_x86_64.whl", ArchiveType: "wheel", ExtractRoot: "ivy", Package: "ms-ivy", Executable: "ivy/bin/ivy_check"},
	}},
	{Name: "p", Version: "3.1.0", URL: "https://api.nuget.org/v3-flatcontainer/p/3.1.0/p.3.1.0.nupkg", SHA256: "b2a212e3b1af1bf2fdc9b80899da2901d6625d1a2e478d478e30028872a4bdc1", Archive: "p.nupkg", ArchiveType: "nuget", ExtractRoot: "p", Package: "P", Executable: "p/p"},
	{Name: "tla2tools", Version: "1.7.4", URL: "https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar", SHA256: "936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88", Archive: "tla2tools.jar", ArchiveType: "file", ExtractRoot: "tla", Executable: "tla/tla2tools.jar"},
}

type Toolchain struct {
	TLAJarPath   string
	JavaPath     string
	PPath        string
	ApalachePath string
	IvyPath      string
	FizzPath     string
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
	bounds, err := ProfileBounds(options.Profile)
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
	if options.Backends == "all" {
		backends = compatibleBackends(backends, metadata.unsupported)
		if len(backends) == 0 {
			return []Request{}, nil
		}
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
	model          verify.Model
	tlaVocabulary  verify.TraceVocabulary
	ivyVocabulary  verify.TraceVocabulary
	fizzVocabulary verify.TraceVocabulary
	actionNames    map[string]string
	propertyNames  map[string]string
	fairness       []string
	abstractions   []verify.Abstraction
	unsupported    []verify.Unsupported
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
		result.actionNames[fizz.ActionIdentifier(action.Name)] = action.Name
	}
	for _, property := range model.Properties {
		result.propertyNames[tla.PropertyIdentifier(property.Name)] = property.Name
		result.propertyNames[ivy.PropertyIdentifier(property.Name)] = property.Name
		result.propertyNames[fizz.PropertyIdentifier(property.Name)] = property.Name
		for _, assumption := range property.Fairness {
			fairness[assumption] = struct{}{}
		}
		switch property.Kind {
		case verify.QuiescentProperty:
			result.unsupported = append(result.unsupported, verify.Unsupported{
				Backend: "ivy", Construct: "property " + property.Name,
				Reason: "Ivy generation supports inductive safety properties only", Source: property.Source,
			})
		case verify.ProgressProperty:
			for _, backend := range []Backend{SANY, TLC, Apalache, ApalacheProof, P, PEx, Ivy, Fizz} {
				result.unsupported = append(result.unsupported, verify.Unsupported{
					Backend: string(backend), Construct: "property " + property.Name,
					Reason: "backend generation does not support temporal progress properties", Source: property.Source,
				})
			}
		default:
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
		result.fizzVocabulary, err = fizz.TraceVocabulary(model)
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
	case "fizz":
		request.TraceVocabulary = metadata.fizzVocabulary
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
	case Fizz:
		request.ModelDir = filepath.Join(options.ModelRoot, options.Target, "fizz")
		request.ToolPath = t.FizzPath
		request.ToolVersion = pinnedToolVersion("fizzbee")
		request.Bounds = FizzBounds(request.Bounds)
		if request.ToolPath == "" {
			return request, errors.New("FizzBee verification requires -fizz-tool or UMPIRE_FIZZ_TOOL")
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
		case Fizz:
			if profile != "smoke" {
				return nil, errors.New("FizzBee is available only in the smoke execution profile")
			}
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

func compatibleBackends(backends []Backend, unsupported []verify.Unsupported) []Backend {
	result := make([]Backend, 0, len(backends))
	for _, backend := range backends {
		if len(unsupportedForBackend(backend, unsupported)) == 0 {
			result = append(result, backend)
		}
	}
	return result
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

func ProfileBounds(profile string) (verify.Bounds, error) {
	switch profile {
	case "smoke":
		return verify.Bounds{MaxDepth: 100, Schedules: 100}, nil
	case "nightly":
		return verify.Bounds{MaxDepth: 1_000, Schedules: 10_000}, nil
	default:
		return verify.Bounds{}, fmt.Errorf("unknown verification profile %q", profile)
	}
}

func FizzBounds(bounds verify.Bounds) verify.Bounds {
	if bounds.MaxDepth > 5 {
		bounds.MaxDepth = 5
	}
	return bounds
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
