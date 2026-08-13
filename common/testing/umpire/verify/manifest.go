package verify

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
)

type ToolVersion struct {
	Name        string         `json:"name"`
	Version     string         `json:"version"`
	SHA256      string         `json:"sha256,omitempty"`
	Artifacts   []ToolArtifact `json:"artifacts,omitempty"`
	ImageDigest string         `json:"imageDigest,omitempty"`
}

type ToolArtifact struct {
	Platform string `json:"platform"`
	SHA256   string `json:"sha256"`
}

type Unsupported struct {
	Backend   string     `json:"backend"`
	Construct string     `json:"construct"`
	Reason    string     `json:"reason"`
	Source    Provenance `json:"source,omitempty"`
}

type ManifestOptions struct {
	GeneratorVersion string
	Guarantee        Status
	Tools            []ToolVersion
	Unsupported      []Unsupported
	Omitted          []Abstraction
	Symmetry         []string
	StateConstraints []string
}

type Manifest struct {
	SchemaVersion      string              `json:"schemaVersion"`
	GeneratorVersion   string              `json:"generatorVersion"`
	ModelVersion       string              `json:"modelVersion"`
	ModelHash          string              `json:"modelHash"`
	Guarantee          Status              `json:"requestedGuarantee"`
	Bounds             map[string]int      `json:"bounds"`
	IdentityPools      map[string][]string `json:"identityPools"`
	Actions            []string            `json:"actions"`
	EnvironmentActions []string            `json:"environmentActions,omitempty"`
	Properties         []string            `json:"properties"`
	Strengthening      []string            `json:"strengthening,omitempty"`
	Fairness           []string            `json:"fairness,omitempty"`
	Sources            []Provenance        `json:"sources,omitempty"`
	Abstractions       []Abstraction       `json:"abstractions,omitempty"`
	Omitted            []Abstraction       `json:"omitted,omitempty"`
	Unsupported        []Unsupported       `json:"unsupported,omitempty"`
	Tools              []ToolVersion       `json:"tools"`
	Inventory          []InventoryItem     `json:"inventory,omitempty"`
	Refinements        []Refinement        `json:"refinements,omitempty"`
	Symmetry           []string            `json:"symmetry,omitempty"`
	StateConstraints   []string            `json:"stateConstraints,omitempty"`
}

func NewManifest(model Model, options ManifestOptions) (Manifest, error) {
	if err := Validate(model); err != nil {
		return Manifest{}, fmt.Errorf("create verification manifest: %w", err)
	}
	if options.GeneratorVersion == "" {
		return Manifest{}, errors.New("create verification manifest: generator version is empty")
	}
	hash, err := HashModel(model)
	if err != nil {
		return Manifest{}, err
	}
	model = normalizeModel(model)
	result := Manifest{
		SchemaVersion:    "umpire-verification-manifest/v1",
		GeneratorVersion: options.GeneratorVersion,
		ModelVersion:     model.Version,
		ModelHash:        hash,
		Guarantee:        options.Guarantee,
		Bounds:           make(map[string]int, len(model.Entities)),
		IdentityPools:    make(map[string][]string, len(model.Entities)),
		Abstractions:     slices.Clone(model.Abstractions),
		Omitted:          slices.Clone(options.Omitted),
		Unsupported:      slices.Clone(options.Unsupported),
		Tools:            slices.Clone(options.Tools),
		Inventory:        slices.Clone(model.Inventory),
		Refinements:      slices.Clone(model.Refinements),
		Symmetry:         slices.Clone(options.Symmetry),
		StateConstraints: slices.Clone(options.StateConstraints),
	}
	sourceSet := map[Provenance]struct{}{}
	for _, entity := range model.Entities {
		result.Bounds[entity.Name] = len(entity.IDs)
		result.IdentityPools[entity.Name] = slices.Clone(entity.IDs)
		addSource(sourceSet, entity.Source)
	}
	for _, relation := range model.Relations {
		addSource(sourceSet, relation.SourceLocation)
	}
	for _, action := range model.Actions {
		result.Actions = append(result.Actions, action.Name)
		if action.Unrealized {
			result.EnvironmentActions = append(result.EnvironmentActions, action.Name)
		}
		addSource(sourceSet, action.Source)
	}
	fairness := map[string]struct{}{}
	for _, property := range model.Properties {
		result.Properties = append(result.Properties, property.Name)
		if property.Strengthening {
			result.Strengthening = append(result.Strengthening, property.Name)
		}
		for _, assumption := range property.Fairness {
			fairness[assumption] = struct{}{}
		}
		addSource(sourceSet, property.Source)
	}
	for assumption := range fairness {
		result.Fairness = append(result.Fairness, assumption)
	}
	for source := range sourceSet {
		result.Sources = append(result.Sources, source)
	}
	slices.Sort(result.Fairness)
	slices.Sort(result.Strengthening)
	slices.Sort(result.EnvironmentActions)
	slices.SortFunc(result.Sources, compareProvenance)
	slices.SortFunc(result.Tools, func(left, right ToolVersion) int { return compareString(left.Name, right.Name) })
	for index := range result.Tools {
		result.Tools[index].Artifacts = slices.Clone(result.Tools[index].Artifacts)
		slices.SortFunc(result.Tools[index].Artifacts, func(left, right ToolArtifact) int {
			return compareString(left.Platform, right.Platform)
		})
	}
	slices.SortFunc(result.Unsupported, func(left, right Unsupported) int {
		if comparison := compareString(left.Backend, right.Backend); comparison != 0 {
			return comparison
		}
		return compareString(left.Construct, right.Construct)
	})
	slices.SortFunc(result.Omitted, func(left, right Abstraction) int { return compareString(left.Name, right.Name) })
	slices.Sort(result.Symmetry)
	slices.Sort(result.StateConstraints)
	return result, nil
}

func MarshalManifest(manifest Manifest) ([]byte, error) {
	encoded, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(encoded, '\n'), nil
}

func addSource(sources map[Provenance]struct{}, source Provenance) {
	if source.Path != "" || source.Symbol != "" {
		sources[source] = struct{}{}
	}
}

func compareProvenance(left, right Provenance) int {
	if comparison := compareString(left.Path, right.Path); comparison != 0 {
		return comparison
	}
	return compareString(left.Symbol, right.Symbol)
}
