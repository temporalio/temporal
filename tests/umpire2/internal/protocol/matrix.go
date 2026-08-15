package protocol

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"go.temporal.io/server/common/testing/umpire"
	coreregress "go.temporal.io/server/common/testing/umpire/regress"
)

// MatrixCase is one deterministic Temporal protocol/profile assignment.
type MatrixCase struct {
	Name      string
	Profile   coreregress.Profile
	Key       ActionKey
	Action    *umpire.Action
	GapReason string
}

// MatrixOptions adds environment capability requirements to selected action keys.
type MatrixOptions struct {
	Requires map[ActionKey][]string
}

// GenerateMatrix adapts the compiled Temporal protocol to generic deterministic pairwise cases.
func GenerateMatrix(compiled *Protocol, profiles []coreregress.Profile, configured ...MatrixOptions) ([]MatrixCase, error) {
	if compiled == nil {
		return nil, errors.New("protocol matrix: protocol is nil")
	}
	if len(profiles) == 0 {
		return nil, errors.New("protocol matrix: profiles are empty")
	}
	if len(configured) > 1 {
		return nil, errors.New("protocol matrix: at most one options value is allowed")
	}
	var options MatrixOptions
	if len(configured) == 1 {
		options = configured[0]
	}
	catalog := compiled.ActionCatalog()
	if len(catalog) == 0 {
		return nil, errors.New("protocol matrix: action catalog is empty")
	}
	profileByName := make(map[string]coreregress.Profile, len(profiles))
	profileNames := make([]string, 0, len(profiles))
	for _, profile := range profiles {
		if profile.Name == "" {
			return nil, errors.New("protocol matrix: profile name is empty")
		}
		if _, duplicate := profileByName[profile.Name]; duplicate {
			return nil, fmt.Errorf("protocol matrix: duplicate profile %q", profile.Name)
		}
		profile.Capabilities = slices.Clone(profile.Capabilities)
		profileByName[profile.Name] = profile
		profileNames = append(profileNames, profile.Name)
	}
	entities := orderedCatalogValues(catalog, func(entry ActionCatalogEntry) string { return string(entry.Key.Entity) })
	edges := orderedCatalogValues(catalog, func(entry ActionCatalogEntry) string { return entry.Key.From + "/" + entry.Key.Event })
	hostings := orderedCatalogValues(catalog, func(entry ActionCatalogEntry) string { return entry.Key.Hosting.String() })
	actionOrGap := orderedCatalogValues(catalog, catalogActionValue)
	dimensions := []umpire.MatrixDimension{
		{Name: "profile", Values: profileNames},
		{Name: "entity", Values: entities},
		{Name: "edge", Values: edges},
		{Name: "hosting", Values: hostings},
		{Name: "action-or-gap", Values: actionOrGap},
	}
	valid := func(candidate umpire.MatrixCase) bool {
		profile := profileByName[candidate.Value("profile")]
		_, found := findCatalogEntry(catalog, candidate, profile, options.Requires)
		return found
	}
	generated, err := umpire.GeneratePairwise(dimensions, valid)
	if err != nil {
		return nil, fmt.Errorf("protocol matrix: %w", err)
	}
	result := make([]MatrixCase, 0, len(generated))
	for _, candidate := range generated {
		profile := profileByName[candidate.Value("profile")]
		entry, found := findCatalogEntry(catalog, candidate, profile, options.Requires)
		if !found {
			return nil, errors.New("protocol matrix: generated invalid candidate")
		}
		matrixCase := MatrixCase{
			Name:      matrixCaseName(candidate),
			Profile:   profile,
			Key:       entry.Key,
			GapReason: entry.GapReason,
		}
		if entry.Action != nil {
			action := cloneAction(*entry.Action)
			matrixCase.Action = &action
		}
		result = append(result, matrixCase)
	}
	return result, nil
}

func orderedCatalogValues(catalog []ActionCatalogEntry, value func(ActionCatalogEntry) string) []string {
	seen := map[string]struct{}{}
	var result []string
	for _, entry := range catalog {
		current := value(entry)
		if _, duplicate := seen[current]; duplicate {
			continue
		}
		seen[current] = struct{}{}
		result = append(result, current)
	}
	return result
}

func catalogActionValue(entry ActionCatalogEntry) string {
	if entry.Action == nil {
		return "gap"
	}
	return "action:" + entry.Action.Name
}

func findCatalogEntry(
	catalog []ActionCatalogEntry,
	candidate umpire.MatrixCase,
	profile coreregress.Profile,
	requirements map[ActionKey][]string,
) (ActionCatalogEntry, bool) {
	capabilities := make(map[string]struct{}, len(profile.Capabilities))
	for _, capability := range profile.Capabilities {
		capabilities[capability] = struct{}{}
	}
	for _, entry := range catalog {
		if string(entry.Key.Entity) != candidate.Value("entity") ||
			entry.Key.From+"/"+entry.Key.Event != candidate.Value("edge") ||
			entry.Key.Hosting.String() != candidate.Value("hosting") ||
			catalogActionValue(entry) != candidate.Value("action-or-gap") {
			continue
		}
		for _, required := range requirements[entry.Key] {
			if _, available := capabilities[required]; !available {
				return ActionCatalogEntry{}, false
			}
		}
		return entry, true
	}
	return ActionCatalogEntry{}, false
}

func matrixCaseName(candidate umpire.MatrixCase) string {
	parts := make([]string, len(candidate.Values))
	for index, value := range candidate.Values {
		parts[index] = value.Value
	}
	return strings.NewReplacer("/", "-", ":", "-", " ", "-").Replace(strings.Join(parts, "__"))
}
