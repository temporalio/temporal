package operators

import (
	"errors"
	"fmt"
	"go/ast"
	"go/types"
	"slices"
	"strings"
	"unicode"

	"github.com/avito-tech/go-mutesting/mutator"
	"go.temporal.io/server/tools/mutationtest/operators/custom"
)

const (
	implementationUpstream implementation = "upstream"
	implementationLocal    implementation = "local"
)

type implementation string

// Operator is an executable mutation operator from the supported catalog.
type Operator struct {
	name   string
	mutate mutator.Mutator
}

// Name returns the operator's canonical category/name identifier.
func (o Operator) Name() string {
	return o.name
}

// Mutate returns the mutations the operator can apply to node.
func (o Operator) Mutate(pkg *types.Package, info *types.Info, node ast.Node) []mutator.Mutation {
	return o.mutate(pkg, info, node)
}

// Descriptor contains stable operator metadata for discovery output.
type Descriptor struct {
	Name           string
	Category       string
	Default        bool
	Implementation string
}

type definition struct {
	name           string
	defaultEnabled bool
	implementation implementation
	mutate         mutator.Mutator
}

type catalog struct {
	definitions []definition
}

// Resolve expands include and exclude selectors into canonical operator order.
func Resolve(include string, exclude string) ([]Operator, error) {
	catalog, err := loadCatalog()
	if err != nil {
		return nil, err
	}
	return catalog.resolve(include, exclude)
}

// List returns every explicitly supported operator in canonical order.
func List() ([]Descriptor, error) {
	catalog, err := loadCatalog()
	if err != nil {
		return nil, err
	}
	descriptors := make([]Descriptor, 0, len(catalog.definitions))
	for _, definition := range catalog.definitions {
		category, _, _ := strings.Cut(definition.name, "/")
		descriptors = append(descriptors, Descriptor{
			Name:           definition.name,
			Category:       category,
			Default:        definition.defaultEnabled,
			Implementation: string(definition.implementation),
		})
	}
	return descriptors, nil
}

func loadCatalog() (catalog, error) {
	definitions, err := builtinDefinitions()
	if err != nil {
		return catalog{}, err
	}
	definitions = append(definitions, definition{
		name:           "boolean/literal",
		implementation: implementationLocal,
		mutate:         custom.BooleanLiteral,
	})
	return newCatalog(definitions)
}

func newCatalog(definitions []definition) (catalog, error) {
	definitions = slices.Clone(definitions)
	slices.SortFunc(definitions, func(left definition, right definition) int {
		return strings.Compare(left.name, right.name)
	})
	seen := make(map[string]struct{}, len(definitions))
	for _, definition := range definitions {
		category, name, ok := strings.Cut(definition.name, "/")
		if !ok || category == "" || name == "" || strings.Contains(name, "/") || strings.IndexFunc(definition.name, unicode.IsSpace) >= 0 {
			return catalog{}, fmt.Errorf("invalid mutation operator name %q", definition.name)
		}
		if category == "all" || category == "default" {
			return catalog{}, fmt.Errorf("reserved mutation operator category %q", category)
		}
		if definition.mutate == nil {
			return catalog{}, fmt.Errorf("mutation operator %q has a nil mutator", definition.name)
		}
		if definition.implementation != implementationUpstream && definition.implementation != implementationLocal {
			return catalog{}, fmt.Errorf("mutation operator %q has invalid implementation %q", definition.name, definition.implementation)
		}
		if _, duplicate := seen[definition.name]; duplicate {
			return catalog{}, fmt.Errorf("duplicate mutation operator %q", definition.name)
		}
		seen[definition.name] = struct{}{}
	}
	return catalog{definitions: definitions}, nil
}

func (c catalog) resolve(include string, exclude string) ([]Operator, error) {
	selected := make(map[string]struct{}, len(c.definitions))
	includeSelectors := strings.Fields(include)
	if len(includeSelectors) == 0 {
		includeSelectors = []string{"default"}
	}
	if err := c.expand(includeSelectors, selected); err != nil {
		return nil, err
	}

	excluded := make(map[string]struct{})
	if err := c.expand(strings.Fields(exclude), excluded); err != nil {
		return nil, err
	}
	for name := range excluded {
		delete(selected, name)
	}

	resolved := make([]Operator, 0, len(selected))
	for _, definition := range c.definitions {
		if _, ok := selected[definition.name]; !ok {
			continue
		}
		resolved = append(resolved, Operator{name: definition.name, mutate: definition.mutate})
	}
	if len(resolved) == 0 {
		return nil, errors.New("mutation selection is empty")
	}
	return resolved, nil
}

func (c catalog) expand(selectors []string, selected map[string]struct{}) error {
	for _, selector := range selectors {
		matched := false
		for _, definition := range c.definitions {
			category, _, _ := strings.Cut(definition.name, "/")
			if selector == "all" ||
				selector == "default" && definition.defaultEnabled ||
				selector == definition.name ||
				selector == category {
				selected[definition.name] = struct{}{}
				matched = true
			}
		}
		if matched {
			continue
		}
		if strings.Count(selector, "/") != 0 && (strings.Count(selector, "/") != 1 || strings.HasPrefix(selector, "/") || strings.HasSuffix(selector, "/")) {
			return fmt.Errorf("invalid mutation selector %q", selector)
		}
		return fmt.Errorf("unknown mutation selector %q", selector)
	}
	return nil
}
