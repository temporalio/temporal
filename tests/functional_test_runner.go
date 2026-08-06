package tests

import (
	"fmt"
	"regexp"
	"testing"

	"go.temporal.io/server/tests/testcore"
)

// RunOption selects functional tests for [Run]. Filters are combined with
// logical AND in the order supplied to Run.
type RunOption func(*runOptions)

type runSelectorKind uint8

const (
	runSelectorExactNames runSelectorKind = iota
	runSelectorRegex
	runSelectorPredicate
)

type runSelector struct {
	kind       runSelectorKind
	exactNames []string
	regex      string
	predicate  func(FunctionalTestEntry) bool
}

type runOptions struct {
	selectors []runSelector
}

// WithFunctionalTestNames selects the entries with exactly these logical
// names. Run reports any unknown name before it starts a test.
func WithFunctionalTestNames(names ...string) RunOption {
	names = append([]string(nil), names...)
	return func(options *runOptions) {
		options.selectors = append(options.selectors, runSelector{kind: runSelectorExactNames, exactNames: names})
	}
}

// WithFunctionalTestNameRegex selects entries whose logical names match
// pattern. Run reports an invalid pattern before it starts a test.
func WithFunctionalTestNameRegex(pattern string) RunOption {
	return func(options *runOptions) {
		options.selectors = append(options.selectors, runSelector{kind: runSelectorRegex, regex: pattern})
	}
}

// WithFunctionalTestPredicate selects entries for which predicate returns
// true. It is the only selector that permits an empty selection.
func WithFunctionalTestPredicate(predicate func(FunctionalTestEntry) bool) RunOption {
	return func(options *runOptions) {
		options.selectors = append(options.selectors, runSelector{kind: runSelectorPredicate, predicate: predicate})
	}
}

// Run invokes selected functional tests as subtests of t. Options are
// validated in order before testcore creates a router or starts a subtest.
func Run(t *testing.T, factory testcore.ClusterFactory, options ...RunOption) {
	t.Helper()
	entries, err := selectFunctionalTestEntries(options...)
	if err != nil {
		t.Fatal(err)
	}
	testcore.Run(t, factory, func() {
		runFunctionalTestEntries(t, entries)
	})
}

func selectFunctionalTestEntries(options ...RunOption) ([]FunctionalTestEntry, error) {
	var configuration runOptions
	for i, option := range options {
		if option == nil {
			return nil, fmt.Errorf("functional test option %d is nil", i)
		}
		option(&configuration)
	}

	entries := FunctionalTestEntries()
	knownNames := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		knownNames[entry.Name] = struct{}{}
	}

	allowsEmptySelection := false
	for _, selector := range configuration.selectors {
		switch selector.kind {
		case runSelectorPredicate:
			if selector.predicate == nil {
				return nil, fmt.Errorf("functional test predicate is nil")
			}
			allowsEmptySelection = true
		case runSelectorExactNames:
			for _, name := range selector.exactNames {
				if _, ok := knownNames[name]; !ok {
					return nil, fmt.Errorf("unknown functional test entry %q", name)
				}
			}
		case runSelectorRegex:
			if _, err := regexp.Compile(selector.regex); err != nil {
				return nil, fmt.Errorf("invalid functional test name regex %q: %w", selector.regex, err)
			}
		default:
			return nil, fmt.Errorf("unknown functional test selector kind %d", selector.kind)
		}
	}

	selected := entries
	for _, selector := range configuration.selectors {
		selected = filterFunctionalTestEntries(selected, selector)
	}
	if len(selected) == 0 && !allowsEmptySelection {
		return nil, fmt.Errorf("functional test selection is empty; use WithFunctionalTestPredicate to allow it")
	}
	return selected, nil
}

func filterFunctionalTestEntries(entries []FunctionalTestEntry, selector runSelector) []FunctionalTestEntry {
	switch selector.kind {
	case runSelectorPredicate:
		filtered := make([]FunctionalTestEntry, 0, len(entries))
		for _, entry := range entries {
			if selector.predicate(entry) {
				filtered = append(filtered, entry)
			}
		}
		return filtered
	case runSelectorExactNames:
		wanted := make(map[string]struct{}, len(selector.exactNames))
		for _, name := range selector.exactNames {
			wanted[name] = struct{}{}
		}
		filtered := make([]FunctionalTestEntry, 0, len(entries))
		for _, entry := range entries {
			if _, ok := wanted[entry.Name]; ok {
				filtered = append(filtered, entry)
			}
		}
		return filtered
	case runSelectorRegex:
		pattern := regexp.MustCompile(selector.regex)
		filtered := make([]FunctionalTestEntry, 0, len(entries))
		for _, entry := range entries {
			if pattern.MatchString(entry.Name) {
				filtered = append(filtered, entry)
			}
		}
		return filtered
	default:
		return nil
	}
}

func runFunctionalTestEntries(t *testing.T, entries []FunctionalTestEntry) {
	t.Helper()
	for _, entry := range entries {
		t.Run(entry.Name, entry.run)
	}
}
