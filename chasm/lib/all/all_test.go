package all_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/all"
	"go.temporal.io/server/common/log"
)

// excludedLibDirs are subdirectories of chasm/lib that are intentionally absent
// from RegisterAll. Add a new entry here only when the library is genuinely
// unsuitable for decoding contexts (e.g. test-only libraries).
var excludedLibDirs = map[string]bool{
	"all":   true, // this package itself
	"tests": true, // test-only library
}

// TestAllLibrariesRegistered scans the chasm/lib directory and asserts that every
// library subdirectory is represented in the registry built by RegisterAll.
// If this test fails after adding a new library, add its NewNilLibrary() call to
// chasm/lib/all/all.go.
func TestAllLibrariesRegistered(t *testing.T) {
	registry := chasm.NewRegistry(log.NewNoopLogger())
	require.NoError(t, all.RegisterAll(registry))

	registeredNames := make(map[string]bool)
	for _, name := range registry.LibraryNames() {
		registeredNames[name] = true
	}

	// chasm/lib is the parent of this package's directory.
	entries, err := os.ReadDir("..")
	require.NoError(t, err)

	for _, entry := range entries {
		if !entry.IsDir() || excludedLibDirs[entry.Name()] {
			continue
		}
		require.True(t,
			registeredNames[entry.Name()],
			"library directory %q exists in chasm/lib but is not registered in RegisterAll; "+
				"add its NewNilLibrary() call to chasm/lib/all/all.go",
			entry.Name(),
		)
	}
}
