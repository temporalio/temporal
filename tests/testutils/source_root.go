package testutils

import (
	"os"
	"path/filepath"
	"runtime"
)

var (
	_, fileName, _, _ = runtime.Caller(0) // should be .../temporal/tests/testhelper/source_root.go
	rootDirectory     = filepath.Dir(filepath.Dir(filepath.Dir(fileName)))
)

// GetRepoRootDirectory returns the root directory of the temporal repo.
func GetRepoRootDirectory(opts ...Option) string {
	p := &osParams{
		Getenv: os.Getenv,
	}
	for _, opt := range opts {
		opt(p)
	}
	if customRootDirectory := p.Getenv("TEMPORAL_ROOT"); customRootDirectory != "" {
		return customRootDirectory
	}
	return rootDirectory
}

// region Options for GetRepoRootDirectory.
// Used for testing.

type osParams struct {
	Getenv func(string) string
}

type Option func(os *osParams)

func WithGetenv(getenv func(string) string) Option {
	return func(os *osParams) {
		os.Getenv = getenv
	}
}

// endregion
