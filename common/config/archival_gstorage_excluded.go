//go:build temporal_minimal && !temporal_with_gstorage

package config

import (
	"fmt"
)

// The Google Cloud Storage archiver is excluded from this build.
func (g *GstorageArchiver) validate(path string) error {
	if g != nil {
		return fmt.Errorf("%s: Google Cloud Storage archival is not compiled into this server; remove this config or rebuild with the temporal_with_gstorage build tag", path)
	}
	return nil
}
