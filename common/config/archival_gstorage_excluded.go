//go:build temporal_minimal && !temporal_with_gstorage

package config

import (
	"errors"
)

// The Google Cloud Storage archiver is excluded from this build.
func validateGstorageArchiver(cfg *GstorageArchiver) error {
	if cfg != nil {
		return errors.New("archival provider gstorage is configured, but Google Cloud Storage archival is excluded from this build, see build tag temporal_with_gstorage")
	}
	return nil
}
