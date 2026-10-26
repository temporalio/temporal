//go:build temporal_minimal && !temporal_with_azblob

package config

import (
	"fmt"
)

// validate fails if a is set, since the Azure Blob Storage archiver is excluded from this build.
func (a *AzblobArchiver) validate(path string) error {
	if a != nil {
		return fmt.Errorf("%s: Azure Blob Storage archival is not compiled into this server; remove this config or rebuild with the temporal_with_azblob build tag", path)
	}
	return nil
}
