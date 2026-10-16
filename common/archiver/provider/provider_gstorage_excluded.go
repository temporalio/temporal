//go:build temporal_minimal && !temporal_with_gstorage

package provider

import (
	"errors"
	"fmt"

	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/config"
)

// The Google Cloud Storage archiver is excluded from this build.
var errGstorageExcluded = fmt.Errorf("%w: %q (Google Cloud Storage archival is excluded from this build, see build tag temporal_with_gstorage)", ErrUnknownScheme, archiver.URISchemeGstorage)

func validateGstorageConfigs(h *config.HistoryArchiverProvider, v *config.VisibilityArchiverProvider) error {
	if (h != nil && h.Gstorage != nil) || (v != nil && v.Gstorage != nil) {
		return errors.New("archival provider gstorage is configured, but Google Cloud Storage archival is excluded from this build, see build tag temporal_with_gstorage")
	}
	return nil
}

func (p *archiverProvider) newGstorageHistoryArchiver() (archiver.HistoryArchiver, error) {
	return nil, errGstorageExcluded
}

func (p *archiverProvider) newGstorageVisibilityArchiver() (archiver.VisibilityArchiver, error) {
	return nil, errGstorageExcluded
}
