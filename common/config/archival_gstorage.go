//go:build !temporal_minimal || temporal_with_gstorage

package config

func validateGstorageArchival(*Archival) error {
	return nil
}
