//go:build !temporal_minimal || temporal_with_gstorage

package config

func validateGstorageArchiver(*GstorageArchiver) error {
	return nil
}
