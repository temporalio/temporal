//go:build !temporal_minimal || temporal_with_gstorage

package config

func (*GstorageArchiver) validate() error {
	return nil
}
