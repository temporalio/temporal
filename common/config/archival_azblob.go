//go:build !temporal_minimal || temporal_with_azblob

package config

func (*AzblobArchiver) validate(string) error {
	return nil
}
