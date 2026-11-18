//go:build !temporal_minimal || temporal_with_s3store

package config

func (*S3Archiver) validate(string) error {
	return nil
}
