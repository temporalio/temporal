//go:build !temporal_minimal || temporal_with_s3store

package config

import (
	"fmt"
	"slices"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func (s *S3Archiver) validate(path string) error {
	if err := s.Validate(); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

// Validate validates the S3 archiver configuration.
func (s *S3Archiver) Validate() error {
	if s == nil || s.StorageClass == "" {
		return nil
	}
	if !slices.Contains(types.StorageClass("").Values(), types.StorageClass(s.StorageClass)) {
		return fmt.Errorf("invalid S3 storageClass %q", s.StorageClass)
	}
	return nil
}
