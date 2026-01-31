// AWS Credential Provider for S3 Archiver

package s3store

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"go.temporal.io/server/common/config"
)

// newS3Credentials creates AWS credentials based on the provided configuration.
// It supports three credential provider types:
//   - "static": Uses explicitly provided access key and secret
//   - "environment": Reads credentials from environment variables
//   - "aws-sdk-default" or empty: Uses AWS SDK default credential chain
//
// Returns nil for "aws-sdk-default" to allow session.NewSession to use the default credential chain.
func newS3Credentials(cfg *config.S3Archiver) (*credentials.Credentials, error) {
	// Default to aws-sdk-default if not specified (backward compatibility)
	provider := strings.ToLower(cfg.CredentialProvider)
	if provider == "" {
		provider = "aws-sdk-default"
	}

	switch provider {
	case "static":
		return credentials.NewStaticCredentials(
			cfg.Static.AccessKeyID,
			cfg.Static.SecretAccessKey,
			cfg.Static.Token,
		), nil

	case "environment":
		return credentials.NewEnvCredentials(), nil

	case "aws-sdk-default":
		// Return nil to let session.NewSession use default credential chain
		return nil, nil

	default:
		return nil, fmt.Errorf(
			"unknown AWS credential provider specified: %q. Accepted options are 'static', 'environment', or 'aws-sdk-default'",
			cfg.CredentialProvider,
		)
	}
}

// createS3Session creates an AWS session with the provided configuration and credentials.
func createS3Session(cfg *config.S3Archiver) (*session.Session, error) {
	if len(cfg.Region) == 0 {
		return nil, errEmptyAwsRegion
	}

	creds, err := newS3Credentials(cfg)
	if err != nil {
		return nil, err
	}

	s3Config := &aws.Config{
		Endpoint:         cfg.Endpoint,
		Region:           aws.String(cfg.Region),
		S3ForcePathStyle: aws.Bool(cfg.S3ForcePathStyle),
		LogLevel:         (*aws.LogLevelType)(&cfg.LogLevel),
	}

	// Only set credentials if explicitly provided (not aws-sdk-default)
	if creds != nil {
		s3Config.Credentials = creds
	}

	return session.NewSession(s3Config)
}
