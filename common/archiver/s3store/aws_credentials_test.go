package s3store

import (
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
)

func TestNewS3Credentials_Static(t *testing.T) {
	cfg := &config.S3Archiver{
		Region:             "us-east-1",
		CredentialProvider: "static",
		Static: config.S3StaticCredentialProvider{
			AccessKeyID:     "test-access-key",
			SecretAccessKey: "test-secret-key",
		},
	}

	creds, err := newS3Credentials(cfg)
	require.NoError(t, err)
	require.NotNil(t, creds)

	// Verify credentials can be retrieved
	value, err := creds.Get()
	require.NoError(t, err)
	assert.Equal(t, "test-access-key", value.AccessKeyID)
	assert.Equal(t, "test-secret-key", value.SecretAccessKey)
	assert.Empty(t, value.SessionToken)
}

func TestNewS3Credentials_StaticWithToken(t *testing.T) {
	cfg := &config.S3Archiver{
		Region:             "us-east-1",
		CredentialProvider: "static",
		Static: config.S3StaticCredentialProvider{
			AccessKeyID:     "test-access-key",
			SecretAccessKey: "test-secret-key",
			Token:           "test-session-token",
		},
	}

	creds, err := newS3Credentials(cfg)
	require.NoError(t, err)
	require.NotNil(t, creds)

	// Verify credentials can be retrieved
	value, err := creds.Get()
	require.NoError(t, err)
	assert.Equal(t, "test-access-key", value.AccessKeyID)
	assert.Equal(t, "test-secret-key", value.SecretAccessKey)
	assert.Equal(t, "test-session-token", value.SessionToken)
}

func TestNewS3Credentials_Environment(t *testing.T) {
	cfg := &config.S3Archiver{
		Region:             "us-east-1",
		CredentialProvider: "environment",
	}

	creds, err := newS3Credentials(cfg)
	require.NoError(t, err)
	require.NotNil(t, creds)

	// Note: We can't validate the actual credential values without setting env vars,
	// but we can verify the credentials object was created
	assert.NotNil(t, creds)
}

func TestNewS3Credentials_AwsSdkDefault(t *testing.T) {
	cfg := &config.S3Archiver{
		Region:             "us-east-1",
		CredentialProvider: "aws-sdk-default",
	}

	creds, err := newS3Credentials(cfg)
	require.NoError(t, err)
	// Should return nil to allow session to use default credential chain
	assert.Nil(t, creds)
}

func TestNewS3Credentials_EmptyProvider(t *testing.T) {
	cfg := &config.S3Archiver{
		Region:             "us-east-1",
		CredentialProvider: "",
	}

	creds, err := newS3Credentials(cfg)
	require.NoError(t, err)
	// Empty provider should default to aws-sdk-default (nil)
	assert.Nil(t, creds)
}

func TestNewS3Credentials_InvalidProvider(t *testing.T) {
	cfg := &config.S3Archiver{
		Region:             "us-east-1",
		CredentialProvider: "invalid-provider",
	}

	creds, err := newS3Credentials(cfg)
	require.Error(t, err)
	assert.Nil(t, creds)
	assert.Contains(t, err.Error(), "unknown AWS credential provider")
	assert.Contains(t, err.Error(), "invalid-provider")
}

func TestNewS3Credentials_CaseInsensitive(t *testing.T) {
	testCases := []struct {
		name     string
		provider string
		expectNil bool
	}{
		{
			name:     "uppercase STATIC",
			provider: "STATIC",
			expectNil: false,
		},
		{
			name:     "mixed case Static",
			provider: "Static",
			expectNil: false,
		},
		{
			name:     "uppercase ENVIRONMENT",
			provider: "ENVIRONMENT",
			expectNil: false,
		},
		{
			name:     "mixed case Environment",
			provider: "Environment",
			expectNil: false,
		},
		{
			name:     "uppercase AWS-SDK-DEFAULT",
			provider: "AWS-SDK-DEFAULT",
			expectNil: true,
		},
		{
			name:     "mixed case Aws-Sdk-Default",
			provider: "Aws-Sdk-Default",
			expectNil: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.S3Archiver{
				Region:             "us-east-1",
				CredentialProvider: tc.provider,
				Static: config.S3StaticCredentialProvider{
					AccessKeyID:     "test-key",
					SecretAccessKey: "test-secret",
				},
			}

			creds, err := newS3Credentials(cfg)
			require.NoError(t, err)
			if tc.expectNil {
				assert.Nil(t, creds)
			} else {
				assert.NotNil(t, creds)
			}
		})
	}
}

func TestCreateS3Session_Success(t *testing.T) {
	cfg := &config.S3Archiver{
		Region:             "us-east-1",
		CredentialProvider: "static",
		Static: config.S3StaticCredentialProvider{
			AccessKeyID:     "test-access-key",
			SecretAccessKey: "test-secret-key",
		},
	}

	sess, err := createS3Session(cfg)
	require.NoError(t, err)
	require.NotNil(t, sess)

	// Verify session config
	assert.Equal(t, "us-east-1", *sess.Config.Region)
}

func TestCreateS3Session_WithEndpoint(t *testing.T) {
	endpoint := "http://localhost:4566"
	cfg := &config.S3Archiver{
		Region:             "us-east-1",
		Endpoint:           &endpoint,
		S3ForcePathStyle:   true,
		CredentialProvider: "static",
		Static: config.S3StaticCredentialProvider{
			AccessKeyID:     "test-access-key",
			SecretAccessKey: "test-secret-key",
		},
	}

	sess, err := createS3Session(cfg)
	require.NoError(t, err)
	require.NotNil(t, sess)

	// Verify session config
	assert.Equal(t, "us-east-1", *sess.Config.Region)
	assert.Equal(t, endpoint, *sess.Config.Endpoint)
	assert.True(t, *sess.Config.S3ForcePathStyle)
}

func TestCreateS3Session_EmptyRegion(t *testing.T) {
	cfg := &config.S3Archiver{
		Region:             "",
		CredentialProvider: "static",
		Static: config.S3StaticCredentialProvider{
			AccessKeyID:     "test-access-key",
			SecretAccessKey: "test-secret-key",
		},
	}

	sess, err := createS3Session(cfg)
	require.Error(t, err)
	assert.Nil(t, sess)
	assert.Equal(t, errEmptyAwsRegion, err)
}

func TestCreateS3Session_InvalidCredentialProvider(t *testing.T) {
	cfg := &config.S3Archiver{
		Region:             "us-east-1",
		CredentialProvider: "invalid",
	}

	sess, err := createS3Session(cfg)
	require.Error(t, err)
	assert.Nil(t, sess)
	assert.Contains(t, err.Error(), "unknown AWS credential provider")
}

func TestCreateS3Session_BackwardCompatibility(t *testing.T) {
	// Test that existing configs without credential provider still work
	cfg := &config.S3Archiver{
		Region: "us-west-2",
	}

	sess, err := createS3Session(cfg)
	require.NoError(t, err)
	require.NotNil(t, sess)

	// Verify session uses default credential chain
	assert.Equal(t, "us-west-2", *sess.Config.Region)
	// Credentials should not be explicitly set (using default chain)
	assert.NotNil(t, sess.Config.Credentials)
}

func TestNewS3Credentials_EnvironmentWithRealEnvVars(t *testing.T) {
	// Save original env vars
	originalAccessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	originalSecretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	defer func() {
		// Restore original env vars
		if originalAccessKey != "" {
			os.Setenv("AWS_ACCESS_KEY_ID", originalAccessKey)
		} else {
			os.Unsetenv("AWS_ACCESS_KEY_ID")
		}
		if originalSecretKey != "" {
			os.Setenv("AWS_SECRET_ACCESS_KEY", originalSecretKey)
		} else {
			os.Unsetenv("AWS_SECRET_ACCESS_KEY")
		}
	}()

	// Set test env vars
	os.Setenv("AWS_ACCESS_KEY_ID", "env-test-key")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "env-test-secret")

	cfg := &config.S3Archiver{
		Region:             "us-east-1",
		CredentialProvider: "environment",
	}

	creds, err := newS3Credentials(cfg)
	require.NoError(t, err)
	require.NotNil(t, creds)

	// Verify credentials can be retrieved from environment
	value, err := creds.Get()
	require.NoError(t, err)
	assert.Equal(t, "env-test-key", value.AccessKeyID)
	assert.Equal(t, "env-test-secret", value.SecretAccessKey)
	assert.Equal(t, credentials.EnvProviderName, value.ProviderName)
}
