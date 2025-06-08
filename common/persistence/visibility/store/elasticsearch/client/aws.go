package client

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	v1credentials "github.com/aws/aws-sdk-go/aws/credentials"
	elasticaws "github.com/olivere/elastic/v7/aws/v4"
)

func NewAwsHttpClient(cfg ESAWSRequestSigningConfig) (*http.Client, error) {
	if !cfg.Enabled {
		return nil, nil
	}

	if cfg.Region == "" {
		cfg.Region = os.Getenv("AWS_REGION")
		if cfg.Region == "" {
			return nil, fmt.Errorf("unable to resolve AWS region for obtaining AWS Elastic signing credentials")
		}
	}

	// Get AWS SDK v2 credentials
	var v2Creds aws.CredentialsProvider

	switch strings.ToLower(cfg.CredentialProvider) {
	case "static":
		v2Creds = credentials.NewStaticCredentialsProvider(
			cfg.Static.AccessKeyID,
			cfg.Static.SecretAccessKey,
			cfg.Static.Token,
		)
	case "environment":
		// Use default config to get environment credentials
		awsCfg, err := config.LoadDefaultConfig(context.TODO(),
			config.WithRegion(cfg.Region),
		)
		if err != nil {
			return nil, err
		}
		v2Creds = awsCfg.Credentials
	case "aws-sdk-default":
		awsCfg, err := config.LoadDefaultConfig(context.TODO(),
			config.WithRegion(cfg.Region),
		)
		if err != nil {
			return nil, err
		}
		v2Creds = awsCfg.Credentials
	default:
		return nil, fmt.Errorf("unknown AWS credential provider specified: %+v. Accepted options are 'static', 'environment' or 'aws-sdk-default'", cfg.CredentialProvider)
	}

	// Convert v2 credentials to v1 format for olivere/elastic compatibility
	v1Creds := v1credentials.NewCredentials(&v2ToV1CredentialsAdapter{
		v2Provider: v2Creds,
	})

	return elasticaws.NewV4SigningClient(v1Creds, cfg.Region), nil
}

// v2ToV1CredentialsAdapter adapts AWS SDK v2 credentials to v1 format
type v2ToV1CredentialsAdapter struct {
	v2Provider aws.CredentialsProvider
}

func (a *v2ToV1CredentialsAdapter) Retrieve() (v1credentials.Value, error) {
	creds, err := a.v2Provider.Retrieve(context.TODO())
	if err != nil {
		return v1credentials.Value{}, err
	}

	return v1credentials.Value{
		AccessKeyID:     creds.AccessKeyID,
		SecretAccessKey: creds.SecretAccessKey,
		SessionToken:    creds.SessionToken,
		ProviderName:    creds.Source,
	}, nil
}

func (a *v2ToV1CredentialsAdapter) IsExpired() bool {
	// AWS SDK v2 handles expiration internally
	return false
}
