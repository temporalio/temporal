package client

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	elasticaws "github.com/olivere/elastic/v7/aws/v4"
)

func NewAwsHttpClient(config ESAWSRequestSigningConfig) (*http.Client, error) {
	if !config.Enabled {
		return nil, nil
	}

	if config.Region == "" {
		config.Region = os.Getenv("AWS_REGION")
		if config.Region == "" {
			return nil, fmt.Errorf("unable to resolve AWS region for obtaining AWS Elastic signing credentials")
		}
	}

	ctx := context.Background()
	var credentialsProvider aws.CredentialsProvider

	switch strings.ToLower(config.CredentialProvider) {
	case "static":
		credentialsProvider = credentials.NewStaticCredentialsProvider(
			config.Static.AccessKeyID,
			config.Static.SecretAccessKey,
			config.Static.Token,
		)
	case "environment", "aws-sdk-default":
		cfg, err := awsconfig.LoadDefaultConfig(ctx,
			awsconfig.WithRegion(config.Region),
		)
		if err != nil {
			return nil, err
		}
		credentialsProvider = cfg.Credentials
	default:
		return nil, fmt.Errorf("unknown AWS credential provider specified: %+v. Accepted options are 'static', 'environment' or 'aws-sdk-default'", config.CredentialProvider)
	}

	return elasticaws.NewV4SigningClient(credentialsProvider, config.Region), nil
}
