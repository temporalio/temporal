package client

import (
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
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

	var awsCredentials *credentials.Credentials

	switch strings.ToLower(config.CredentialProvider) {
	case "static":
		awsCredentials = credentials.NewStaticCredentials(
			config.Static.AccessKeyID,
			config.Static.SecretAccessKey,
			config.Static.Token,
		)
	case "environment":
		awsCredentials = credentials.NewEnvCredentials()
	case "aws-sdk-default":
		awsSession, err := session.NewSession(&aws.Config{
			Region: &config.Region,
		})

		if err != nil {
			return nil, err
		}

		awsCredentials = awsSession.Config.Credentials
	default:
		return nil, fmt.Errorf("unknown AWS credential provider specified: %+v. Accepted options are 'static', 'environment' or 'session'", config.CredentialProvider)
	}

	return elasticaws.NewV4SigningClient(awsCredentials, config.Region), nil
}
