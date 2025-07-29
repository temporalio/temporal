package client

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

// AWSSigningTransport implements http.RoundTripper with AWS SigV4 signing
type AWSSigningTransport struct {
	base        http.RoundTripper
	signer      *v4.Signer
	credentials aws.CredentialsProvider
	region      string
	service     string
}

func (t *AWSSigningTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Clone the request to avoid modifying the original
	clonedReq := req.Clone(req.Context())

	// Get credentials
	creds, err := t.credentials.Retrieve(context.TODO())
	if err != nil {
		return nil, err
	}

	// Sign the request
	err = t.signer.SignHTTP(context.TODO(), creds, clonedReq, "", t.service, t.region, time.Now())
	if err != nil {
		return nil, err
	}

	// Execute the request
	return t.base.RoundTrip(clonedReq)
}

func NewAwsHttpClientV8(cfg ESAWSRequestSigningConfig) (*http.Client, error) {
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

	// Create AWS signing transport
	awsTransport := &AWSSigningTransport{
		base:        http.DefaultTransport,
		signer:      v4.NewSigner(),
		credentials: v2Creds,
		region:      cfg.Region,
		service:     "es", // Elasticsearch service name
	}

	return &http.Client{
		Transport: awsTransport,
	}, nil
}
