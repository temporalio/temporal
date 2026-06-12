package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4signer "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

type awsSigningTransport struct {
	creds   aws.CredentialsProvider
	signer  *v4signer.Signer
	region  string
	service string
	wrapped http.RoundTripper
}

func (t *awsSigningTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	creds, err := t.creds.Retrieve(req.Context())
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve AWS credentials: %w", err)
	}

	var bodyBytes []byte
	if req.Body != nil {
		bodyBytes, err = io.ReadAll(req.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read request body: %w", err)
		}
		req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	}

	hash := fmt.Sprintf("%x", sha256.Sum256(bodyBytes))
	err = t.signer.SignHTTP(req.Context(), creds, req, hash, t.service, t.region, time.Now())
	if err != nil {
		return nil, fmt.Errorf("failed to sign request: %w", err)
	}

	if bodyBytes != nil {
		// set the request body just in case the signer consumes the body
		req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	}

	return t.wrapped.RoundTrip(req)
}

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

	var credsProvider aws.CredentialsProvider

	switch strings.ToLower(config.CredentialProvider) {
	case "static":
		credsProvider = credentials.NewStaticCredentialsProvider(
			config.Static.AccessKeyID,
			config.Static.SecretAccessKey,
			config.Static.Token,
		)
	case "environment":
		envConfig, err := awsconfig.NewEnvConfig()
		if err != nil {
			return nil, err
		}
		credsProvider = credentials.NewStaticCredentialsProvider(
			envConfig.Credentials.AccessKeyID,
			envConfig.Credentials.SecretAccessKey,
			envConfig.Credentials.SessionToken,
		)
	case "aws-sdk-default":
		cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
			awsconfig.WithRegion(config.Region),
		)
		if err != nil {
			return nil, err
		}
		credsProvider = cfg.Credentials
	default:
		return nil, fmt.Errorf("unknown AWS credential provider specified: %+v. Accepted options are 'static', 'environment' or 'aws-sdk-default'", config.CredentialProvider)
	}

	return &http.Client{
		Transport: &awsSigningTransport{
			creds:   credsProvider,
			signer:  v4signer.NewSigner(),
			region:  config.Region,
			service: "es",
			wrapped: http.DefaultTransport,
		},
	}, nil
}
