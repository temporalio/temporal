// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package client

import (
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	elasticaws "github.com/olivere/elastic/aws/v4"
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
