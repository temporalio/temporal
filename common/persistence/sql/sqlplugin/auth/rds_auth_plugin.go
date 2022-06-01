// The MIT License
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

package auth

import (
	"context"
	"encoding/base64"
	"errors"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	AWSConfig "github.com/aws/aws-sdk-go-v2/config"
	AWSAuth "github.com/aws/aws-sdk-go-v2/feature/rds/auth"

	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/config"
)

const defaultTimeout = time.Second * 10
const rdsCaUrl = "https://s3.amazonaws.com/rds-downloads/rds-combined-ca-bundle.pem"

var rdsAuthFn = AWSAuth.BuildAuthToken

func init() {
	RegisterPlugin("rds-iam-auth", NewRDSAuthPlugin(nil))
}

func fetchRdsCA(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", rdsCaUrl, nil)
	if err != nil {
		return "", err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}

	defer resp.Body.Close()
	pem, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(pem), nil
}

type RDSAuthPlugin struct {
	awsConfig        *aws.Config
	rdsPemBundle     string
	initRdsPemBundle sync.Once
}

func NewRDSAuthPlugin(awsConfig *aws.Config) AuthPlugin {
	return &RDSAuthPlugin{
		awsConfig: awsConfig,
	}
}

func (plugin *RDSAuthPlugin) getToken(ctx context.Context, addr string, region string, user string, credentials aws.CredentialsProvider) (string, error) {
	reqCtx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	return rdsAuthFn(reqCtx, addr, region, user, credentials)
}

func (plugin *RDSAuthPlugin) resolveAwsConfig(ctx context.Context) (*aws.Config, error) {
	if plugin.awsConfig != nil {
		return plugin.awsConfig, nil
	}

	reqCtx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	cfg, err := AWSConfig.LoadDefaultConfig(reqCtx)
	if err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (plugin *RDSAuthPlugin) GetConfig(ctx context.Context, cfg *config.SQL) (*config.SQL, error) {
	awsCfg, err := plugin.resolveAwsConfig(ctx)
	if err != nil {
		return nil, err
	}

	token, err := plugin.getToken(ctx, cfg.ConnectAddr, awsCfg.Region, cfg.User, awsCfg.Credentials)
	if err != nil {
		return nil, err
	}

	cfg.Password = token
	cfg.ConnectProtocol = "tcp"

	if cfg.ConnectAttributes == nil {
		cfg.ConnectAttributes = map[string]string{}
	}

	// mysql requires this plugin to use the token as a password
	if cfg.PluginName == "mysql" {
		cfg.ConnectAttributes["allowCleartextPasswords"] = "true"
	}

	// if TLS is not configured, we default to the RDS CA
	// this is required for mysql to send cleartext passwords
	if cfg.TLS == nil {
		var fetchErr error
		plugin.initRdsPemBundle.Do(func() {
			ca, err := fetchRdsCA(ctx)
			if err != nil {
				fetchErr = err
				return
			}

			plugin.rdsPemBundle = ca
		})

		if fetchErr != nil {
			return nil, fetchErr
		}

		if plugin.rdsPemBundle == "" {
			return nil, errors.New("rds_auth_plugin: unable to retrieve rds ca certificates")
		}

		cfg.TLS = &auth.TLS{
			Enabled: true,
			CaData:  plugin.rdsPemBundle,
		}
	}

	return cfg, nil
}
