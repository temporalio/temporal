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

	"github.com/aws/aws-sdk-go-v2/aws"
	AWSConfig "github.com/aws/aws-sdk-go-v2/config"
	AWSAuth "github.com/aws/aws-sdk-go-v2/feature/rds/auth"

	"go.temporal.io/server/common/config"
)

var rdsAuthFn = AWSAuth.BuildAuthToken

func init() {
	RegisterPlugin("rds-iam-auth", NewRDSAuthPlugin(nil))
}

type RDSAuthPlugin struct {
	awsConfig *aws.Config
}

func NewRDSAuthPlugin(awsConfig *aws.Config) AuthPlugin {
	return &RDSAuthPlugin{
		awsConfig: awsConfig,
	}
}

func (plugin *RDSAuthPlugin) GetConfig(cfg *config.SQL) (*config.SQL, error) {
	awsCfg := plugin.awsConfig
	if awsCfg == nil {
		c, err := AWSConfig.LoadDefaultConfig(context.TODO())
		if err != nil {
			return nil, err
		}

		awsCfg = &c
	}

	token, err := rdsAuthFn(context.TODO(), cfg.ConnectAddr, awsCfg.Region, cfg.User, awsCfg.Credentials)
	if err != nil {
		return nil, err
	}

	cfg.Password = token
	cfg.ConnectProtocol = "tcp"

	if cfg.ConnectAttributes == nil {
		cfg.ConnectAttributes = map[string]string{}
	}

	if cfg.PluginName == "mysql" {
		cfg.ConnectAttributes["allowCleartextPasswords"] = "true"
	}

	return cfg, nil
}
