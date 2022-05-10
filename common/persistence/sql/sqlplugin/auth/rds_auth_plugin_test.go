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
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/config"
)

type (
	rdsAuthPluginTestSuite struct {
		suite.Suite
		controller *gomock.Controller
	}
)

func TestSessionTestSuite(t *testing.T) {
	s := new(rdsAuthPluginTestSuite)
	suite.Run(t, s)
}

func (s *rdsAuthPluginTestSuite) SetupSuite() {

}

func (s *rdsAuthPluginTestSuite) TearDownSuite() {

}

func (s *rdsAuthPluginTestSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
}

func (s *rdsAuthPluginTestSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *rdsAuthPluginTestSuite) TestRdsAuthPlugin() {
	rdsAuthFn = func(ctx context.Context, endpoint, region, dbUser string, creds aws.CredentialsProvider, optFns ...func(options *auth.BuildAuthTokenOptions)) (string, error) {
		return "token", nil
	}

	plugin := NewRDSAuthPlugin(aws.NewConfig())

	cfg, err := plugin.GetConfig(&config.SQL{
		PluginName:        "mysql",
		ConnectAttributes: map[string]string{},
	})

	s.Equal(nil, err)
	s.Equal(&config.SQL{
		PluginName:      "mysql",
		Password:        "token",
		ConnectProtocol: "tcp",
		ConnectAttributes: map[string]string{
			"allowCleartextPasswords": "true",
		},
	}, cfg)
}
