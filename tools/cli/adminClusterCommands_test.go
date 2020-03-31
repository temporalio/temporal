// The MIT License (MIT)
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
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package cli

import (
	"flag"
	"testing"

	"github.com/bmizerany/assert"
	"github.com/golang/mock/gomock"
	"github.com/urfave/cli"

	serverFrontendTest "github.com/uber/cadence/.gen/go/cadence/workflowservicetest"
	serverShared "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
)

func TestAdminAddSearchAttribute_isValueTypeValid(t *testing.T) {
	testCases := []struct {
		name     string
		input    int
		expected bool
	}{
		{
			name:     "negative",
			input:    -1,
			expected: false,
		},
		{
			name:     "valid",
			input:    0,
			expected: true,
		},
		{
			name:     "valid",
			input:    5,
			expected: true,
		},
		{
			name:     "unknown",
			input:    6,
			expected: false,
		},
	}

	for _, testCase := range testCases {
		assert.Equal(t, testCase.expected, isValueTypeValid(testCase.input))
	}
}

func TestAdminFailover(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	serverFrontendClient := serverFrontendTest.NewMockClient(mockCtrl)
	domainCLI := &domainCLIImpl{
		frontendClient: serverFrontendClient,
	}

	var listDomainsResponse = &serverShared.ListDomainsResponse{
		Domains: []*serverShared.DescribeDomainResponse{
			{
				DomainInfo: &serverShared.DomainInfo{
					Name:        common.StringPtr("test-domain"),
					Description: common.StringPtr("a test domain"),
					OwnerEmail:  common.StringPtr("test@uber.com"),
					Data: map[string]string{
						common.DomainDataKeyForManagedFailover: "true",
					},
				},
				ReplicationConfiguration: &serverShared.DomainReplicationConfiguration{
					ActiveClusterName: common.StringPtr("active"),
					Clusters: []*serverShared.ClusterReplicationConfiguration{
						{
							ClusterName: common.StringPtr("active"),
						},
						{
							ClusterName: common.StringPtr("standby"),
						},
					},
				},
			},
		},
	}

	serverFrontendClient.EXPECT().ListDomains(gomock.Any(), gomock.Any()).Return(listDomainsResponse, nil).Times(1)
	serverFrontendClient.EXPECT().UpdateDomain(gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)
	set := flag.NewFlagSet("test", 0)
	set.String(FlagActiveClusterName, "standby", "test flag")

	cliContext := cli.NewContext(nil, set, nil)
	succeed, failed := domainCLI.failoverDomains(cliContext)
	assert.Equal(t, []string{"test-domain"}, succeed)
	assert.Equal(t, 0, len(failed))

	serverFrontendClient.EXPECT().ListDomains(gomock.Any(), gomock.Any()).Return(listDomainsResponse, nil).Times(1)
	set = flag.NewFlagSet("test", 0)
	set.String(FlagActiveClusterName, "active", "test flag")

	cliContext = cli.NewContext(nil, set, nil)
	succeed, failed = domainCLI.failoverDomains(cliContext)
	assert.Equal(t, 0, len(succeed))
	assert.Equal(t, 0, len(failed))
}
