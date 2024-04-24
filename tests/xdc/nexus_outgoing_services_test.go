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

//go:build !race

// need to run xdc tests with race detector off because of ringpop bug causing data race issue

package xdc

import (
	"flag"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/durationpb"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/tests"
)

type (
	NexusOutgoingServicesTestSuite struct {
		xdcBaseSuite
	}
)

func TestNexusOutgoingServicesTestSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(NexusOutgoingServicesTestSuite))
}

func (s *NexusOutgoingServicesTestSuite) SetupSuite() {
	s.dynamicConfigOverrides = map[dynamicconfig.Key]interface{}{
		// Make sure we don't hit the rate limiter in tests
		dynamicconfig.FrontendGlobalNamespaceNamespaceReplicationInducingAPIsRPS: 1000,
		dynamicconfig.FrontendEnableNexusAPIs:                                    true,
	}
	s.setupSuite([]string{"nexus_outgoing_services_active", "nexus_outgoing_services_standby"})
}

func (s *NexusOutgoingServicesTestSuite) SetupTest() {
	s.setupTest()
}

func (s *NexusOutgoingServicesTestSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *NexusOutgoingServicesTestSuite) TestOutgoingServicesAreReplicatedFromActiveToStandby() {
	ctx := tests.NewContext()
	namespace := s.T().Name() + "-" + common.GenerateRandomString(5)
	activeFrontendClient := s.cluster1.GetFrontendClient()
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         s.clusterReplicationConfig(),
		ActiveClusterName:                s.clusterNames[0],
		WorkflowExecutionRetentionPeriod: durationpb.New(7 * time.Hour * 24),
	}
	_, err := activeFrontendClient.RegisterNamespace(ctx, regReq)
	s.NoError(err)
	activeOperatorClient := s.cluster1.GetOperatorClient()
	_, err = activeOperatorClient.CreateNexusOutgoingService(ctx, &operatorservice.CreateNexusOutgoingServiceRequest{
		Namespace: namespace,
		Name:      "test",
		Spec: &nexus.OutgoingServiceSpec{
			Url:               "http://dont-care",
			PublicCallbackUrl: "http://dont-care/callback",
		},
	})
	s.NoError(err)

	standbyOperatorClient := s.cluster2.GetOperatorClient()
	s.EventuallyWithT(func(t *assert.CollectT) {
		response, err := standbyOperatorClient.GetNexusOutgoingService(ctx, &operatorservice.GetNexusOutgoingServiceRequest{
			Namespace: namespace,
			Name:      "test",
		})
		assert.NoError(t, err)
		assert.Equal(t, "http://dont-care", response.GetService().GetSpec().GetUrl())
	}, 15*time.Second, 500*time.Millisecond)
}
