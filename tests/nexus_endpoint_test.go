// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package tests

import (
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"

	"go.temporal.io/server/api/matchingservice/v1"
	commonnexus "go.temporal.io/server/common/nexus"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/protorequire"
)

func TestNexusEndpointsFunctionalSuite(t *testing.T) {
	t.Run("Common", func(t *testing.T) {
		s := new(CommonSuite)
		suite.Run(t, s)
	})
	t.Run("Matching", func(t *testing.T) {
		s := new(MatchingSuite)
		suite.Run(t, s)
	})
	t.Run("Operator", func(t *testing.T) {
		s := new(OperatorSuite)
		suite.Run(t, s)
	})
}

type NexusEndpointFunctionalSuite struct {
	FunctionalTestBase
	// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
	// not merely log an error
	*require.Assertions
	protorequire.ProtoAssertions
}

func (s *NexusEndpointFunctionalSuite) SetupSuite() {
	s.setupSuite("testdata/es_cluster.yaml")
}

func (s *NexusEndpointFunctionalSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *NexusEndpointFunctionalSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
	s.ProtoAssertions = protorequire.New(s.T())
}

type CommonSuite struct {
	NexusEndpointFunctionalSuite
}

func (s *CommonSuite) TestListOrdering() {
	// get initial table version since it has been modified by other tests
	resp, err := s.testCluster.GetMatchingClient().ListNexusEndpoints(NewContext(), &matchingservice.ListNexusEndpointsRequest{
		LastKnownTableVersion: 0,
		PageSize:              0,
	})
	s.NoError(err)
	initialTableVersion := resp.TableVersion

	// create some endpoints
	numEndpoints := 40 // minimum number of endpoints to test, there may be more in DB from other tests
	for i := 0; i < numEndpoints; i++ {
		s.createNexusEndpoint(s.randomizeStr("test-endpoint-name"))
	}
	tableVersion := initialTableVersion + int64(numEndpoints)

	// list from persistence manager level
	persistence := s.testCluster.testBase.NexusEndpointManager
	persistenceResp1, err := persistence.ListNexusEndpoints(NewContext(), &p.ListNexusEndpointsRequest{
		LastKnownTableVersion: tableVersion,
		PageSize:              numEndpoints / 2,
	})
	s.NoError(err)
	s.Len(persistenceResp1.Entries, numEndpoints/2)
	s.NotNil(persistenceResp1.NextPageToken)
	persistenceResp2, err := persistence.ListNexusEndpoints(NewContext(), &p.ListNexusEndpointsRequest{
		LastKnownTableVersion: tableVersion,
		PageSize:              numEndpoints / 2,
		NextPageToken:         persistenceResp1.NextPageToken,
	})
	s.NoError(err)
	s.Len(persistenceResp2.Entries, numEndpoints/2)

	// list from matching level
	matchingClient := s.testCluster.GetMatchingClient()
	matchingResp1, err := matchingClient.ListNexusEndpoints(NewContext(), &matchingservice.ListNexusEndpointsRequest{
		LastKnownTableVersion: tableVersion,
		PageSize:              int32(numEndpoints / 2),
	})
	s.NoError(err)
	s.Len(matchingResp1.Endpoints, numEndpoints/2)
	s.NotNil(matchingResp1.NextPageToken)
	matchingResp2, err := matchingClient.ListNexusEndpoints(NewContext(), &matchingservice.ListNexusEndpointsRequest{
		LastKnownTableVersion: tableVersion,
		PageSize:              int32(numEndpoints / 2),
		NextPageToken:         matchingResp1.NextPageToken,
	})
	s.NoError(err)
	s.Len(matchingResp2.Endpoints, numEndpoints/2)

	// list from operator level
	operatorResp1, err := s.operatorClient.ListNexusEndpoints(NewContext(), &operatorservice.ListNexusEndpointsRequest{
		PageSize: int32(numEndpoints / 2),
	})
	s.NoError(err)
	s.Len(operatorResp1.Endpoints, numEndpoints/2)
	s.NotNil(operatorResp1.NextPageToken)
	operatorResp2, err := s.operatorClient.ListNexusEndpoints(NewContext(), &operatorservice.ListNexusEndpointsRequest{
		PageSize:      int32(numEndpoints / 2),
		NextPageToken: operatorResp1.NextPageToken,
	})
	s.NoError(err)
	s.Len(operatorResp2.Endpoints, numEndpoints/2)

	// assert list orders match
	for i := 0; i < numEndpoints/2; i++ {
		s.Equal(persistenceResp1.Entries[i].Id, matchingResp1.Endpoints[i].Id)
		s.Equal(persistenceResp2.Entries[i].Id, matchingResp2.Endpoints[i].Id)

		s.Equal(persistenceResp1.Entries[i].Id, operatorResp1.Endpoints[i].Id)
		s.Equal(persistenceResp2.Entries[i].Id, operatorResp2.Endpoints[i].Id)
	}
}

type MatchingSuite struct {
	NexusEndpointFunctionalSuite
}

func (s *MatchingSuite) TestCreate() {
	endpoint := s.createNexusEndpoint(s.T().Name())
	s.Equal(int64(1), endpoint.Version)
	s.Nil(endpoint.LastModifiedTime)
	s.NotNil(endpoint.CreatedTime)
	s.NotEmpty(endpoint.Id)
	s.Equal(endpoint.Spec.Name, s.T().Name())
	s.Equal(endpoint.Spec.Target.GetWorker().Namespace, s.namespace)
	s.Equal("/"+commonnexus.RouteDispatchNexusTaskByEndpoint.Path(endpoint.Id), endpoint.UrlPrefix)

	_, err := s.testCluster.GetMatchingClient().CreateNexusEndpoint(NewContext(), &matchingservice.CreateNexusEndpointRequest{
		Spec: &nexus.EndpointSpec{
			Name: s.T().Name(),
			Target: &nexus.EndpointTarget{
				Variant: &nexus.EndpointTarget_Worker_{
					Worker: &nexus.EndpointTarget_Worker{
						Namespace: s.namespace,
						TaskQueue: "dont-care",
					}},
			},
		},
	})
	var existsErr *serviceerror.AlreadyExists
	s.ErrorAs(err, &existsErr)
}

func (s *MatchingSuite) TestUpdate() {
	endpoint := s.createNexusEndpoint(s.T().Name())
	type testcase struct {
		name      string
		request   *matchingservice.UpdateNexusEndpointRequest
		assertion func(*matchingservice.UpdateNexusEndpointResponse, error)
	}
	testCases := []testcase{
		{
			name: "valid update",
			request: &matchingservice.UpdateNexusEndpointRequest{
				Version: 1,
				Id:      endpoint.Id,
				Spec: &nexus.EndpointSpec{
					Name: "updated name",
					Target: &nexus.EndpointTarget{
						Variant: &nexus.EndpointTarget_Worker_{
							Worker: &nexus.EndpointTarget_Worker{
								Namespace: s.namespace,
								TaskQueue: s.defaultTaskQueue().Name,
							},
						},
					},
				},
			},
			assertion: func(resp *matchingservice.UpdateNexusEndpointResponse, err error) {
				s.NoError(err)
				s.NotNil(resp.Endpoint)
				s.Equal("/"+commonnexus.RouteDispatchNexusTaskByEndpoint.Path(endpoint.Id), endpoint.UrlPrefix)
				s.Equal(int64(2), resp.Endpoint.Version)
				s.Equal("updated name", resp.Endpoint.Spec.Name)
				s.NotNil(resp.Endpoint.LastModifiedTime)
			},
		},
		{
			name: "invalid update: endpoint not found",
			request: &matchingservice.UpdateNexusEndpointRequest{
				Version: 1,
				Id:      "not-found",
				Spec: &nexus.EndpointSpec{
					Name: "updated name",
					Target: &nexus.EndpointTarget{
						Variant: &nexus.EndpointTarget_Worker_{
							Worker: &nexus.EndpointTarget_Worker{
								Namespace: s.namespace,
								TaskQueue: s.defaultTaskQueue().Name,
							},
						},
					},
				},
			},
			assertion: func(resp *matchingservice.UpdateNexusEndpointResponse, err error) {
				var notFoundErr *serviceerror.NotFound
				s.ErrorAs(err, &notFoundErr)
			},
		},
		{
			name: "invalid update: endpoint version mismatch",
			request: &matchingservice.UpdateNexusEndpointRequest{
				Version: 1,
				Id:      endpoint.Id,
				Spec: &nexus.EndpointSpec{
					Name: "updated name",
					Target: &nexus.EndpointTarget{
						Variant: &nexus.EndpointTarget_Worker_{
							Worker: &nexus.EndpointTarget_Worker{
								Namespace: s.namespace,
								TaskQueue: s.defaultTaskQueue().Name,
							},
						},
					},
				},
			},
			assertion: func(resp *matchingservice.UpdateNexusEndpointResponse, err error) {
				var fpErr *serviceerror.FailedPrecondition
				s.ErrorAs(err, &fpErr)
			},
		},
	}

	matchingClient := s.testCluster.GetMatchingClient()
	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			resp, err := matchingClient.UpdateNexusEndpoint(NewContext(), tc.request)
			tc.assertion(resp, err)
		})
	}
}

func (s *MatchingSuite) TestDelete() {
	endpoint := s.createNexusEndpoint("endpoint-to-delete-matching")
	type testcase struct {
		name       string
		endpointID string
		assertion  func(*matchingservice.DeleteNexusEndpointResponse, error)
	}
	testCases := []testcase{
		{
			name:       "invalid delete: not found",
			endpointID: "missing-endpoint",
			assertion: func(resp *matchingservice.DeleteNexusEndpointResponse, err error) {
				var notFoundErr *serviceerror.NotFound
				s.ErrorAs(err, &notFoundErr)
			},
		},
		{
			name:       "valid delete",
			endpointID: endpoint.Id,
			assertion: func(resp *matchingservice.DeleteNexusEndpointResponse, err error) {
				s.NoError(err)
			},
		},
	}

	matchingClient := s.testCluster.GetMatchingClient()
	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			resp, err := matchingClient.DeleteNexusEndpoint(
				NewContext(),
				&matchingservice.DeleteNexusEndpointRequest{
					Id: tc.endpointID,
				})
			tc.assertion(resp, err)
		})
	}
}

func (s *MatchingSuite) TestList() {
	// initialize some endpoints
	s.createNexusEndpoint("list-test-endpoint0")
	s.createNexusEndpoint("list-test-endpoint1")
	s.createNexusEndpoint("list-test-endpoint2")

	// get expected table version and endpoints for the course of the tests
	matchingClient := s.testCluster.GetMatchingClient()
	resp, err := matchingClient.ListNexusEndpoints(
		NewContext(),
		&matchingservice.ListNexusEndpointsRequest{
			PageSize:              100,
			LastKnownTableVersion: 0,
			Wait:                  false,
		})
	s.NoError(err)
	s.NotNil(resp)
	tableVersion := resp.TableVersion
	endpointsOrdered := resp.Endpoints
	nextPageToken := []byte(endpointsOrdered[2].Id)

	type testcase struct {
		name      string
		request   *matchingservice.ListNexusEndpointsRequest
		assertion func(*matchingservice.ListNexusEndpointsResponse, error)
	}
	testCases := []testcase{
		{
			name: "list nexus endpoints: first_page=true | wait=false | table_version=unknown",
			request: &matchingservice.ListNexusEndpointsRequest{
				NextPageToken:         nil,
				LastKnownTableVersion: 0,
				Wait:                  false,
				PageSize:              2,
			},
			assertion: func(resp *matchingservice.ListNexusEndpointsResponse, err error) {
				s.NoError(err)
				s.Equal(tableVersion, resp.TableVersion)
				s.Equal([]byte(endpointsOrdered[2].Id), resp.NextPageToken)
				s.ProtoElementsMatch(resp.Endpoints, endpointsOrdered[0:2])
			},
		},
		{
			name: "list nexus endpoints: first_page=true | wait=true | table_version=unknown",
			request: &matchingservice.ListNexusEndpointsRequest{
				NextPageToken:         nil,
				LastKnownTableVersion: 0,
				Wait:                  true,
				PageSize:              3,
			},
			assertion: func(resp *matchingservice.ListNexusEndpointsResponse, err error) {
				s.NoError(err)
				s.Equal(tableVersion, resp.TableVersion)
				s.ProtoElementsMatch(resp.Endpoints, endpointsOrdered[0:3])
			},
		},
		{
			name: "list nexus endpoints: first_page=false | wait=false | table_version=greater",
			request: &matchingservice.ListNexusEndpointsRequest{
				NextPageToken:         nextPageToken,
				LastKnownTableVersion: tableVersion + 1,
				Wait:                  false,
				PageSize:              2,
			},
			assertion: func(resp *matchingservice.ListNexusEndpointsResponse, err error) {
				var failedPreErr *serviceerror.FailedPrecondition
				s.ErrorAs(err, &failedPreErr)
			},
		},
		{
			name: "list nexus endpoints: first_page=false | wait=false | table_version=lesser",
			request: &matchingservice.ListNexusEndpointsRequest{
				NextPageToken:         nextPageToken,
				LastKnownTableVersion: tableVersion - 1,
				Wait:                  false,
				PageSize:              2,
			},
			assertion: func(resp *matchingservice.ListNexusEndpointsResponse, err error) {
				var failedPreErr *serviceerror.FailedPrecondition
				s.ErrorAs(err, &failedPreErr)
			},
		},
		{
			name: "list nexus endpoints: first_page=false | wait=false | table_version=expected",
			request: &matchingservice.ListNexusEndpointsRequest{
				NextPageToken:         nextPageToken,
				LastKnownTableVersion: tableVersion,
				Wait:                  false,
				PageSize:              2,
			},
			assertion: func(resp *matchingservice.ListNexusEndpointsResponse, err error) {
				s.NoError(err)
				s.Equal(tableVersion, resp.TableVersion)
				s.ProtoEqual(resp.Endpoints[0], endpointsOrdered[2])
			},
		},
		{
			name: "list nexus endpoints: first_page=false | wait=true | table_version=expected",
			request: &matchingservice.ListNexusEndpointsRequest{
				NextPageToken:         nextPageToken,
				LastKnownTableVersion: tableVersion,
				Wait:                  true,
				PageSize:              2,
			},
			assertion: func(resp *matchingservice.ListNexusEndpointsResponse, err error) {
				var invalidErr *serviceerror.InvalidArgument
				s.ErrorAs(err, &invalidErr)
			},
		},
		{
			name: "list nexus endpoints: first_page=true | wait=true | table_version=expected",
			request: &matchingservice.ListNexusEndpointsRequest{
				NextPageToken:         nil,
				LastKnownTableVersion: tableVersion,
				Wait:                  true,
				PageSize:              3,
			},
			assertion: func(resp *matchingservice.ListNexusEndpointsResponse, err error) {
				s.NoError(err)
				s.Equal(tableVersion+1, resp.TableVersion)
				s.NotNil(resp.NextPageToken)
				s.Len(resp.Endpoints, 3)
			},
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			listReqDone := make(chan struct{})
			go func() {
				defer close(listReqDone)
				resp, err := matchingClient.ListNexusEndpoints(NewContext(), tc.request)
				tc.assertion(resp, err)
			}()
			if tc.request.Wait && tc.request.NextPageToken == nil && tc.request.LastKnownTableVersion != 0 {
				s.createNexusEndpoint("new-endpoint")
			}
			<-listReqDone
		})
	}
}

type OperatorSuite struct {
	NexusEndpointFunctionalSuite
}

func (s *OperatorSuite) TestCreate() {
	type testcase struct {
		name      string
		request   *operatorservice.CreateNexusEndpointRequest
		assertion func(*operatorservice.CreateNexusEndpointResponse, error)
	}
	testCases := []testcase{
		{
			name: "valid create",
			request: &operatorservice.CreateNexusEndpointRequest{
				Spec: &nexus.EndpointSpec{
					Name: s.T().Name(),
					Target: &nexus.EndpointTarget{
						Variant: &nexus.EndpointTarget_Worker_{
							Worker: &nexus.EndpointTarget_Worker{
								Namespace: s.namespace,
								TaskQueue: s.defaultTaskQueue().Name,
							},
						},
					},
				},
			},
			assertion: func(resp *operatorservice.CreateNexusEndpointResponse, err error) {
				s.NotNil(resp.Endpoint)
				s.Equal(int64(1), resp.Endpoint.Version)
				s.Nil(resp.Endpoint.LastModifiedTime)
				s.NotNil(resp.Endpoint.CreatedTime)
				s.NotEmpty(resp.Endpoint.Id)
				s.Equal(resp.Endpoint.Spec.Name, s.T().Name())
				s.Equal(resp.Endpoint.Spec.Target.GetWorker().Namespace, s.namespace)
				s.Equal("/"+commonnexus.RouteDispatchNexusTaskByEndpoint.Path(resp.Endpoint.Id), resp.Endpoint.UrlPrefix)
			},
		},
		{
			name: "invalid: name already in use",
			request: &operatorservice.CreateNexusEndpointRequest{
				Spec: &nexus.EndpointSpec{
					Name: s.T().Name(),
					Target: &nexus.EndpointTarget{
						Variant: &nexus.EndpointTarget_Worker_{
							Worker: &nexus.EndpointTarget_Worker{
								Namespace: s.namespace,
								TaskQueue: s.defaultTaskQueue().Name,
							},
						},
					},
				},
			},
			assertion: func(resp *operatorservice.CreateNexusEndpointResponse, err error) {
				var existsErr *serviceerror.AlreadyExists
				s.ErrorAs(err, &existsErr)
			},
		},
		{
			name: "invalid: name unset",
			request: &operatorservice.CreateNexusEndpointRequest{
				Spec: &nexus.EndpointSpec{
					Target: &nexus.EndpointTarget{
						Variant: &nexus.EndpointTarget_Worker_{
							Worker: &nexus.EndpointTarget_Worker{
								Namespace: s.namespace,
								TaskQueue: s.defaultTaskQueue().Name,
							},
						},
					},
				},
			},
			assertion: func(resp *operatorservice.CreateNexusEndpointResponse, err error) {
				s.ErrorAs(err, new(*serviceerror.InvalidArgument))
				s.ErrorContains(err, "endpoint name not set")
			},
		},
		{
			name: "invalid: name too long",
			request: &operatorservice.CreateNexusEndpointRequest{
				Spec: &nexus.EndpointSpec{
					Name: string(make([]byte, 300)),
					Target: &nexus.EndpointTarget{
						Variant: &nexus.EndpointTarget_Worker_{
							Worker: &nexus.EndpointTarget_Worker{
								Namespace: s.namespace,
								TaskQueue: s.defaultTaskQueue().Name,
							},
						},
					},
				},
			},
			assertion: func(resp *operatorservice.CreateNexusEndpointResponse, err error) {
				s.ErrorAs(err, new(*serviceerror.InvalidArgument))
				s.ErrorContains(err, "endpoint name exceeds length limit")
			},
		},
		{
			name: "invalid: malformed name",
			request: &operatorservice.CreateNexusEndpointRequest{
				Spec: &nexus.EndpointSpec{
					Name: "\n```\n",
					Target: &nexus.EndpointTarget{
						Variant: &nexus.EndpointTarget_Worker_{
							Worker: &nexus.EndpointTarget_Worker{
								Namespace: s.namespace,
								TaskQueue: s.defaultTaskQueue().Name,
							},
						},
					},
				},
			},
			assertion: func(resp *operatorservice.CreateNexusEndpointResponse, err error) {
				s.ErrorAs(err, new(*serviceerror.InvalidArgument))
				s.ErrorContains(err, "endpoint name must match the regex")
			},
		},
		{
			name: "invalid: namespace unset",
			request: &operatorservice.CreateNexusEndpointRequest{
				Spec: &nexus.EndpointSpec{
					Name: s.randomizeStr(s.T().Name()),
					Target: &nexus.EndpointTarget{
						Variant: &nexus.EndpointTarget_Worker_{
							Worker: &nexus.EndpointTarget_Worker{
								TaskQueue: s.defaultTaskQueue().Name,
							},
						},
					},
				},
			},
			assertion: func(resp *operatorservice.CreateNexusEndpointResponse, err error) {
				s.ErrorAs(err, new(*serviceerror.InvalidArgument))
				s.ErrorContains(err, "target namespace not set")
			},
		},
		{
			name: "invalid: namespace not found",
			request: &operatorservice.CreateNexusEndpointRequest{
				Spec: &nexus.EndpointSpec{
					Name: s.randomizeStr(s.T().Name()),
					Target: &nexus.EndpointTarget{
						Variant: &nexus.EndpointTarget_Worker_{
							Worker: &nexus.EndpointTarget_Worker{
								Namespace: "missing-namespace",
								TaskQueue: s.defaultTaskQueue().Name,
							},
						},
					},
				},
			},
			assertion: func(resp *operatorservice.CreateNexusEndpointResponse, err error) {
				var preCondErr *serviceerror.FailedPrecondition
				s.ErrorAs(err, &preCondErr)
			},
		},
		{
			name: "invalid: task queue unset",
			request: &operatorservice.CreateNexusEndpointRequest{
				Spec: &nexus.EndpointSpec{
					Name: s.randomizeStr(s.T().Name()),
					Target: &nexus.EndpointTarget{
						Variant: &nexus.EndpointTarget_Worker_{
							Worker: &nexus.EndpointTarget_Worker{
								Namespace: s.namespace,
							},
						},
					},
				},
			},
			assertion: func(resp *operatorservice.CreateNexusEndpointResponse, err error) {
				s.ErrorAs(err, new(*serviceerror.InvalidArgument))
				s.ErrorContains(err, "TaskQueue is not set on request")
			},
		},
		{
			name: "invalid: task queue too long",
			request: &operatorservice.CreateNexusEndpointRequest{
				Spec: &nexus.EndpointSpec{
					Name: s.randomizeStr(s.T().Name()),
					Target: &nexus.EndpointTarget{
						Variant: &nexus.EndpointTarget_Worker_{
							Worker: &nexus.EndpointTarget_Worker{
								Namespace: s.namespace,
								TaskQueue: string(make([]byte, 1005)),
							},
						},
					},
				},
			},
			assertion: func(resp *operatorservice.CreateNexusEndpointResponse, err error) {
				s.ErrorAs(err, new(*serviceerror.InvalidArgument))
				s.ErrorContains(err, "TaskQueue length exceeds limit")
			},
		},
		{
			name: "invalid: empty URL",
			request: &operatorservice.CreateNexusEndpointRequest{
				Spec: &nexus.EndpointSpec{
					Name: s.randomizeStr(s.T().Name()),
					Target: &nexus.EndpointTarget{
						Variant: &nexus.EndpointTarget_External_{
							External: &nexus.EndpointTarget_External{},
						},
					},
				},
			},
			assertion: func(resp *operatorservice.CreateNexusEndpointResponse, err error) {
				s.ErrorAs(err, new(*serviceerror.InvalidArgument))
				s.ErrorContains(err, "empty target URL")
			},
		},
		{
			name: "invalid: URL too long",
			request: &operatorservice.CreateNexusEndpointRequest{
				Spec: &nexus.EndpointSpec{
					Name: s.randomizeStr(s.T().Name()),
					Target: &nexus.EndpointTarget{
						Variant: &nexus.EndpointTarget_External_{
							External: &nexus.EndpointTarget_External{
								Url: "http://foo/" + strings.Repeat("pattern", 4096/len("pattern")),
							},
						},
					},
				},
			},
			assertion: func(resp *operatorservice.CreateNexusEndpointResponse, err error) {
				s.ErrorAs(err, new(*serviceerror.InvalidArgument))
				s.ErrorContains(err, "URL length exceeds limit")
			},
		},
		{
			name: "invalid: URL invalid",
			request: &operatorservice.CreateNexusEndpointRequest{
				Spec: &nexus.EndpointSpec{
					Name: s.randomizeStr(s.T().Name()),
					Target: &nexus.EndpointTarget{
						Variant: &nexus.EndpointTarget_External_{
							External: &nexus.EndpointTarget_External{
								Url: "-http://foo",
							},
						},
					},
				},
			},
			assertion: func(resp *operatorservice.CreateNexusEndpointResponse, err error) {
				s.ErrorAs(err, new(*serviceerror.InvalidArgument))
				s.ErrorContains(err, "invalid target URL: parse")
			},
		},
		{
			name: "invalid: URL invalid scheme",
			request: &operatorservice.CreateNexusEndpointRequest{
				Spec: &nexus.EndpointSpec{
					Name: s.randomizeStr(s.T().Name()),
					Target: &nexus.EndpointTarget{
						Variant: &nexus.EndpointTarget_External_{
							External: &nexus.EndpointTarget_External{
								Url: "smtp://foo",
							},
						},
					},
				},
			},
			assertion: func(resp *operatorservice.CreateNexusEndpointResponse, err error) {
				s.ErrorAs(err, new(*serviceerror.InvalidArgument))
				s.ErrorContains(err, "invalid target URL scheme:")
			},
		},
		{
			name: "invalid: description too large",
			request: &operatorservice.CreateNexusEndpointRequest{
				Spec: &nexus.EndpointSpec{
					Name: s.randomizeStr(s.T().Name()),
					Target: &nexus.EndpointTarget{
						Variant: &nexus.EndpointTarget_Worker_{
							Worker: &nexus.EndpointTarget_Worker{
								Namespace: s.namespace,
								TaskQueue: s.defaultTaskQueue().Name,
							},
						},
					},
					Description: &common.Payload{
						Data: make([]byte, 4100),
					},
				},
			},
			assertion: func(resp *operatorservice.CreateNexusEndpointResponse, err error) {
				s.ErrorAs(err, new(*serviceerror.InvalidArgument))
				s.ErrorContains(err, "description length exceeds limit of 4096")
			},
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			resp, err := s.operatorClient.CreateNexusEndpoint(NewContext(), tc.request)
			tc.assertion(resp, err)
		})
	}
}

func (s *OperatorSuite) TestUpdate() {
	endpoint := s.createNexusEndpoint(s.T().Name())
	type testcase struct {
		name      string
		request   *operatorservice.UpdateNexusEndpointRequest
		assertion func(*operatorservice.UpdateNexusEndpointResponse, error)
	}
	testCases := []testcase{
		{
			name: "valid update",
			request: &operatorservice.UpdateNexusEndpointRequest{
				Version: 1,
				Id:      endpoint.Id,
				Spec: &nexus.EndpointSpec{
					Name: "updated name",
					Target: &nexus.EndpointTarget{
						Variant: &nexus.EndpointTarget_Worker_{
							Worker: &nexus.EndpointTarget_Worker{
								Namespace: s.namespace,
								TaskQueue: s.defaultTaskQueue().Name,
							},
						},
					},
				},
			},
			assertion: func(resp *operatorservice.UpdateNexusEndpointResponse, err error) {
				s.NoError(err)
				s.NotNil(resp.Endpoint)
				s.Equal("/"+commonnexus.RouteDispatchNexusTaskByEndpoint.Path(endpoint.Id), endpoint.UrlPrefix)
				s.Equal(int64(2), resp.Endpoint.Version)
				s.Equal("updated name", resp.Endpoint.Spec.Name)
				s.NotNil(resp.Endpoint.LastModifiedTime)
			},
		},
		{
			name: "invalid: endpoint not found",
			request: &operatorservice.UpdateNexusEndpointRequest{
				Version: 1,
				Id:      "not-found",
				Spec: &nexus.EndpointSpec{
					Name: "updated name",
					Target: &nexus.EndpointTarget{
						Variant: &nexus.EndpointTarget_Worker_{
							Worker: &nexus.EndpointTarget_Worker{
								Namespace: s.namespace,
								TaskQueue: s.defaultTaskQueue().Name,
							},
						},
					},
				},
			},
			assertion: func(resp *operatorservice.UpdateNexusEndpointResponse, err error) {
				var notFoundErr *serviceerror.NotFound
				s.ErrorAs(err, &notFoundErr)
			},
		},
		{
			name: "invalid: endpoint version mismatch",
			request: &operatorservice.UpdateNexusEndpointRequest{
				Version: 1,
				Id:      endpoint.Id,
				Spec: &nexus.EndpointSpec{
					Name: "updated name",
					Target: &nexus.EndpointTarget{
						Variant: &nexus.EndpointTarget_Worker_{
							Worker: &nexus.EndpointTarget_Worker{
								Namespace: s.namespace,
								TaskQueue: s.defaultTaskQueue().Name,
							},
						},
					},
				},
			},
			assertion: func(resp *operatorservice.UpdateNexusEndpointResponse, err error) {
				var fpErr *serviceerror.FailedPrecondition
				s.ErrorAs(err, &fpErr)
			},
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			resp, err := s.operatorClient.UpdateNexusEndpoint(NewContext(), tc.request)
			tc.assertion(resp, err)
		})
	}
}

func (s *OperatorSuite) TestDelete() {
	endpoint := s.createNexusEndpoint("endpoint-to-delete-operator")
	type testcase struct {
		name      string
		serviceId string
		assertion func(*operatorservice.DeleteNexusEndpointResponse, error)
	}
	testCases := []testcase{
		{
			name:      "invalid delete: not found",
			serviceId: uuid.NewString(),
			assertion: func(resp *operatorservice.DeleteNexusEndpointResponse, err error) {
				var notFoundErr *serviceerror.NotFound
				s.ErrorAs(err, &notFoundErr)
			},
		},
		{
			name:      "valid delete",
			serviceId: endpoint.Id,
			assertion: func(resp *operatorservice.DeleteNexusEndpointResponse, err error) {
				s.NoError(err)
			},
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			resp, err := s.operatorClient.DeleteNexusEndpoint(
				NewContext(),
				&operatorservice.DeleteNexusEndpointRequest{
					Id:      tc.serviceId,
					Version: 1,
				})
			tc.assertion(resp, err)
		})
	}
}

func (s *OperatorSuite) TestList() {
	// initialize some endpoints
	s.createNexusEndpoint("operator-list-test-service0")
	s.createNexusEndpoint("operator-list-test-service1")
	serviceToFilter := s.createNexusEndpoint("operator-list-test-service2")

	// get ordered endpoints for the course of the tests
	resp, err := s.operatorClient.ListNexusEndpoints(NewContext(), &operatorservice.ListNexusEndpointsRequest{})
	s.NoError(err)
	s.NotNil(resp)
	endpointsOrdered := resp.Endpoints

	resp, err = s.operatorClient.ListNexusEndpoints(NewContext(), &operatorservice.ListNexusEndpointsRequest{PageSize: 2})
	s.NoError(err)
	s.NotNil(resp)
	nextPageToken := resp.NextPageToken

	type testcase struct {
		name      string
		request   *operatorservice.ListNexusEndpointsRequest
		assertion func(*operatorservice.ListNexusEndpointsResponse, error)
	}
	testCases := []testcase{
		{
			name: "list first page",
			request: &operatorservice.ListNexusEndpointsRequest{
				NextPageToken: nil,
				PageSize:      2,
			},
			assertion: func(resp *operatorservice.ListNexusEndpointsResponse, err error) {
				s.NoError(err)
				s.Equal(nextPageToken, resp.NextPageToken)
				s.ProtoElementsMatch(resp.Endpoints, endpointsOrdered[0:2])
			},
		},
		{
			name: "list non-first page",
			request: &operatorservice.ListNexusEndpointsRequest{
				NextPageToken: nextPageToken,
				PageSize:      2,
			},
			assertion: func(resp *operatorservice.ListNexusEndpointsResponse, err error) {
				s.NoError(err)
				s.ProtoEqual(resp.Endpoints[0], endpointsOrdered[2])
			},
		},
		{
			name:    "list with no page size",
			request: &operatorservice.ListNexusEndpointsRequest{},
			assertion: func(resp *operatorservice.ListNexusEndpointsResponse, err error) {
				s.NoError(err)
				s.NotEmpty(resp.Endpoints)
			},
		},
		{
			name: "list with filter found",
			request: &operatorservice.ListNexusEndpointsRequest{
				NextPageToken: nil,
				PageSize:      2,
				Name:          serviceToFilter.Spec.Name,
			},
			assertion: func(resp *operatorservice.ListNexusEndpointsResponse, err error) {
				s.NoError(err)
				s.Nil(resp.NextPageToken)
				s.Len(resp.Endpoints, 1)
				s.ProtoEqual(resp.Endpoints[0], serviceToFilter)
			},
		},
		{
			name: "list with filter not found",
			request: &operatorservice.ListNexusEndpointsRequest{
				NextPageToken: nil,
				PageSize:      2,
				Name:          "missing-endpoint",
			},
			assertion: func(resp *operatorservice.ListNexusEndpointsResponse, err error) {
				s.NoError(err)
				s.Nil(resp.NextPageToken)
				s.Empty(resp.Endpoints)
			},
		},
		{
			name: "list with page size too large",
			request: &operatorservice.ListNexusEndpointsRequest{
				NextPageToken: nil,
				PageSize:      1005,
			},
			assertion: func(resp *operatorservice.ListNexusEndpointsResponse, err error) {
				var invalidErr *serviceerror.InvalidArgument
				s.ErrorAs(err, &invalidErr)
			},
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			resp, err := s.operatorClient.ListNexusEndpoints(NewContext(), tc.request)
			tc.assertion(resp, err)
		})
	}
}

func (s *OperatorSuite) TestGet() {
	endpoint := s.createNexusEndpoint(s.T().Name())

	type testcase struct {
		name      string
		request   *operatorservice.GetNexusEndpointRequest
		assertion func(*operatorservice.GetNexusEndpointResponse, error)
	}
	testCases := []testcase{
		{
			name: "valid get",
			request: &operatorservice.GetNexusEndpointRequest{
				Id: endpoint.Id,
			},
			assertion: func(response *operatorservice.GetNexusEndpointResponse, err error) {
				s.NoError(err)
				s.ProtoEqual(endpoint, response.Endpoint)
			},
		},
		{
			name: "invalid: missing endpoint",
			request: &operatorservice.GetNexusEndpointRequest{
				Id: uuid.NewString(),
			},
			assertion: func(response *operatorservice.GetNexusEndpointResponse, err error) {
				var notFoundErr *serviceerror.NotFound
				s.ErrorAs(err, &notFoundErr)
			},
		},
		{
			name:    "invalid: endpoint ID not set",
			request: &operatorservice.GetNexusEndpointRequest{},
			assertion: func(response *operatorservice.GetNexusEndpointResponse, err error) {
				s.ErrorContains(err, "endpoint ID not set")
			},
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			resp, err := s.operatorClient.GetNexusEndpoint(NewContext(), tc.request)
			tc.assertion(resp, err)
		})
	}
}

func (s *NexusEndpointFunctionalSuite) defaultTaskQueue() *taskqueuepb.TaskQueue {
	name := fmt.Sprintf("functional-queue-%v", s.T().Name())
	return &taskqueuepb.TaskQueue{Name: name, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
}

func (s *NexusEndpointFunctionalSuite) createNexusEndpoint(name string) *nexus.Endpoint {
	resp, err := s.testCluster.GetMatchingClient().CreateNexusEndpoint(
		NewContext(),
		&matchingservice.CreateNexusEndpointRequest{
			Spec: &nexus.EndpointSpec{
				Name: name,
				Target: &nexus.EndpointTarget{
					Variant: &nexus.EndpointTarget_Worker_{
						Worker: &nexus.EndpointTarget_Worker{
							Namespace: s.namespace,
							TaskQueue: s.defaultTaskQueue().Name,
						},
					},
				},
			},
		})

	s.NoError(err)
	s.NotNil(resp.Endpoint)
	return resp.Endpoint
}
