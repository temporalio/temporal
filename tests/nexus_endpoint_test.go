package tests

import (
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	commonnexus "go.temporal.io/server/common/nexus"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/tests/testcore"
)

func TestNexusEndpointsFunctionalSuite(t *testing.T) {
	t.Parallel()
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
	testcore.FunctionalTestBase
}

type CommonSuite struct {
	NexusEndpointFunctionalSuite
}

func (s *CommonSuite) TestListOrdering() {
	// get initial table version since it has been modified by other tests
	resp, err := s.GetTestCluster().MatchingClient().ListNexusEndpoints(testcore.NewContext(), matchingservice.ListNexusEndpointsRequest_builder{
		LastKnownTableVersion: 0,
		PageSize:              0,
	}.Build())
	s.NoError(err)
	initialTableVersion := resp.GetTableVersion()

	// create some endpoints
	numEndpoints := 40 // minimum number of endpoints to test, there may be more in DB from other tests
	for i := 0; i < numEndpoints; i++ {
		s.createNexusEndpoint(testcore.RandomizeStr("test-endpoint-name"))
	}
	tableVersion := initialTableVersion + int64(numEndpoints)

	// list from persistence manager level
	persistence := s.GetTestCluster().TestBase().NexusEndpointManager
	persistenceResp1, err := persistence.ListNexusEndpoints(testcore.NewContext(), &p.ListNexusEndpointsRequest{
		LastKnownTableVersion: tableVersion,
		PageSize:              numEndpoints / 2,
	})
	s.NoError(err)
	s.Len(persistenceResp1.Entries, numEndpoints/2)
	s.NotNil(persistenceResp1.NextPageToken)
	persistenceResp2, err := persistence.ListNexusEndpoints(testcore.NewContext(), &p.ListNexusEndpointsRequest{
		LastKnownTableVersion: tableVersion,
		PageSize:              numEndpoints / 2,
		NextPageToken:         persistenceResp1.NextPageToken,
	})
	s.NoError(err)
	s.Len(persistenceResp2.Entries, numEndpoints/2)

	// list from matching level
	matchingClient := s.GetTestCluster().MatchingClient()
	matchingResp1, err := matchingClient.ListNexusEndpoints(testcore.NewContext(), matchingservice.ListNexusEndpointsRequest_builder{
		LastKnownTableVersion: tableVersion,
		PageSize:              int32(numEndpoints / 2),
	}.Build())
	s.NoError(err)
	s.Len(matchingResp1.GetEntries(), numEndpoints/2)
	s.NotNil(matchingResp1.GetNextPageToken())
	matchingResp2, err := matchingClient.ListNexusEndpoints(testcore.NewContext(), matchingservice.ListNexusEndpointsRequest_builder{
		LastKnownTableVersion: tableVersion,
		PageSize:              int32(numEndpoints / 2),
		NextPageToken:         matchingResp1.GetNextPageToken(),
	}.Build())
	s.NoError(err)
	s.Len(matchingResp2.GetEntries(), numEndpoints/2)

	// list from operator level
	operatorResp1, err := s.OperatorClient().ListNexusEndpoints(testcore.NewContext(), operatorservice.ListNexusEndpointsRequest_builder{
		PageSize: int32(numEndpoints / 2),
	}.Build())
	s.NoError(err)
	s.Len(operatorResp1.GetEndpoints(), numEndpoints/2)
	s.NotNil(operatorResp1.GetNextPageToken())
	operatorResp2, err := s.OperatorClient().ListNexusEndpoints(testcore.NewContext(), operatorservice.ListNexusEndpointsRequest_builder{
		PageSize:      int32(numEndpoints / 2),
		NextPageToken: operatorResp1.GetNextPageToken(),
	}.Build())
	s.NoError(err)
	s.Len(operatorResp2.GetEndpoints(), numEndpoints/2)

	// assert list orders match
	for i := 0; i < numEndpoints/2; i++ {
		s.Equal(persistenceResp1.Entries[i].GetId(), matchingResp1.GetEntries()[i].GetId())
		s.Equal(persistenceResp2.Entries[i].GetId(), matchingResp2.GetEntries()[i].GetId())

		s.Equal(persistenceResp1.Entries[i].GetId(), operatorResp1.GetEndpoints()[i].GetId())
		s.Equal(persistenceResp2.Entries[i].GetId(), operatorResp2.GetEndpoints()[i].GetId())
	}
}

type MatchingSuite struct {
	NexusEndpointFunctionalSuite
}

func (s *MatchingSuite) TestCreate() {
	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())
	entry := s.createNexusEndpoint(endpointName)
	s.Equal(int64(1), entry.GetVersion())
	s.NotNil(entry.GetEndpoint().GetClock())
	s.NotNil(entry.GetEndpoint().GetCreatedTime())
	s.NotEmpty(entry.GetId())
	s.Equal(entry.GetEndpoint().GetSpec().GetName(), endpointName)
	s.Equal(entry.GetEndpoint().GetSpec().GetTarget().GetWorker().GetNamespaceId(), s.NamespaceID().String())

	_, err := s.GetTestCluster().MatchingClient().CreateNexusEndpoint(testcore.NewContext(), matchingservice.CreateNexusEndpointRequest_builder{
		Spec: persistencespb.NexusEndpointSpec_builder{
			Name: endpointName,
			Target: persistencespb.NexusEndpointTarget_builder{
				Worker: persistencespb.NexusEndpointTarget_Worker_builder{
					NamespaceId: s.NamespaceID().String(),
					TaskQueue:   "dont-care",
				}.Build(),
			}.Build(),
		}.Build(),
	}.Build())
	var existsErr *serviceerror.AlreadyExists
	s.ErrorAs(err, &existsErr)
}

func (s *MatchingSuite) TestUpdate() {
	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())
	updatedName := testcore.RandomizedNexusEndpoint(s.T().Name() + "-updated")
	endpoint := s.createNexusEndpoint(endpointName)
	type testcase struct {
		name      string
		request   *matchingservice.UpdateNexusEndpointRequest
		assertion func(*matchingservice.UpdateNexusEndpointResponse, error)
	}
	testCases := []testcase{
		{
			name: "valid update",
			request: matchingservice.UpdateNexusEndpointRequest_builder{
				Version: 1,
				Id:      endpoint.GetId(),
				Spec: persistencespb.NexusEndpointSpec_builder{
					Name: updatedName,
					Target: persistencespb.NexusEndpointTarget_builder{
						Worker: persistencespb.NexusEndpointTarget_Worker_builder{
							NamespaceId: s.NamespaceID().String(),
							TaskQueue:   s.defaultTaskQueue().GetName(),
						}.Build(),
					}.Build(),
				}.Build(),
			}.Build(),
			assertion: func(resp *matchingservice.UpdateNexusEndpointResponse, err error) {
				s.NoError(err)
				s.NotNil(resp.GetEntry())
				s.Equal(int64(2), resp.GetEntry().GetVersion())
				s.Equal(updatedName, resp.GetEntry().GetEndpoint().GetSpec().GetName())
				s.NotNil(resp.GetEntry().GetEndpoint().GetClock())
			},
		},
		{
			name: "invalid update: endpoint not found",
			request: matchingservice.UpdateNexusEndpointRequest_builder{
				Version: 1,
				Id:      "not-found",
				Spec: persistencespb.NexusEndpointSpec_builder{
					Name: updatedName,
					Target: persistencespb.NexusEndpointTarget_builder{
						Worker: persistencespb.NexusEndpointTarget_Worker_builder{
							NamespaceId: s.NamespaceID().String(),
							TaskQueue:   s.defaultTaskQueue().GetName(),
						}.Build(),
					}.Build(),
				}.Build(),
			}.Build(),
			assertion: func(resp *matchingservice.UpdateNexusEndpointResponse, err error) {
				var notFoundErr *serviceerror.NotFound
				s.ErrorAs(err, &notFoundErr)
			},
		},
		{
			name: "invalid update: endpoint version mismatch",
			request: matchingservice.UpdateNexusEndpointRequest_builder{
				Version: 1,
				Id:      endpoint.GetId(),
				Spec: persistencespb.NexusEndpointSpec_builder{
					Name: updatedName,
					Target: persistencespb.NexusEndpointTarget_builder{
						Worker: persistencespb.NexusEndpointTarget_Worker_builder{
							NamespaceId: s.NamespaceID().String(),
							TaskQueue:   s.defaultTaskQueue().GetName(),
						}.Build(),
					}.Build(),
				}.Build(),
			}.Build(),
			assertion: func(resp *matchingservice.UpdateNexusEndpointResponse, err error) {
				var fpErr *serviceerror.FailedPrecondition
				s.ErrorAs(err, &fpErr)
			},
		},
	}

	matchingClient := s.GetTestCluster().MatchingClient()
	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			resp, err := matchingClient.UpdateNexusEndpoint(testcore.NewContext(), tc.request)
			tc.assertion(resp, err)
		})
	}
}

func (s *MatchingSuite) TestDelete() {
	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())
	endpoint := s.createNexusEndpoint(endpointName)
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
			endpointID: endpoint.GetId(),
			assertion: func(resp *matchingservice.DeleteNexusEndpointResponse, err error) {
				s.NoError(err)
			},
		},
	}

	matchingClient := s.GetTestCluster().MatchingClient()
	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			resp, err := matchingClient.DeleteNexusEndpoint(
				testcore.NewContext(),
				matchingservice.DeleteNexusEndpointRequest_builder{
					Id: tc.endpointID,
				}.Build())
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
	matchingClient := s.GetTestCluster().MatchingClient()
	resp, err := matchingClient.ListNexusEndpoints(
		testcore.NewContext(),
		matchingservice.ListNexusEndpointsRequest_builder{
			PageSize:              100,
			LastKnownTableVersion: 0,
			Wait:                  false,
		}.Build())
	s.NoError(err)
	s.NotNil(resp)
	tableVersion := resp.GetTableVersion()
	endpointsOrdered := resp.GetEntries()
	nextPageToken := []byte(endpointsOrdered[2].GetId())

	type testcase struct {
		name      string
		request   *matchingservice.ListNexusEndpointsRequest
		assertion func(*matchingservice.ListNexusEndpointsResponse, error)
	}
	testCases := []testcase{
		{
			name: "list nexus endpoints: first_page=true | wait=false | table_version=unknown",
			request: matchingservice.ListNexusEndpointsRequest_builder{
				NextPageToken:         nil,
				LastKnownTableVersion: 0,
				Wait:                  false,
				PageSize:              2,
			}.Build(),
			assertion: func(resp *matchingservice.ListNexusEndpointsResponse, err error) {
				s.NoError(err)
				s.Equal(tableVersion, resp.GetTableVersion())
				s.Equal([]byte(endpointsOrdered[2].GetId()), resp.GetNextPageToken())
				s.ProtoElementsMatch(resp.GetEntries(), endpointsOrdered[0:2])
			},
		},
		{
			name: "list nexus endpoints: first_page=true | wait=true | table_version=unknown",
			request: matchingservice.ListNexusEndpointsRequest_builder{
				NextPageToken:         nil,
				LastKnownTableVersion: 0,
				Wait:                  true,
				PageSize:              3,
			}.Build(),
			assertion: func(resp *matchingservice.ListNexusEndpointsResponse, err error) {
				s.NoError(err)
				s.Equal(tableVersion, resp.GetTableVersion())
				s.ProtoElementsMatch(resp.GetEntries(), endpointsOrdered[0:3])
			},
		},
		{
			name: "list nexus endpoints: first_page=false | wait=false | table_version=greater",
			request: matchingservice.ListNexusEndpointsRequest_builder{
				NextPageToken:         nextPageToken,
				LastKnownTableVersion: tableVersion + 1,
				Wait:                  false,
				PageSize:              2,
			}.Build(),
			assertion: func(resp *matchingservice.ListNexusEndpointsResponse, err error) {
				var failedPreErr *serviceerror.FailedPrecondition
				s.ErrorAs(err, &failedPreErr)
			},
		},
		{
			name: "list nexus endpoints: first_page=false | wait=false | table_version=lesser",
			request: matchingservice.ListNexusEndpointsRequest_builder{
				NextPageToken:         nextPageToken,
				LastKnownTableVersion: tableVersion - 1,
				Wait:                  false,
				PageSize:              2,
			}.Build(),
			assertion: func(resp *matchingservice.ListNexusEndpointsResponse, err error) {
				var failedPreErr *serviceerror.FailedPrecondition
				s.ErrorAs(err, &failedPreErr)
			},
		},
		{
			name: "list nexus endpoints: first_page=false | wait=false | table_version=expected",
			request: matchingservice.ListNexusEndpointsRequest_builder{
				NextPageToken:         nextPageToken,
				LastKnownTableVersion: tableVersion,
				Wait:                  false,
				PageSize:              2,
			}.Build(),
			assertion: func(resp *matchingservice.ListNexusEndpointsResponse, err error) {
				s.NoError(err)
				s.Equal(tableVersion, resp.GetTableVersion())
				s.ProtoEqual(resp.GetEntries()[0], endpointsOrdered[2])
			},
		},
		{
			name: "list nexus endpoints: first_page=false | wait=true | table_version=expected",
			request: matchingservice.ListNexusEndpointsRequest_builder{
				NextPageToken:         nextPageToken,
				LastKnownTableVersion: tableVersion,
				Wait:                  true,
				PageSize:              2,
			}.Build(),
			assertion: func(resp *matchingservice.ListNexusEndpointsResponse, err error) {
				var invalidErr *serviceerror.InvalidArgument
				s.ErrorAs(err, &invalidErr)
			},
		},
		{
			name: "list nexus endpoints: first_page=true | wait=true | table_version=expected",
			request: matchingservice.ListNexusEndpointsRequest_builder{
				NextPageToken:         nil,
				LastKnownTableVersion: tableVersion,
				Wait:                  true,
				PageSize:              3,
			}.Build(),
			assertion: func(resp *matchingservice.ListNexusEndpointsResponse, err error) {
				s.NoError(err)
				s.Equal(tableVersion+1, resp.GetTableVersion())
				s.NotNil(resp.GetNextPageToken())
				s.Len(resp.GetEntries(), 3)
			},
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			listReqDone := make(chan struct{})
			go func() {
				defer close(listReqDone)
				resp, err := matchingClient.ListNexusEndpoints(testcore.NewContext(), tc.request) //nolint:revive
				tc.assertion(resp, err)
			}()
			if tc.request.GetWait() && len(tc.request.GetNextPageToken()) == 0 && tc.request.GetLastKnownTableVersion() != 0 {
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
	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())
	type testcase struct {
		name      string
		request   *operatorservice.CreateNexusEndpointRequest
		assertion func(*operatorservice.CreateNexusEndpointResponse, error)
	}
	testCases := []testcase{
		{
			name: "valid create",
			request: operatorservice.CreateNexusEndpointRequest_builder{
				Spec: nexuspb.EndpointSpec_builder{
					Name: endpointName,
					Target: nexuspb.EndpointTarget_builder{
						Worker: nexuspb.EndpointTarget_Worker_builder{
							Namespace: s.Namespace().String(),
							TaskQueue: s.defaultTaskQueue().GetName(),
						}.Build(),
					}.Build(),
				}.Build(),
			}.Build(),
			assertion: func(resp *operatorservice.CreateNexusEndpointResponse, err error) {
				s.NoError(err)
				s.NotNil(resp.GetEndpoint())
				s.Equal(int64(1), resp.GetEndpoint().GetVersion())
				s.Nil(resp.GetEndpoint().GetLastModifiedTime())
				s.NotNil(resp.GetEndpoint().GetCreatedTime())
				s.NotEmpty(resp.GetEndpoint().GetId())
				s.Equal(resp.GetEndpoint().GetSpec().GetName(), endpointName)
				s.Equal(resp.GetEndpoint().GetSpec().GetTarget().GetWorker().GetNamespace(), s.Namespace().String())
				s.Equal("/"+commonnexus.RouteDispatchNexusTaskByEndpoint.Path(resp.GetEndpoint().GetId()), resp.GetEndpoint().GetUrlPrefix())
			},
		},
		{
			name: "invalid: name already in use",
			request: operatorservice.CreateNexusEndpointRequest_builder{
				Spec: nexuspb.EndpointSpec_builder{
					Name: endpointName,
					Target: nexuspb.EndpointTarget_builder{
						Worker: nexuspb.EndpointTarget_Worker_builder{
							Namespace: s.Namespace().String(),
							TaskQueue: s.defaultTaskQueue().GetName(),
						}.Build(),
					}.Build(),
				}.Build(),
			}.Build(),
			assertion: func(resp *operatorservice.CreateNexusEndpointResponse, err error) {
				var existsErr *serviceerror.AlreadyExists
				s.ErrorAs(err, &existsErr)
			},
		},
		{
			name: "invalid: name unset",
			request: operatorservice.CreateNexusEndpointRequest_builder{
				Spec: nexuspb.EndpointSpec_builder{
					Target: nexuspb.EndpointTarget_builder{
						Worker: nexuspb.EndpointTarget_Worker_builder{
							Namespace: s.Namespace().String(),
							TaskQueue: s.defaultTaskQueue().GetName(),
						}.Build(),
					}.Build(),
				}.Build(),
			}.Build(),
			assertion: func(resp *operatorservice.CreateNexusEndpointResponse, err error) {
				s.ErrorAs(err, new(*serviceerror.InvalidArgument))
				s.ErrorContains(err, "endpoint name not set")
			},
		},
		{
			name: "invalid: name too long",
			request: operatorservice.CreateNexusEndpointRequest_builder{
				Spec: nexuspb.EndpointSpec_builder{
					Name: string(make([]byte, 300)),
					Target: nexuspb.EndpointTarget_builder{
						Worker: nexuspb.EndpointTarget_Worker_builder{
							Namespace: s.Namespace().String(),
							TaskQueue: s.defaultTaskQueue().GetName(),
						}.Build(),
					}.Build(),
				}.Build(),
			}.Build(),
			assertion: func(resp *operatorservice.CreateNexusEndpointResponse, err error) {
				s.ErrorAs(err, new(*serviceerror.InvalidArgument))
				s.ErrorContains(err, "endpoint name exceeds length limit")
			},
		},
		{
			name: "invalid: malformed name",
			request: operatorservice.CreateNexusEndpointRequest_builder{
				Spec: nexuspb.EndpointSpec_builder{
					Name: "test_\n```\n",
					Target: nexuspb.EndpointTarget_builder{
						Worker: nexuspb.EndpointTarget_Worker_builder{
							Namespace: s.Namespace().String(),
							TaskQueue: s.defaultTaskQueue().GetName(),
						}.Build(),
					}.Build(),
				}.Build(),
			}.Build(),
			assertion: func(resp *operatorservice.CreateNexusEndpointResponse, err error) {
				s.ErrorAs(err, new(*serviceerror.InvalidArgument))
				s.ErrorContains(err, "endpoint name must match the regex")
			},
		},
		{
			name: "invalid: namespace unset",
			request: operatorservice.CreateNexusEndpointRequest_builder{
				Spec: nexuspb.EndpointSpec_builder{
					Name: testcore.RandomizeStr(endpointName),
					Target: nexuspb.EndpointTarget_builder{
						Worker: nexuspb.EndpointTarget_Worker_builder{
							TaskQueue: s.defaultTaskQueue().GetName(),
						}.Build(),
					}.Build(),
				}.Build(),
			}.Build(),
			assertion: func(resp *operatorservice.CreateNexusEndpointResponse, err error) {
				s.ErrorAs(err, new(*serviceerror.InvalidArgument))
				s.ErrorContains(err, "target namespace not set")
			},
		},
		{
			name: "invalid: namespace not found",
			request: operatorservice.CreateNexusEndpointRequest_builder{
				Spec: nexuspb.EndpointSpec_builder{
					Name: testcore.RandomizeStr(endpointName),
					Target: nexuspb.EndpointTarget_builder{
						Worker: nexuspb.EndpointTarget_Worker_builder{
							Namespace: "missing-namespace",
							TaskQueue: s.defaultTaskQueue().GetName(),
						}.Build(),
					}.Build(),
				}.Build(),
			}.Build(),
			assertion: func(resp *operatorservice.CreateNexusEndpointResponse, err error) {
				var preCondErr *serviceerror.FailedPrecondition
				s.ErrorAs(err, &preCondErr)
			},
		},
		{
			name: "invalid: task queue unset",
			request: operatorservice.CreateNexusEndpointRequest_builder{
				Spec: nexuspb.EndpointSpec_builder{
					Name: testcore.RandomizeStr(endpointName),
					Target: nexuspb.EndpointTarget_builder{
						Worker: nexuspb.EndpointTarget_Worker_builder{
							Namespace: s.Namespace().String(),
						}.Build(),
					}.Build(),
				}.Build(),
			}.Build(),
			assertion: func(resp *operatorservice.CreateNexusEndpointResponse, err error) {
				s.ErrorAs(err, new(*serviceerror.InvalidArgument))
				s.ErrorContains(err, "taskQueue is not set")
			},
		},
		{
			name: "invalid: task queue too long",
			request: operatorservice.CreateNexusEndpointRequest_builder{
				Spec: nexuspb.EndpointSpec_builder{
					Name: testcore.RandomizeStr(endpointName),
					Target: nexuspb.EndpointTarget_builder{
						Worker: nexuspb.EndpointTarget_Worker_builder{
							Namespace: s.Namespace().String(),
							TaskQueue: string(make([]byte, 1005)),
						}.Build(),
					}.Build(),
				}.Build(),
			}.Build(),
			assertion: func(resp *operatorservice.CreateNexusEndpointResponse, err error) {
				s.ErrorAs(err, new(*serviceerror.InvalidArgument))
				s.ErrorContains(err, "taskQueue length exceeds limit")
			},
		},
		{
			name: "invalid: empty URL",
			request: operatorservice.CreateNexusEndpointRequest_builder{
				Spec: nexuspb.EndpointSpec_builder{
					Name: testcore.RandomizeStr(endpointName),
					Target: nexuspb.EndpointTarget_builder{
						External: &nexuspb.EndpointTarget_External{},
					}.Build(),
				}.Build(),
			}.Build(),
			assertion: func(resp *operatorservice.CreateNexusEndpointResponse, err error) {
				s.ErrorAs(err, new(*serviceerror.InvalidArgument))
				s.ErrorContains(err, "empty target URL")
			},
		},
		{
			name: "invalid: URL too long",
			request: operatorservice.CreateNexusEndpointRequest_builder{
				Spec: nexuspb.EndpointSpec_builder{
					Name: testcore.RandomizeStr(endpointName),
					Target: nexuspb.EndpointTarget_builder{
						External: nexuspb.EndpointTarget_External_builder{
							Url: "http://foo/" + strings.Repeat("pattern", 4096/len("pattern")),
						}.Build(),
					}.Build(),
				}.Build(),
			}.Build(),
			assertion: func(resp *operatorservice.CreateNexusEndpointResponse, err error) {
				s.ErrorAs(err, new(*serviceerror.InvalidArgument))
				s.ErrorContains(err, "URL length exceeds limit")
			},
		},
		{
			name: "invalid: URL invalid",
			request: operatorservice.CreateNexusEndpointRequest_builder{
				Spec: nexuspb.EndpointSpec_builder{
					Name: testcore.RandomizeStr(endpointName),
					Target: nexuspb.EndpointTarget_builder{
						External: nexuspb.EndpointTarget_External_builder{
							Url: "-http://foo",
						}.Build(),
					}.Build(),
				}.Build(),
			}.Build(),
			assertion: func(resp *operatorservice.CreateNexusEndpointResponse, err error) {
				s.ErrorAs(err, new(*serviceerror.InvalidArgument))
				s.ErrorContains(err, "invalid target URL: parse")
			},
		},
		{
			name: "invalid: URL invalid scheme",
			request: operatorservice.CreateNexusEndpointRequest_builder{
				Spec: nexuspb.EndpointSpec_builder{
					Name: testcore.RandomizeStr(endpointName),
					Target: nexuspb.EndpointTarget_builder{
						External: nexuspb.EndpointTarget_External_builder{
							Url: "smtp://foo",
						}.Build(),
					}.Build(),
				}.Build(),
			}.Build(),
			assertion: func(resp *operatorservice.CreateNexusEndpointResponse, err error) {
				s.ErrorAs(err, new(*serviceerror.InvalidArgument))
				s.ErrorContains(err, "invalid target URL scheme:")
			},
		},
		{
			name: "invalid: description too large",
			request: operatorservice.CreateNexusEndpointRequest_builder{
				Spec: nexuspb.EndpointSpec_builder{
					Name: testcore.RandomizeStr(endpointName),
					Target: nexuspb.EndpointTarget_builder{
						Worker: nexuspb.EndpointTarget_Worker_builder{
							Namespace: s.Namespace().String(),
							TaskQueue: s.defaultTaskQueue().GetName(),
						}.Build(),
					}.Build(),
					Description: commonpb.Payload_builder{
						Data: make([]byte, 20001),
					}.Build(),
				}.Build(),
			}.Build(),
			assertion: func(resp *operatorservice.CreateNexusEndpointResponse, err error) {
				s.ErrorAs(err, new(*serviceerror.InvalidArgument))
				s.ErrorContains(err, "description size exceeds limit of 20000")
			},
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			resp, err := s.OperatorClient().CreateNexusEndpoint(testcore.NewContext(), tc.request)
			tc.assertion(resp, err)
		})
	}
}

func (s *OperatorSuite) TestUpdate() {
	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())
	updatedName := testcore.RandomizedNexusEndpoint(s.T().Name() + "-updated")
	endpoint := s.createNexusEndpoint(endpointName)
	type testcase struct {
		name      string
		request   *operatorservice.UpdateNexusEndpointRequest
		assertion func(*operatorservice.UpdateNexusEndpointResponse, error)
	}
	testCases := []testcase{
		{
			name: "valid update",
			request: operatorservice.UpdateNexusEndpointRequest_builder{
				Version: 1,
				Id:      endpoint.GetId(),
				Spec: nexuspb.EndpointSpec_builder{
					Name: updatedName,
					Target: nexuspb.EndpointTarget_builder{
						Worker: nexuspb.EndpointTarget_Worker_builder{
							Namespace: s.Namespace().String(),
							TaskQueue: s.defaultTaskQueue().GetName(),
						}.Build(),
					}.Build(),
				}.Build(),
			}.Build(),
			assertion: func(resp *operatorservice.UpdateNexusEndpointResponse, err error) {
				s.NoError(err)
				s.NotNil(resp.GetEndpoint())
				s.Equal(int64(2), resp.GetEndpoint().GetVersion())
				s.Equal(updatedName, resp.GetEndpoint().GetSpec().GetName())
				s.NotNil(resp.GetEndpoint().GetLastModifiedTime())
			},
		},
		{
			name: "invalid: endpoint not found",
			request: operatorservice.UpdateNexusEndpointRequest_builder{
				Version: 1,
				Id:      "not-found",
				Spec: nexuspb.EndpointSpec_builder{
					Name: updatedName,
					Target: nexuspb.EndpointTarget_builder{
						Worker: nexuspb.EndpointTarget_Worker_builder{
							Namespace: s.Namespace().String(),
							TaskQueue: s.defaultTaskQueue().GetName(),
						}.Build(),
					}.Build(),
				}.Build(),
			}.Build(),
			assertion: func(resp *operatorservice.UpdateNexusEndpointResponse, err error) {
				var notFoundErr *serviceerror.NotFound
				s.ErrorAs(err, &notFoundErr)
			},
		},
		{
			name: "invalid: endpoint version mismatch",
			request: operatorservice.UpdateNexusEndpointRequest_builder{
				Version: 1,
				Id:      endpoint.GetId(),
				Spec: nexuspb.EndpointSpec_builder{
					Name: updatedName,
					Target: nexuspb.EndpointTarget_builder{
						Worker: nexuspb.EndpointTarget_Worker_builder{
							Namespace: s.Namespace().String(),
							TaskQueue: s.defaultTaskQueue().GetName(),
						}.Build(),
					}.Build(),
				}.Build(),
			}.Build(),
			assertion: func(resp *operatorservice.UpdateNexusEndpointResponse, err error) {
				var fpErr *serviceerror.FailedPrecondition
				s.ErrorAs(err, &fpErr)
			},
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			resp, err := s.OperatorClient().UpdateNexusEndpoint(testcore.NewContext(), tc.request)
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
			serviceId: endpoint.GetId(),
			assertion: func(resp *operatorservice.DeleteNexusEndpointResponse, err error) {
				s.NoError(err)
			},
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			resp, err := s.OperatorClient().DeleteNexusEndpoint(
				testcore.NewContext(),
				operatorservice.DeleteNexusEndpointRequest_builder{
					Id:      tc.serviceId,
					Version: 1,
				}.Build())
			tc.assertion(resp, err)
		})
	}
}

func (s *OperatorSuite) TestList() {
	// initialize some endpoints
	s.createNexusEndpoint("operator-list-test-service0")
	s.createNexusEndpoint("operator-list-test-service1")
	entryToFilter := s.createNexusEndpoint("operator-list-test-service2")

	// get ordered endpoints for the course of the tests
	resp, err := s.OperatorClient().ListNexusEndpoints(testcore.NewContext(), &operatorservice.ListNexusEndpointsRequest{})
	s.NoError(err)
	s.NotNil(resp)
	endpointsOrdered := resp.GetEndpoints()

	resp, err = s.OperatorClient().ListNexusEndpoints(testcore.NewContext(), operatorservice.ListNexusEndpointsRequest_builder{PageSize: 2}.Build())
	s.NoError(err)
	s.NotNil(resp)
	nextPageToken := resp.GetNextPageToken()

	type testcase struct {
		name      string
		request   *operatorservice.ListNexusEndpointsRequest
		assertion func(*operatorservice.ListNexusEndpointsResponse, error)
	}
	testCases := []testcase{
		{
			name: "list first page",
			request: operatorservice.ListNexusEndpointsRequest_builder{
				NextPageToken: nil,
				PageSize:      2,
			}.Build(),
			assertion: func(resp *operatorservice.ListNexusEndpointsResponse, err error) {
				s.NoError(err)
				s.Equal(nextPageToken, resp.GetNextPageToken())
				s.ProtoElementsMatch(resp.GetEndpoints(), endpointsOrdered[0:2])
			},
		},
		{
			name: "list non-first page",
			request: operatorservice.ListNexusEndpointsRequest_builder{
				NextPageToken: nextPageToken,
				PageSize:      2,
			}.Build(),
			assertion: func(resp *operatorservice.ListNexusEndpointsResponse, err error) {
				s.NoError(err)
				s.ProtoEqual(resp.GetEndpoints()[0], endpointsOrdered[2])
			},
		},
		{
			name:    "list with no page size",
			request: &operatorservice.ListNexusEndpointsRequest{},
			assertion: func(resp *operatorservice.ListNexusEndpointsResponse, err error) {
				s.NoError(err)
				s.NotEmpty(resp.GetEndpoints())
			},
		},
		{
			name: "list with filter found",
			request: operatorservice.ListNexusEndpointsRequest_builder{
				NextPageToken: nil,
				PageSize:      2,
				Name:          entryToFilter.GetEndpoint().GetSpec().GetName(),
			}.Build(),
			assertion: func(resp *operatorservice.ListNexusEndpointsResponse, err error) {
				s.NoError(err)
				s.Nil(resp.GetNextPageToken())
				s.Len(resp.GetEndpoints(), 1)
				s.Equal(resp.GetEndpoints()[0].GetSpec().GetName(), entryToFilter.GetEndpoint().GetSpec().GetName())
			},
		},
		{
			name: "list with filter not found",
			request: operatorservice.ListNexusEndpointsRequest_builder{
				NextPageToken: nil,
				PageSize:      2,
				Name:          "missing-endpoint",
			}.Build(),
			assertion: func(resp *operatorservice.ListNexusEndpointsResponse, err error) {
				s.NoError(err)
				s.Nil(resp.GetNextPageToken())
				s.Empty(resp.GetEndpoints())
			},
		},
		{
			name: "list with page size too large",
			request: operatorservice.ListNexusEndpointsRequest_builder{
				NextPageToken: nil,
				PageSize:      1005,
			}.Build(),
			assertion: func(resp *operatorservice.ListNexusEndpointsResponse, err error) {
				var invalidErr *serviceerror.InvalidArgument
				s.ErrorAs(err, &invalidErr)
			},
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			resp, err := s.OperatorClient().ListNexusEndpoints(testcore.NewContext(), tc.request)
			tc.assertion(resp, err)
		})
	}
}

func (s *OperatorSuite) TestGet() {
	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())
	endpoint := s.createNexusEndpoint(endpointName)

	type testcase struct {
		name      string
		request   *operatorservice.GetNexusEndpointRequest
		assertion func(*operatorservice.GetNexusEndpointResponse, error)
	}
	testCases := []testcase{
		{
			name: "valid get",
			request: operatorservice.GetNexusEndpointRequest_builder{
				Id: endpoint.GetId(),
			}.Build(),
			assertion: func(response *operatorservice.GetNexusEndpointResponse, err error) {
				s.NoError(err)
				s.Equal(endpoint.GetId(), response.GetEndpoint().GetId())
				s.Equal(endpoint.GetVersion(), response.GetEndpoint().GetVersion())
				s.Equal(endpoint.GetEndpoint().GetCreatedTime(), response.GetEndpoint().GetCreatedTime())
				s.Equal(endpoint.GetEndpoint().GetSpec().GetName(), response.GetEndpoint().GetSpec().GetName())
				s.Equal(endpoint.GetEndpoint().GetSpec().GetTarget().GetWorker().GetNamespaceId(), s.GetNamespaceID(response.GetEndpoint().GetSpec().GetTarget().GetWorker().GetNamespace()))
				s.Equal(endpoint.GetEndpoint().GetSpec().GetTarget().GetWorker().GetTaskQueue(), response.GetEndpoint().GetSpec().GetTarget().GetWorker().GetTaskQueue())
			},
		},
		{
			name: "invalid: missing endpoint",
			request: operatorservice.GetNexusEndpointRequest_builder{
				Id: uuid.NewString(),
			}.Build(),
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
			resp, err := s.OperatorClient().GetNexusEndpoint(testcore.NewContext(), tc.request)
			tc.assertion(resp, err)
		})
	}
}

func (s *NexusEndpointFunctionalSuite) defaultTaskQueue() *taskqueuepb.TaskQueue {
	name := fmt.Sprintf("functional-queue-%v", s.T().Name())
	return taskqueuepb.TaskQueue_builder{Name: name, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build()
}

func (s *NexusEndpointFunctionalSuite) createNexusEndpoint(name string) *persistencespb.NexusEndpointEntry {
	resp, err := s.GetTestCluster().MatchingClient().CreateNexusEndpoint(
		testcore.NewContext(),
		matchingservice.CreateNexusEndpointRequest_builder{
			Spec: persistencespb.NexusEndpointSpec_builder{
				Name: name,
				Target: persistencespb.NexusEndpointTarget_builder{
					Worker: persistencespb.NexusEndpointTarget_Worker_builder{
						NamespaceId: s.NamespaceID().String(),
						TaskQueue:   s.defaultTaskQueue().GetName(),
					}.Build(),
				}.Build(),
			}.Build(),
		}.Build())

	s.NoError(err)
	s.NotNil(resp.GetEntry())
	return resp.GetEntry()
}
