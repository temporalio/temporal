package tests

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	commonnexus "go.temporal.io/server/common/nexus"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
)

func TestNexusEndpointsCommonSuite(t *testing.T) {
	parallelsuite.RunLegacySequential(t, &CommonSuite{})
}

func TestNexusEndpointsMatchingSuite(t *testing.T) {
	parallelsuite.RunLegacySequential(t, &MatchingSuite{})
}

func TestNexusEndpointsOperatorSuite(t *testing.T) {
	parallelsuite.RunLegacySequential(t, &OperatorSuite{})
}

type CommonSuite struct {
	parallelsuite.Suite[*CommonSuite]
}

func (s *CommonSuite) TestListOrdering() {
	// The nexus endpoints table is cluster-global; this test asserts exact table
	// versions and entry counts, so it needs a cluster with no concurrent writers.
	env := newNexusTestEnv(s.T(), false,
		testcore.WithDynamicConfig(dynamicconfig.NexusEndpointListMaxPageSize, 1000),
	)
	ctx := s.Context()

	// get initial table version since it has been modified by other tests
	resp, err := env.GetTestCluster().MatchingClient().ListNexusEndpoints(ctx, &matchingservice.ListNexusEndpointsRequest{
		LastKnownTableVersion: 0,
		PageSize:              0,
	})
	s.NoError(err)
	initialTableVersion := resp.TableVersion

	// create some endpoints
	numEndpoints := 40 // minimum number of endpoints to test, there may be more in DB from other tests
	for range numEndpoints {
		env.createRandomNexusEndpoint(s.Context(), s.T())
	}
	tableVersion := initialTableVersion + int64(numEndpoints)

	// list from persistence manager level
	persistence := env.GetTestCluster().TestBase().NexusEndpointManager
	persistenceResp1, err := persistence.ListNexusEndpoints(ctx, &p.ListNexusEndpointsRequest{
		LastKnownTableVersion: tableVersion,
		PageSize:              numEndpoints / 2,
	})
	s.NoError(err)
	s.Len(persistenceResp1.Entries, numEndpoints/2)
	s.NotNil(persistenceResp1.NextPageToken)
	persistenceResp2, err := persistence.ListNexusEndpoints(ctx, &p.ListNexusEndpointsRequest{
		LastKnownTableVersion: tableVersion,
		PageSize:              numEndpoints / 2,
		NextPageToken:         persistenceResp1.NextPageToken,
	})
	s.NoError(err)
	s.Len(persistenceResp2.Entries, numEndpoints/2)

	// list from matching level
	matchingClient := env.GetTestCluster().MatchingClient()
	matchingResp1, err := matchingClient.ListNexusEndpoints(ctx, &matchingservice.ListNexusEndpointsRequest{
		LastKnownTableVersion: tableVersion,
		PageSize:              int32(numEndpoints / 2),
	})
	s.NoError(err)
	s.Len(matchingResp1.Entries, numEndpoints/2)
	s.NotNil(matchingResp1.NextPageToken)
	matchingResp2, err := matchingClient.ListNexusEndpoints(ctx, &matchingservice.ListNexusEndpointsRequest{
		LastKnownTableVersion: tableVersion,
		PageSize:              int32(numEndpoints / 2),
		NextPageToken:         matchingResp1.NextPageToken,
	})
	s.NoError(err)
	s.Len(matchingResp2.Entries, numEndpoints/2)

	// list from operator level
	operatorResp1, err := env.OperatorClient().ListNexusEndpoints(ctx, &operatorservice.ListNexusEndpointsRequest{
		PageSize: int32(numEndpoints / 2),
	})
	s.NoError(err)
	s.Len(operatorResp1.Endpoints, numEndpoints/2)
	s.NotNil(operatorResp1.NextPageToken)
	operatorResp2, err := env.OperatorClient().ListNexusEndpoints(ctx, &operatorservice.ListNexusEndpointsRequest{
		PageSize:      int32(numEndpoints / 2),
		NextPageToken: operatorResp1.NextPageToken,
	})
	s.NoError(err)
	s.Len(operatorResp2.Endpoints, numEndpoints/2)

	// assert list orders match
	for i := 0; i < numEndpoints/2; i++ {
		s.Equal(persistenceResp1.Entries[i].Id, matchingResp1.Entries[i].Id)
		s.Equal(persistenceResp2.Entries[i].Id, matchingResp2.Entries[i].Id)

		s.Equal(persistenceResp1.Entries[i].Id, operatorResp1.Endpoints[i].Id)
		s.Equal(persistenceResp2.Entries[i].Id, operatorResp2.Endpoints[i].Id)
	}
}

type MatchingSuite struct {
	parallelsuite.Suite[*MatchingSuite]
}

func (s *MatchingSuite) TestCreate() {
	env := newNexusTestEnv(s.T(), false)
	ctx := s.Context()
	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())
	entry := env.createNexusEndpoint(s.Context(), s.T(), endpointName, "unused")
	s.Equal(int64(1), entry.Version)
	s.NotNil(entry.CreatedTime)
	s.NotEmpty(entry.Id)
	s.Equal(entry.Spec.Name, endpointName)
	s.Equal(entry.Spec.Target.GetWorker().Namespace, env.Namespace().String())

	_, err := env.GetTestCluster().MatchingClient().CreateNexusEndpoint(ctx, &matchingservice.CreateNexusEndpointRequest{
		Spec: &persistencespb.NexusEndpointSpec{
			Name: endpointName,
			Target: &persistencespb.NexusEndpointTarget{
				Variant: &persistencespb.NexusEndpointTarget_Worker_{
					Worker: &persistencespb.NexusEndpointTarget_Worker{
						NamespaceId: env.NamespaceID().String(),
						TaskQueue:   "dont-care",
					},
				},
			},
		},
	})
	var existsErr *serviceerror.AlreadyExists
	s.ErrorAs(err, &existsErr)
}

func (s *MatchingSuite) TestUpdate() {
	env := newNexusTestEnv(s.T(), false)
	updatedName := testcore.RandomizedNexusEndpoint(s.T().Name() + "-updated")
	endpoint := env.createRandomNexusEndpoint(s.Context(), s.T())
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
				Spec: &persistencespb.NexusEndpointSpec{
					Name: updatedName,
					Target: &persistencespb.NexusEndpointTarget{
						Variant: &persistencespb.NexusEndpointTarget_Worker_{
							Worker: &persistencespb.NexusEndpointTarget_Worker{
								NamespaceId: env.NamespaceID().String(),
								TaskQueue:   env.Tv().TaskQueue().Name,
							},
						},
					},
				},
			},
			assertion: func(resp *matchingservice.UpdateNexusEndpointResponse, err error) {
				s.NoError(err)
				s.NotNil(resp.Entry)
				s.Equal(int64(2), resp.Entry.Version)
				s.Equal(updatedName, resp.Entry.Endpoint.Spec.Name)
				s.NotNil(resp.Entry.Endpoint.Clock)
			},
		},
		{
			name: "invalid update: endpoint not found",
			request: &matchingservice.UpdateNexusEndpointRequest{
				Version: 1,
				Id:      "not-found",
				Spec: &persistencespb.NexusEndpointSpec{
					Name: updatedName,
					Target: &persistencespb.NexusEndpointTarget{
						Variant: &persistencespb.NexusEndpointTarget_Worker_{
							Worker: &persistencespb.NexusEndpointTarget_Worker{
								NamespaceId: env.NamespaceID().String(),
								TaskQueue:   env.Tv().TaskQueue().Name,
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
				Spec: &persistencespb.NexusEndpointSpec{
					Name: updatedName,
					Target: &persistencespb.NexusEndpointTarget{
						Variant: &persistencespb.NexusEndpointTarget_Worker_{
							Worker: &persistencespb.NexusEndpointTarget_Worker{
								NamespaceId: env.NamespaceID().String(),
								TaskQueue:   env.Tv().TaskQueue().Name,
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

	matchingClient := env.GetTestCluster().MatchingClient()
	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			resp, err := matchingClient.UpdateNexusEndpoint(s.Context(), tc.request)
			tc.assertion(resp, err)
		})
	}
}

func (s *MatchingSuite) TestDelete() {
	env := newNexusTestEnv(s.T(), false)
	endpoint := env.createRandomNexusEndpoint(s.Context(), s.T())
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

	matchingClient := env.GetTestCluster().MatchingClient()
	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			resp, err := matchingClient.DeleteNexusEndpoint(
				s.Context(),
				&matchingservice.DeleteNexusEndpointRequest{
					Id: tc.endpointID,
				})
			tc.assertion(resp, err)
		})
	}
}

func (s *MatchingSuite) TestList() {
	// The nexus endpoints table is cluster-global; this test asserts exact entry
	// ordering and counts, so it needs a cluster with no concurrent writers.
	env := newNexusTestEnv(s.T(), false,
		testcore.WithDynamicConfig(dynamicconfig.NexusEndpointListMaxPageSize, 1000),
	)
	ctx := s.Context()

	// initialize some endpoints
	env.createRandomNexusEndpoint(s.Context(), s.T())
	env.createRandomNexusEndpoint(s.Context(), s.T())
	env.createRandomNexusEndpoint(s.Context(), s.T())

	// get expected table version and endpoints for the course of the tests
	matchingClient := env.GetTestCluster().MatchingClient()
	resp, err := matchingClient.ListNexusEndpoints(
		ctx,
		&matchingservice.ListNexusEndpointsRequest{
			PageSize:              100,
			LastKnownTableVersion: 0,
			Wait:                  false,
		})
	s.NoError(err)
	s.NotNil(resp)
	tableVersion := resp.TableVersion
	endpointsOrdered := resp.Entries
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
				s.ProtoElementsMatch(resp.Entries, endpointsOrdered[0:2])
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
				s.ProtoElementsMatch(resp.Entries, endpointsOrdered[0:3])
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
				s.ProtoEqual(resp.Entries[0], endpointsOrdered[2])
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
				s.Len(resp.Entries, 3)
			},
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			ctx := s.Context()
			listReqDone := make(chan struct{})
			go func() {
				defer close(listReqDone)
				resp, err := matchingClient.ListNexusEndpoints(ctx, tc.request) //nolint:revive
				tc.assertion(resp, err)
			}()
			if tc.request.Wait && tc.request.NextPageToken == nil && tc.request.LastKnownTableVersion != 0 {
				env.createRandomNexusEndpoint(s.Context(), s.T())
			}
			select {
			case <-listReqDone:
			case <-ctx.Done():
				s.Fail("timed out waiting for list nexus endpoints request to complete")
			}
		})
	}
}

type OperatorSuite struct {
	parallelsuite.Suite[*OperatorSuite]
}

func (s *OperatorSuite) TestCreate() {
	env := newNexusTestEnv(s.T(), false)
	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())
	type testcase struct {
		name      string
		request   *operatorservice.CreateNexusEndpointRequest
		assertion func(*operatorservice.CreateNexusEndpointResponse, error)
	}
	testCases := []testcase{
		{
			name: "valid create",
			request: &operatorservice.CreateNexusEndpointRequest{
				Spec: &nexuspb.EndpointSpec{
					Name: endpointName,
					Target: &nexuspb.EndpointTarget{
						Variant: &nexuspb.EndpointTarget_Worker_{
							Worker: &nexuspb.EndpointTarget_Worker{
								Namespace: env.Namespace().String(),
								TaskQueue: env.Tv().TaskQueue().Name,
							},
						},
					},
				},
			},
			assertion: func(resp *operatorservice.CreateNexusEndpointResponse, err error) {
				s.NoError(err)
				s.NotNil(resp.Endpoint)
				s.Equal(int64(1), resp.Endpoint.Version)
				s.Nil(resp.Endpoint.LastModifiedTime)
				s.NotNil(resp.Endpoint.CreatedTime)
				s.NotEmpty(resp.Endpoint.Id)
				s.Equal(resp.Endpoint.Spec.Name, endpointName)
				s.Equal(resp.Endpoint.Spec.Target.GetWorker().Namespace, env.Namespace().String())
				s.Equal("/"+commonnexus.RouteDispatchNexusTaskByEndpoint.Path(resp.Endpoint.Id), resp.Endpoint.UrlPrefix)
			},
		},
		{
			name: "invalid: name already in use",
			request: &operatorservice.CreateNexusEndpointRequest{
				Spec: &nexuspb.EndpointSpec{
					Name: endpointName,
					Target: &nexuspb.EndpointTarget{
						Variant: &nexuspb.EndpointTarget_Worker_{
							Worker: &nexuspb.EndpointTarget_Worker{
								Namespace: env.Namespace().String(),
								TaskQueue: env.Tv().TaskQueue().Name,
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
				Spec: &nexuspb.EndpointSpec{
					Target: &nexuspb.EndpointTarget{
						Variant: &nexuspb.EndpointTarget_Worker_{
							Worker: &nexuspb.EndpointTarget_Worker{
								Namespace: env.Namespace().String(),
								TaskQueue: env.Tv().TaskQueue().Name,
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
				Spec: &nexuspb.EndpointSpec{
					Name: string(make([]byte, 300)),
					Target: &nexuspb.EndpointTarget{
						Variant: &nexuspb.EndpointTarget_Worker_{
							Worker: &nexuspb.EndpointTarget_Worker{
								Namespace: env.Namespace().String(),
								TaskQueue: env.Tv().TaskQueue().Name,
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
				Spec: &nexuspb.EndpointSpec{
					Name: "test_\n```\n",
					Target: &nexuspb.EndpointTarget{
						Variant: &nexuspb.EndpointTarget_Worker_{
							Worker: &nexuspb.EndpointTarget_Worker{
								Namespace: env.Namespace().String(),
								TaskQueue: env.Tv().TaskQueue().Name,
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
				Spec: &nexuspb.EndpointSpec{
					Name: testcore.RandomizeStr(endpointName),
					Target: &nexuspb.EndpointTarget{
						Variant: &nexuspb.EndpointTarget_Worker_{
							Worker: &nexuspb.EndpointTarget_Worker{
								TaskQueue: env.Tv().TaskQueue().Name,
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
				Spec: &nexuspb.EndpointSpec{
					Name: testcore.RandomizeStr(endpointName),
					Target: &nexuspb.EndpointTarget{
						Variant: &nexuspb.EndpointTarget_Worker_{
							Worker: &nexuspb.EndpointTarget_Worker{
								Namespace: "missing-namespace",
								TaskQueue: env.Tv().TaskQueue().Name,
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
				Spec: &nexuspb.EndpointSpec{
					Name: testcore.RandomizeStr(endpointName),
					Target: &nexuspb.EndpointTarget{
						Variant: &nexuspb.EndpointTarget_Worker_{
							Worker: &nexuspb.EndpointTarget_Worker{
								Namespace: env.Namespace().String(),
							},
						},
					},
				},
			},
			assertion: func(resp *operatorservice.CreateNexusEndpointResponse, err error) {
				s.ErrorAs(err, new(*serviceerror.InvalidArgument))
				s.ErrorContains(err, "taskQueue is not set")
			},
		},
		{
			name: "invalid: task queue too long",
			request: &operatorservice.CreateNexusEndpointRequest{
				Spec: &nexuspb.EndpointSpec{
					Name: testcore.RandomizeStr(endpointName),
					Target: &nexuspb.EndpointTarget{
						Variant: &nexuspb.EndpointTarget_Worker_{
							Worker: &nexuspb.EndpointTarget_Worker{
								Namespace: env.Namespace().String(),
								TaskQueue: string(make([]byte, 1005)),
							},
						},
					},
				},
			},
			assertion: func(resp *operatorservice.CreateNexusEndpointResponse, err error) {
				s.ErrorAs(err, new(*serviceerror.InvalidArgument))
				s.ErrorContains(err, "taskQueue length exceeds limit")
			},
		},
		{
			name: "invalid: empty URL",
			request: &operatorservice.CreateNexusEndpointRequest{
				Spec: &nexuspb.EndpointSpec{
					Name: testcore.RandomizeStr(endpointName),
					Target: &nexuspb.EndpointTarget{
						Variant: &nexuspb.EndpointTarget_External_{
							External: &nexuspb.EndpointTarget_External{},
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
				Spec: &nexuspb.EndpointSpec{
					Name: testcore.RandomizeStr(endpointName),
					Target: &nexuspb.EndpointTarget{
						Variant: &nexuspb.EndpointTarget_External_{
							External: &nexuspb.EndpointTarget_External{
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
				Spec: &nexuspb.EndpointSpec{
					Name: testcore.RandomizeStr(endpointName),
					Target: &nexuspb.EndpointTarget{
						Variant: &nexuspb.EndpointTarget_External_{
							External: &nexuspb.EndpointTarget_External{
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
				Spec: &nexuspb.EndpointSpec{
					Name: testcore.RandomizeStr(endpointName),
					Target: &nexuspb.EndpointTarget{
						Variant: &nexuspb.EndpointTarget_External_{
							External: &nexuspb.EndpointTarget_External{
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
				Spec: &nexuspb.EndpointSpec{
					Name: testcore.RandomizeStr(endpointName),
					Target: &nexuspb.EndpointTarget{
						Variant: &nexuspb.EndpointTarget_Worker_{
							Worker: &nexuspb.EndpointTarget_Worker{
								Namespace: env.Namespace().String(),
								TaskQueue: env.Tv().TaskQueue().Name,
							},
						},
					},
					Description: &commonpb.Payload{
						Data: make([]byte, 20001),
					},
				},
			},
			assertion: func(resp *operatorservice.CreateNexusEndpointResponse, err error) {
				s.ErrorAs(err, new(*serviceerror.InvalidArgument))
				s.ErrorContains(err, "description size exceeds limit of 20000")
			},
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			resp, err := env.OperatorClient().CreateNexusEndpoint(s.Context(), tc.request)
			tc.assertion(resp, err)
		})
	}
}

func (s *OperatorSuite) TestUpdate() {
	env := newNexusTestEnv(s.T(), false)
	updatedName := testcore.RandomizedNexusEndpoint(s.T().Name() + "-updated")
	endpoint := env.createRandomNexusEndpoint(s.Context(), s.T())
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
				Spec: &nexuspb.EndpointSpec{
					Name: updatedName,
					Target: &nexuspb.EndpointTarget{
						Variant: &nexuspb.EndpointTarget_Worker_{
							Worker: &nexuspb.EndpointTarget_Worker{
								Namespace: env.Namespace().String(),
								TaskQueue: env.Tv().TaskQueue().Name,
							},
						},
					},
				},
			},
			assertion: func(resp *operatorservice.UpdateNexusEndpointResponse, err error) {
				s.NoError(err)
				s.NotNil(resp.Endpoint)
				s.Equal(int64(2), resp.Endpoint.Version)
				s.Equal(updatedName, resp.Endpoint.Spec.Name)
				s.NotNil(resp.Endpoint.LastModifiedTime)
			},
		},
		{
			name: "invalid: endpoint not found",
			request: &operatorservice.UpdateNexusEndpointRequest{
				Version: 1,
				Id:      "not-found",
				Spec: &nexuspb.EndpointSpec{
					Name: updatedName,
					Target: &nexuspb.EndpointTarget{
						Variant: &nexuspb.EndpointTarget_Worker_{
							Worker: &nexuspb.EndpointTarget_Worker{
								Namespace: env.Namespace().String(),
								TaskQueue: env.Tv().TaskQueue().Name,
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
				Spec: &nexuspb.EndpointSpec{
					Name: updatedName,
					Target: &nexuspb.EndpointTarget{
						Variant: &nexuspb.EndpointTarget_Worker_{
							Worker: &nexuspb.EndpointTarget_Worker{
								Namespace: env.Namespace().String(),
								TaskQueue: env.Tv().TaskQueue().Name,
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
			resp, err := env.OperatorClient().UpdateNexusEndpoint(s.Context(), tc.request)
			tc.assertion(resp, err)
		})
	}
}

func (s *OperatorSuite) TestDelete() {
	env := newNexusTestEnv(s.T(), false)
	endpoint := env.createRandomNexusEndpoint(s.Context(), s.T())
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
			resp, err := env.OperatorClient().DeleteNexusEndpoint(
				s.Context(),
				&operatorservice.DeleteNexusEndpointRequest{
					Id:      tc.serviceId,
					Version: 1,
				})
			tc.assertion(resp, err)
		})
	}
}

func (s *OperatorSuite) TestList() {
	// The nexus endpoints table is cluster-global; this test asserts exact entry
	// ordering, so it needs a cluster with no concurrent writers.
	env := newNexusTestEnv(s.T(), false,
		testcore.WithDynamicConfig(dynamicconfig.NexusEndpointListMaxPageSize, 1000),
	)
	ctx := s.Context()

	// initialize some endpoints
	env.createRandomNexusEndpoint(s.Context(), s.T())
	env.createRandomNexusEndpoint(s.Context(), s.T())
	entryToFilter := env.createRandomNexusEndpoint(s.Context(), s.T())

	// get ordered endpoints for the course of the tests
	resp, err := env.OperatorClient().ListNexusEndpoints(ctx, &operatorservice.ListNexusEndpointsRequest{})
	s.NoError(err)
	s.NotNil(resp)
	endpointsOrdered := resp.Endpoints

	resp, err = env.OperatorClient().ListNexusEndpoints(ctx, &operatorservice.ListNexusEndpointsRequest{PageSize: 2})
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
				Name:          entryToFilter.Spec.Name,
			},
			assertion: func(resp *operatorservice.ListNexusEndpointsResponse, err error) {
				s.NoError(err)
				s.Nil(resp.NextPageToken)
				s.Len(resp.Endpoints, 1)
				s.Equal(resp.Endpoints[0].Spec.Name, entryToFilter.Spec.Name)
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
			resp, err := env.OperatorClient().ListNexusEndpoints(s.Context(), tc.request)
			tc.assertion(resp, err)
		})
	}
}

func (s *OperatorSuite) TestGet() {
	env := newNexusTestEnv(s.T(), false)
	endpoint := env.createRandomNexusEndpoint(s.Context(), s.T())

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
				s.Equal(endpoint.Id, response.Endpoint.Id)
				s.Equal(endpoint.Version, response.Endpoint.Version)
				s.Equal(endpoint.CreatedTime, response.Endpoint.CreatedTime)
				s.Equal(endpoint.Spec.Name, response.Endpoint.Spec.Name)
				s.Equal(endpoint.Spec.Target.GetWorker().Namespace, response.Endpoint.Spec.Target.GetWorker().Namespace)
				s.Equal(endpoint.Spec.Target.GetWorker().TaskQueue, response.Endpoint.Spec.Target.GetWorker().TaskQueue)
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
			resp, err := env.OperatorClient().GetNexusEndpoint(s.Context(), tc.request)
			tc.assertion(resp, err)
		})
	}
}
