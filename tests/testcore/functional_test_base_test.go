package testcore

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/testing/mockapi/operatorservicemock/v1"
	"go.temporal.io/server/common/testing/mockapi/workflowservicemock/v1"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/service/worker"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type FunctionalTestBaseSuite struct {
	functionalTestBase
}

func TestFunctionalTestBaseSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, &FunctionalTestBaseSuite{})
}

func (s *FunctionalTestBaseSuite) SetupSuite() {
	s.SetupSuiteWithCluster()
}

func (s *FunctionalTestBaseSuite) TearDownSuite() {
	s.TearDownCluster()
}

func (s *FunctionalTestBaseSuite) SetupTest() {
	s.functionalTestBase.SetupTest()
}

func (s *FunctionalTestBaseSuite) TestWorkerServiceHealthCheck() {
	// This test verifies that the worker service exposes a working gRPC health check endpoint.
	conn, err := grpc.NewClient(
		s.WorkerGRPCAddress(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	s.NoError(err)
	defer func() { _ = conn.Close() }()

	healthClient := healthpb.NewHealthClient(conn)
	s.Eventually(
		func() bool {
			resp, err := healthClient.Check(context.Background(), &healthpb.HealthCheckRequest{
				Service: worker.ServiceName,
			})
			return err == nil && resp.Status == healthpb.HealthCheckResponse_SERVING
		},
		10*time.Second,
		100*time.Millisecond,
	)
}

func TestRegisterNamespace(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name              string
		transientFailures int
		createErr         error
		cancel            bool
		wantAttempts      int
	}{
		{name: "RetriesContention", transientFailures: 4, wantAttempts: 5},
		{name: "DuplicateNamespace", createErr: serviceerror.NewNamespaceAlreadyExists("duplicate"), wantAttempts: 1},
		{name: "Cancellation", createErr: serviceerror.NewUnavailable("conditional failure"), cancel: true, wantAttempts: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(testcontext.For(t))
				defer cancel()

				ctrl := gomock.NewController(t)
				metadata := cluster.NewMockMetadata(ctrl)
				metadata.EXPECT().GetCurrentClusterName().Return("active")
				manager := persistence.NewMockMetadataManager(ctrl)
				frontend := workflowservicemock.NewMockWorkflowServiceClient(ctrl)
				operator := operatorservicemock.NewMockOperatorServiceClient(ctrl)
				base := &functionalTestBase{
					Logger: log.NewNoopLogger(),
					testCluster: &TestCluster{
						testBase: &persistencetests.TestBase{
							ClusterMetadata: metadata,
							MetadataManager: manager,
						},
						host: &temporalImpl{},
					},
				}
				base.testCluster.host.frontend.once.Do(func() {
					base.testCluster.host.frontend.frontend = frontend
					base.testCluster.host.frontend.operator = operator
				})

				ns := namespace.Name("test-namespace")
				var requestedID namespace.ID
				attempts := 0
				manager.EXPECT().CreateNamespace(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, req *persistence.CreateNamespaceRequest) (*persistence.CreateNamespaceResponse, error) {
						attempts++
						require.Equal(t, ns.String(), req.Namespace.Info.Name)
						if attempts == 1 {
							requestedID = namespace.ID(req.Namespace.Info.Id)
							require.NotEmpty(t, requestedID)
						}
						require.Equal(t, requestedID.String(), req.Namespace.Info.Id)
						if tc.cancel {
							cancel()
						}
						if attempts <= tc.transientFailures {
							return nil, serviceerror.NewUnavailable("conditional failure")
						}
						if tc.createErr != nil {
							return nil, tc.createErr
						}
						return &persistence.CreateNamespaceResponse{ID: requestedID.String()}, nil
					}).AnyTimes()

				if tc.createErr == nil {
					frontend.EXPECT().DescribeNamespace(gomock.Any(), &workflowservice.DescribeNamespaceRequest{
						Namespace: ns.String(),
					}).Return(&workflowservice.DescribeNamespaceResponse{}, nil)
					operator.EXPECT().AddSearchAttributes(gomock.Any(), gomock.Any()).
						Return(&operatorservice.AddSearchAttributesResponse{}, nil)
					operator.EXPECT().ListSearchAttributes(gomock.Any(), &operatorservice.ListSearchAttributesRequest{
						Namespace: ns.String(),
					}).Return(&operatorservice.ListSearchAttributesResponse{
						CustomAttributes: searchattribute.TestSearchAttributesToRegister(),
					}, nil)
				}

				id, err := base.RegisterNamespace(ctx, ns, 1, enumspb.ARCHIVAL_STATE_DISABLED, "", "")
				if tc.createErr != nil {
					require.ErrorIs(t, err, tc.createErr)
					require.Equal(t, namespace.EmptyID, id)
				} else {
					require.NoError(t, err)
					require.Equal(t, requestedID, id)
				}

				require.Equal(t, tc.wantAttempts, attempts)
			})
		})
	}
}
