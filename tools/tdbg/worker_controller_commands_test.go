package tdbg_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/api/adminservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type wciAdminClient struct {
	adminservice.AdminServiceClient
	req     *adminservice.MigrateWorkerControllerInstanceRequest
	listReq *adminservice.ListWorkerControllerInstancesRequest
}

func (c *wciAdminClient) ListWorkerControllerInstances(
	_ context.Context,
	req *adminservice.ListWorkerControllerInstancesRequest,
	_ ...grpc.CallOption,
) (*adminservice.ListWorkerControllerInstancesResponse, error) {
	c.listReq = req
	return &adminservice.ListWorkerControllerInstancesResponse{
		Instances: []*adminservice.ListWorkerControllerInstancesResponse_Instance{{
			InstanceId:        "wci-id",
			Status:            "Running",
			ValidationStatus:  "Succeeded",
			ComputeProviders:  []string{"aws-lambda", "gcp-cloud-run"},
			DeploymentVersion: "dep:b1",
			StartTime:         timestamppb.Now(),
			LastUpdateTime:    timestamppb.New(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)),
		}},
	}, nil
}

func (c *wciAdminClient) MigrateWorkerControllerInstance(
	_ context.Context,
	req *adminservice.MigrateWorkerControllerInstanceRequest,
	_ ...grpc.CallOption,
) (*adminservice.MigrateWorkerControllerInstanceResponse, error) {
	c.req = req
	return &adminservice.MigrateWorkerControllerInstanceResponse{}, nil
}

func TestMigrateWorkerControllerInstance(t *testing.T) {
	admin := &wciAdminClient{}
	stdout, _, err := runMigrate(t, migrateClientFactory{admin: admin},
		"-n", "my-ns", "worker-controller", "migrate-to-workflow", "--deployment-name", "dep", "--build-id", "b1")
	require.NoError(t, err)
	require.Equal(t, "my-ns", admin.req.GetNamespace())
	require.Equal(t, "dep", admin.req.GetDeploymentName())
	require.Equal(t, "b1", admin.req.GetBuildId())
	require.NotEmpty(t, admin.req.GetIdentity())
	require.Equal(t, adminservice.MigrateWorkerControllerInstanceRequest_TARGET_WORKFLOW, admin.req.GetTarget())
	require.Contains(t, stdout, "dep:b1")

	_, _, err = runMigrate(t, migrateClientFactory{admin: admin}, "-n", "my-ns", "wc", "migrate-to-workflow", "--deployment-name", "dep")
	require.Error(t, err, "--build-id is required")
}

func TestMigrateWorkerControllerInstanceToCHASM(t *testing.T) {
	admin := &wciAdminClient{}
	stdout, _, err := runMigrate(t, migrateClientFactory{admin: admin},
		"-n", "my-ns", "wc", "migrate-to-chasm", "--deployment-name", "dep", "--build-id", "b1")
	require.NoError(t, err)
	require.Equal(t, adminservice.MigrateWorkerControllerInstanceRequest_TARGET_CHASM, admin.req.GetTarget())
	require.Equal(t, "dep", admin.req.GetDeploymentName())
	require.Equal(t, "b1", admin.req.GetBuildId())
	require.Contains(t, stdout, "to CHASM")
}

func TestListWorkerControllerInstances(t *testing.T) {
	admin := &wciAdminClient{}
	stdout, _, err := runMigrate(t, migrateClientFactory{admin: admin}, "-n", "my-ns", "wc", "list")
	require.NoError(t, err)
	require.Equal(t, "my-ns", admin.listReq.GetNamespace())
	require.Empty(t, admin.listReq.GetQuery(), "the server applies the running-only default")
	for _, want := range []string{"wci-id", "Running", "Succeeded", "aws-lambda,gcp-cloud-run", "dep:b1", "LASTUPDATETIME", "2026-01-02 03:04:05"} {
		require.Contains(t, stdout, want)
	}

	_, _, err = runMigrate(t, migrateClientFactory{admin: admin}, "-n", "my-ns", "wc", "list", "-q", "WorkerDeploymentName = 'dep'", "--print-json")
	require.NoError(t, err)
	require.Equal(t, "WorkerDeploymentName = 'dep'", admin.listReq.GetQuery())
}
