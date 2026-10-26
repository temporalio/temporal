package tdbg_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/api/adminservice/v1"
	"google.golang.org/grpc"
)

type wciAdminClient struct {
	adminservice.AdminServiceClient
	req *adminservice.MigrateWorkerControllerInstanceRequest
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
