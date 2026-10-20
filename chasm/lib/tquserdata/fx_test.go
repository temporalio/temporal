package tquserdata

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"google.golang.org/grpc"
)

func TestFrontendModuleRegistersComponentWithoutServices(t *testing.T) {
	t.Parallel()
	registry := chasm.NewRegistry(log.NewNoopLogger())
	app := fxtest.New(t, fx.Supply(registry), FrontendModule)
	app.RequireStart()
	t.Cleanup(app.RequireStop)

	_, ok := registry.ComponentIDByFqn("taskQueueUserData.taskQueueUserData")
	require.True(t, ok)

	server := grpc.NewServer()
	t.Cleanup(server.Stop)
	registry.RegisterServices(server)
	require.Empty(t, server.GetServiceInfo())
}
