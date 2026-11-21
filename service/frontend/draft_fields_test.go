package frontend

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/rpc/interceptor"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
)

func TestDraftFieldsCoverFrontendServices(t *testing.T) {
	t.Parallel()
	filter := interceptor.NewDraftFieldsInterceptor(nil, nil, nil, nil)
	excluded := map[string]struct{}{
		"temporal.server.api.adminservice.v1.AdminService":       {},
		"temporal.server.api.historyservice.v1.HistoryService":   {},
		"temporal.server.api.matchingservice.v1.MatchingService": {},
		"grpc.health.v1.Health":                                  {},
		"grpc.reflection.v1.ServerReflection":                    {},
		"grpc.reflection.v1alpha.ServerReflection":               {},
	}
	for name := range registeredFrontendGRPCServices(t) {
		_, explicitlyExcluded := excluded[name]
		require.NotEqual(t, explicitlyExcluded, filter.CoversService(name), "frontend service %s must have draft coverage or an explicit exclusion", name)
	}
}

func TestDraftFieldsFrontendServicesAreUnary(t *testing.T) {
	t.Parallel()
	filter := interceptor.NewDraftFieldsInterceptor(nil, nil, nil, nil)
	for name, service := range registeredFrontendGRPCServices(t) {
		if !filter.CoversService(name) {
			continue
		}
		for _, method := range service.Methods {
			require.False(t, method.IsClientStream || method.IsServerStream,
				"%s/%s is streaming. The draft filter only covers unary calls. A stream interceptor must be added first.", name, method.Name)
		}
	}
}

func registeredFrontendGRPCServices(t *testing.T) map[string]grpc.ServiceInfo {
	t.Helper()
	s := &Service{
		server:          grpc.NewServer(),
		healthServer:    health.NewServer(),
		handler:         &WorkflowHandler{},
		adminHandler:    &AdminHandler{},
		operatorHandler: &OperatorHandlerImpl{},
	}
	t.Cleanup(s.server.Stop)
	s.registerGRPCServices()

	return s.server.GetServiceInfo()
}
