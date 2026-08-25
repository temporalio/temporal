package tests

import (
	"testing"

	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
)

type RPCFaultInjectionSuite struct {
	parallelsuite.Suite[*RPCFaultInjectionSuite]
}

func TestRPCFaultInjectionSuite(t *testing.T) {
	parallelsuite.Run(t, &RPCFaultInjectionSuite{})
}

func (s *RPCFaultInjectionSuite) TestResponseFaultAfterHandler() {
	env := testcore.NewEnv(s.T())
	description := "updated before the response fault"
	unregister := env.InjectRPCResponseFault(func(req, resp any, handlerErr error) error {
		if _, ok := req.(*workflowservice.UpdateNamespaceRequest); !ok || resp == nil || handlerErr != nil {
			return nil
		}
		return serviceerror.NewUnavailable("injected response fault")
	})

	_, err := env.FrontendClient().UpdateNamespace(s.Context(), &workflowservice.UpdateNamespaceRequest{
		Namespace: env.Namespace().String(),
		UpdateInfo: &namespacepb.UpdateNamespaceInfo{
			Description: description,
		},
	})
	s.Require().ErrorContains(err, "injected response fault")
	unregister()

	response, err := env.FrontendClient().DescribeNamespace(s.Context(), &workflowservice.DescribeNamespaceRequest{
		Namespace: env.Namespace().String(),
	})
	s.Require().NoError(err)
	s.Require().Equal(description, response.GetNamespaceInfo().GetDescription())
}
