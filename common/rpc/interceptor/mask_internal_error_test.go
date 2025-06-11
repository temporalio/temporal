package interceptor

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMaskUnknownOrInternalErrors(t *testing.T) {

	statusOk := status.New(codes.OK, "OK")
	testMaskUnknownOrInternalErrors(t, statusOk, false)

	statusUnknown := status.New(codes.Unknown, "Unknown")
	testMaskUnknownOrInternalErrors(t, statusUnknown, true)

	statusInternal := status.New(codes.Internal, "Internal")
	testMaskUnknownOrInternalErrors(t, statusInternal, true)
}

func testMaskUnknownOrInternalErrors(t *testing.T, st *status.Status, expectRelpace bool) {
	controller := gomock.NewController(t)
	mockRegistry := namespace.NewMockRegistry(controller)
	mockLogger := log.NewMockLogger(controller)
	dc := dynamicconfig.NewNoopCollection()
	errorMaskInterceptor := NewMaskInternalErrorDetailsInterceptor(
		dynamicconfig.FrontendMaskInternalErrorDetails.Get(dc), mockRegistry, mockLogger)

	err := serviceerror.FromStatus(st)
	if expectRelpace {
		mockLogger.EXPECT().Error(gomock.Any(), gomock.Any()).Times(1)
	}
	errorMessage := errorMaskInterceptor.maskUnknownOrInternalErrors(nil, "test", err)
	if expectRelpace {
		errorHash := common.ErrorHash(err)
		expectedMessage := fmt.Sprintf("rpc error: code = %s desc = %s (%s)", st.Message(), errorFrontendMasked, errorHash)

		assert.Equal(t, expectedMessage, errorMessage.Error())
	} else {
		if err == nil {
			assert.Equal(t, errorMessage, nil)
		} else {
			assert.Equal(t, errorMessage.Error(), st.Message())
		}
	}
}

func TestMaskInternalErrorDetailsInterceptor(t *testing.T) {

	controller := gomock.NewController(t)
	mockRegistry := namespace.NewMockRegistry(controller)
	dc := dynamicconfig.NewNoopCollection()
	mockLogger := log.NewMockLogger(controller)

	errorMask := NewMaskInternalErrorDetailsInterceptor(
		dynamicconfig.FrontendMaskInternalErrorDetails.Get(dc), mockRegistry, mockLogger)

	test_namespace := "test-namespace"
	req := &workflowservice.StartWorkflowExecutionRequest{Namespace: test_namespace}
	mockRegistry.EXPECT().GetNamespace(namespace.Name(test_namespace)).Return(&namespace.Namespace{}, nil).AnyTimes()
	assert.True(t, errorMask.shouldMaskErrors(req))

	namespace_not_found := "namespace-not-found"
	req = &workflowservice.StartWorkflowExecutionRequest{Namespace: namespace_not_found}
	mockRegistry.EXPECT().GetNamespace(namespace.Name(namespace_not_found)).Return(nil, serviceerror.NewNamespaceNotFound("missing-namespace"))
	assert.False(t, errorMask.shouldMaskErrors(req))

	empty_namespace := ""
	req = &workflowservice.StartWorkflowExecutionRequest{Namespace: empty_namespace}
	mockRegistry.EXPECT().GetNamespace(namespace.Name(empty_namespace)).Return(nil, serviceerror.NewNamespaceNotFound("missing-namespace"))
	assert.False(t, errorMask.shouldMaskErrors(req))

	var ei interface{}
	assert.False(t, errorMask.shouldMaskErrors(ei))
}
