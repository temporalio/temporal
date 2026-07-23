package rpcgen_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm/chasmtest/rpcgen"
	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"
)

func TestUnaryMethodGeneratesTypedTransportMessages(t *testing.T) {
	descriptor := historyservice.File_temporal_server_api_historyservice_v1_service_proto.Services().ByName("HistoryService").Methods().ByName("DescribeWorkflowExecution")
	method, err := rpcgen.NewUnaryMethod(descriptor, func() *historyservice.DescribeWorkflowExecutionRequest {
		return &historyservice.DescribeWorkflowExecutionRequest{}
	}, func() *historyservice.DescribeWorkflowExecutionResponse {
		return &historyservice.DescribeWorkflowExecutionResponse{}
	})
	require.NoError(t, err)
	require.Equal(t, "/temporal.server.api.historyservice.v1.HistoryService/DescribeWorkflowExecution", method.FullMethodName())
	request := method.RequestGenerator().Example(1)
	response := method.ResponseGenerator().Example(2)
	_, err = proto.Marshal(request)
	require.NoError(t, err)
	_, err = proto.Marshal(response)
	require.NoError(t, err)
}

func TestUnaryMethodGeneratorReplaysRapidDraws(t *testing.T) {
	descriptor := historyservice.File_temporal_server_api_historyservice_v1_service_proto.Services().ByName("HistoryService").Methods().ByName("DescribeWorkflowExecution")
	method, err := rpcgen.NewUnaryMethod(descriptor, func() *historyservice.DescribeWorkflowExecutionRequest {
		return &historyservice.DescribeWorkflowExecutionRequest{}
	}, func() *historyservice.DescribeWorkflowExecutionResponse {
		return &historyservice.DescribeWorkflowExecutionResponse{}
	})
	require.NoError(t, err)
	rapid.Check(t, func(t *rapid.T) {
		request := method.RequestGenerator().Draw(t, method.FullMethodName()+" request")
		require.NotNil(t, request)
	})
}
