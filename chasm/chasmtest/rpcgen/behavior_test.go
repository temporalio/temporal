package rpcgen_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm/chasmtest/rpcgen"
	"go.temporal.io/server/chasm/chasmtest/rpctest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestBehaviorInstallsExplicitTypedExpectation(t *testing.T) {
	var contract rpctest.RPCContract
	rpcgen.Retryable[*wrapperspb.StringValue, *wrapperspb.StringValue](codes.Unavailable).Expect(&contract, "test", "retry", func(request *wrapperspb.StringValue) bool {
		return request.GetValue() == "first"
	})
	rpcgen.AmbiguousCommit[*wrapperspb.StringValue](wrapperspb.String("committed")).Expect(&contract, "test", "commit", func(request *wrapperspb.StringValue) bool {
		return request.GetValue() == "second"
	})

	_, err := contract.Invoke(context.Background(), "test", wrapperspb.String("first"))
	require.Equal(t, codes.Unavailable, status.Code(err))
	response, err := contract.Invoke(context.Background(), "test", wrapperspb.String("second"))
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Equal(t, "committed", response.(*wrapperspb.StringValue).GetValue())
	require.NoError(t, contract.AssertSatisfied())
}

func TestTimeoutObservesCallerCancellation(t *testing.T) {
	var contract rpctest.RPCContract
	rpcgen.Timeout[*wrapperspb.StringValue, *wrapperspb.StringValue]().Expect(&contract, "test", "timeout", nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := contract.Invoke(ctx, "test", wrapperspb.String("request"))
	require.ErrorIs(t, err, context.Canceled)
}
