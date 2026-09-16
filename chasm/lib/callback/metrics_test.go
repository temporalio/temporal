package callback

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGRPCErrorOutcome(t *testing.T) {
	cases := []struct {
		Name string
		E    error
		Want string
	}{
		{
			"status.Error",
			status.Error(codes.Unavailable, "matching unavailable"),
			"error:Unavailable",
		},
		{
			"NotAGRPCError",
			fmt.Errorf("doesn't implement %w", errors.New("the gRPC interfaces")),
			"error:Unknown",
		},
		{
			"WrappedGRPCError",
			fmt.Errorf("first: %w", fmt.Errorf("second: %w", status.Error(codes.Internal, "third"))),
			"error:Internal",
		},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			got := grpcErrorOutcome(tc.E)
			require.EqualValues(t, tc.Want, got)
		})
	}
}
