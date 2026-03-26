package rpc

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/testing/testlogger"
)

func TestConfigListenerProvider(t *testing.T) {
	cfg := &config.Config{
		Services: map[string]config.Service{
			"service-name": config.Service{
				RPC: config.RPC{
					// use random ports in testing
					GRPCPort:       0,
					MembershipPort: 0,
				},
			},
		},
	}
	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	provider := NewConfigListenerProvider(cfg, "service-name", logger)

	grpcListener := provider.GetGRPCListener()
	defer func() { _ = grpcListener.Close() }()
	membershipListener := provider.GetMembershipListener()
	defer func() { _ = membershipListener.Close() }()

	require.NotEmpty(t, grpcListener.Addr())
	require.NotEmpty(t, membershipListener.Addr())
	require.NotEqual(t, grpcListener.Addr(), membershipListener.Addr())
}
