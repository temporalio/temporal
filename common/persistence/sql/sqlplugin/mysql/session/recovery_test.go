package session

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/resolver"
	"go.uber.org/mock/gomock"
)

func TestSessionRetriesCanceledConnections(t *testing.T) {
	t.Parallel()
	for _, credentials := range []string{"static", "password command"} {
		t.Run(credentials, func(t *testing.T) {
			t.Parallel()
			cfg := &config.SQL{
				User: "temporal", Password: "password", ConnectAddr: "database:3306",
				ConnectProtocol: "unregistered-network", DatabaseName: "temporal",
				MaxConns: 3, MaxIdleConns: 2, MaxConnLifetime: time.Minute,
			}
			if credentials == "password command" {
				cfg.Password = ""
				cfg.PasswordCommand = &config.PasswordCommandConfig{Command: "/usr/bin/printf", Args: []string{"password"}}
			}
			r := resolver.NewMockServiceResolver(gomock.NewController(t))
			session, err := NewSession(sqlplugin.DbKindMain, cfg, r)
			require.NoError(t, err)
			t.Cleanup(session.Close)
			require.Equal(t, 0, session.Stats().OpenConnections)
			require.Equal(t, 3, session.Stats().MaxOpenConnections)
			firstCtx, cancelFirst := context.WithCancel(t.Context())
			defer cancelFirst()
			secondCtx, cancelSecond := context.WithCancel(t.Context())
			defer cancelSecond()
			gomock.InOrder(
				r.EXPECT().Resolve(cfg.ConnectAddr).DoAndReturn(func(string) []string {
					cancelFirst()
					return []string{"first:3306"}
				}),
				r.EXPECT().Resolve(cfg.ConnectAddr).DoAndReturn(func(string) []string {
					cancelSecond()
					return []string{"second:3306"}
				}),
			)
			require.Error(t, session.PingContext(firstCtx))
			require.Error(t, session.PingContext(secondCtx))
			require.Equal(t, 0, session.Stats().OpenConnections)
		})
	}
}

func TestSessionReloadsTLSOnPhysicalConnect(t *testing.T) {
	t.Parallel()
	certificate, err := os.ReadFile("../../../../../auth/testdata/ca.crt")
	require.NoError(t, err)
	caFile := filepath.Join(t.TempDir(), "ca.crt")
	require.NoError(t, os.WriteFile(caFile, certificate, 0600))
	attributes := map[string]string{"tls": customTLSName}
	cfg := &config.SQL{
		User: "temporal", Password: "password", ConnectAddr: "database:3306",
		ConnectProtocol: "unregistered-network", DatabaseName: "temporal",
		ConnectAttributes: attributes,
		TLS:               &auth.TLS{Enabled: true, CaFile: caFile},
	}
	r := resolver.NewMockServiceResolver(gomock.NewController(t))
	session, err := NewSession(sqlplugin.DbKindMain, cfg, r)
	require.NoError(t, err)
	t.Cleanup(session.Close)
	ping := func() error {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		r.EXPECT().Resolve(cfg.ConnectAddr).DoAndReturn(func(string) []string {
			cancel()
			return []string{cfg.ConnectAddr}
		})
		return session.PingContext(ctx)
	}
	require.Error(t, ping())
	require.NoError(t, os.WriteFile(caFile, []byte("invalid certificate"), 0600))
	require.ErrorContains(t, ping(), "failed to append CA file")
	require.NoError(t, os.WriteFile(caFile, certificate, 0600))
	require.Error(t, ping())
	require.Equal(t, map[string]string{"tls": customTLSName}, cfg.ConnectAttributes)
}
