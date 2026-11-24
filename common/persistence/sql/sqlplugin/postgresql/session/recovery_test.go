package session

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql/driver"
	"go.temporal.io/server/common/resolver"
	"go.uber.org/mock/gomock"
)

func TestSessionRetriesCanceledConnections(t *testing.T) {
	t.Parallel()
	for _, backend := range []struct {
		name   string
		driver driver.Driver
	}{
		{name: "pq", driver: &driver.PQDriver{}},
		{name: "pgx", driver: &driver.PGXDriver{}},
	} {
		t.Run(backend.name, func(t *testing.T) {
			t.Parallel()
			for _, credentials := range []string{"static", "password command"} {
				t.Run(credentials, func(t *testing.T) {
					t.Parallel()
					cfg := &config.SQL{
						User: "temporal", Password: "password", ConnectAddr: "database:5432", DatabaseName: "temporal",
						MaxConns: 3, MaxIdleConns: 2, MaxConnLifetime: time.Minute,
					}
					if credentials == "password command" {
						cfg.Password = ""
						cfg.PasswordCommand = &config.PasswordCommandConfig{Command: "/usr/bin/printf", Args: []string{"password"}}
					}
					r := resolver.NewMockServiceResolver(gomock.NewController(t))
					session, err := NewSession(cfg, backend.driver, r)
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
							return []string{"first:invalid-port"}
						}),
						r.EXPECT().Resolve(cfg.ConnectAddr).DoAndReturn(func(string) []string {
							cancelSecond()
							return []string{"second:invalid-port"}
						}),
					)
					require.Error(t, session.PingContext(firstCtx))
					require.Error(t, session.PingContext(secondCtx))
					require.Equal(t, 0, session.Stats().OpenConnections)
				})
			}
		})
	}
}
