package cassandra

import (
	"crypto/tls"
	"strings"

	"github.com/gocql/gocql"

	"github.com/temporalio/temporal/common/service/config"
)

// NewCassandraCluster creates a cassandra cluster from a given configuration
func NewCassandraCluster(cfg config.Cassandra) *gocql.ClusterConfig {
	hosts := parseHosts(cfg.Hosts)
	cluster := gocql.NewCluster(hosts...)
	cluster.ProtoVersion = 4
	if cfg.Port > 0 {
		cluster.Port = cfg.Port
	}
	if cfg.User != "" && cfg.Password != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: cfg.User,
			Password: cfg.Password,
		}
	}
	if cfg.Keyspace != "" {
		cluster.Keyspace = cfg.Keyspace
	}
	if cfg.Datacenter != "" {
		cluster.HostFilter = gocql.DataCentreHostFilter(cfg.Datacenter)
	}
	if cfg.TLS != nil && cfg.TLS.Enabled {
		cluster.SslOpts = &gocql.SslOptions{
			CertPath:               cfg.TLS.CertFile,
			KeyPath:                cfg.TLS.KeyFile,
			CaPath:                 cfg.TLS.CaFile,
			EnableHostVerification: cfg.TLS.EnableHostVerification,

			Config: &tls.Config{
				ServerName: cfg.TLS.ServerName,
			},
		}
	}
	if cfg.MaxConns > 0 {
		cluster.NumConns = cfg.MaxConns
	}

	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())

	return cluster
}

func parseHosts(input string) []string {
	var hosts = make([]string, 0)
	for _, h := range strings.Split(input, ",") {
		if host := strings.TrimSpace(h); len(host) > 0 {
			hosts = append(hosts, host)
		}
	}
	return hosts
}
