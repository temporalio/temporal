package gocql

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/translator"
	"go.temporal.io/server/common/resolver"
)

func NewCassandraCluster(
	cfg config.Cassandra,
	resolver resolver.ServiceResolver,
) (*gocql.ClusterConfig, error) {
	var resolvedHosts []string
	for _, host := range parseHosts(cfg.Hosts) {
		resolvedHosts = append(resolvedHosts, resolver.Resolve(host)...)
	}

	cluster := gocql.NewCluster(resolvedHosts...)
	if err := ConfigureCassandraCluster(cfg, cluster); err != nil {
		return nil, err
	}

	return cluster, nil
}

// Modifies the input cluster config in place.
//
//nolint:revive // cognitive complexity 61 (> max enabled 25)
func ConfigureCassandraCluster(cfg config.Cassandra, cluster *gocql.ClusterConfig) error {
	cluster.ProtoVersion = 4
	if cfg.Port > 0 {
		cluster.Port = cfg.Port
	}
	if cfg.User != "" && cfg.Password != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username:              cfg.User,
			Password:              cfg.Password,
			AllowedAuthenticators: cfg.AllowedAuthenticators,
		}
	}
	if cfg.Keyspace != "" {
		cluster.Keyspace = cfg.Keyspace
	}
	if cfg.Datacenter != "" {
		cluster.HostFilter = gocql.DataCentreHostFilter(cfg.Datacenter)
	}
	if cfg.TLS != nil && cfg.TLS.Enabled {
		if cfg.TLS.CertData != "" && cfg.TLS.CertFile != "" {
			return errors.New("only one of certData or certFile properties should be specified")
		}

		if cfg.TLS.KeyData != "" && cfg.TLS.KeyFile != "" {
			return errors.New("only one of keyData or keyFile properties should be specified")
		}

		if cfg.TLS.CaData != "" && cfg.TLS.CaFile != "" {
			return errors.New("only one of caData or caFile properties should be specified")
		}

		cluster.SslOpts = &gocql.SslOptions{
			CaPath:                 cfg.TLS.CaFile,
			EnableHostVerification: cfg.TLS.EnableHostVerification,
			Config:                 auth.NewTLSConfigForServer(cfg.TLS.ServerName, cfg.TLS.EnableHostVerification),
		}

		var certBytes []byte
		var keyBytes []byte
		var err error

		if cfg.TLS.CertFile != "" {
			certBytes, err = os.ReadFile(cfg.TLS.CertFile)
			if err != nil {
				return fmt.Errorf("error reading client certificate file: %w", err)
			}
		} else if cfg.TLS.CertData != "" {
			certBytes, err = base64.StdEncoding.DecodeString(cfg.TLS.CertData)
			if err != nil {
				return fmt.Errorf("client certificate could not be decoded: %w", err)
			}
		}

		if cfg.TLS.KeyFile != "" {
			keyBytes, err = os.ReadFile(cfg.TLS.KeyFile)
			if err != nil {
				return fmt.Errorf("error reading client certificate private key file: %w", err)
			}
		} else if cfg.TLS.KeyData != "" {
			keyBytes, err = base64.StdEncoding.DecodeString(cfg.TLS.KeyData)
			if err != nil {
				return fmt.Errorf("client certificate private key could not be decoded: %w", err)
			}
		}

		if len(certBytes) > 0 {
			clientCert, err := tls.X509KeyPair(certBytes, keyBytes)
			if err != nil {
				return fmt.Errorf("unable to generate x509 key pair: %w", err)
			}

			cluster.SslOpts.Certificates = []tls.Certificate{clientCert}
		}

		if cfg.TLS.CaData != "" {
			cluster.SslOpts.RootCAs = x509.NewCertPool()
			pem, err := base64.StdEncoding.DecodeString(cfg.TLS.CaData)
			if err != nil {
				return fmt.Errorf("caData could not be decoded: %w", err)
			}
			if !cluster.SslOpts.RootCAs.AppendCertsFromPEM(pem) {
				return errors.New("failed to load decoded CA Cert as PEM")
			}
		}
	}

	if cfg.MaxConns > 0 {
		cluster.NumConns = cfg.MaxConns
	}

	cluster.ConnectTimeout = 10 * time.Second * debug.TimeoutMultiplier
	if cfg.ConnectTimeout > 0 {
		cluster.ConnectTimeout = cfg.ConnectTimeout
	}

	cluster.Timeout = cluster.ConnectTimeout
	if cfg.Timeout > 0 {
		cluster.Timeout = cfg.Timeout
	}

	cluster.WriteTimeout = cluster.Timeout
	if cfg.WriteTimeout > 0 {
		cluster.WriteTimeout = cfg.WriteTimeout
	}

	cluster.ProtoVersion = 4
	cluster.Consistency = cfg.Consistency.GetConsistency()
	cluster.SerialConsistency = cfg.Consistency.GetSerialConsistency()
	cluster.DisableInitialHostLookup = cfg.DisableInitialHostLookup

	cluster.ReconnectionPolicy = &gocql.ExponentialReconnectionPolicy{
		MaxRetries:      30,
		InitialInterval: time.Second,
		MaxInterval:     10 * time.Second,
	}

	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())

	if cfg.AddressTranslator != nil && cfg.AddressTranslator.Translator != "" {
		addressTranslator, err := translator.LookupTranslator(cfg.AddressTranslator.Translator)
		if err != nil {
			return err
		}
		cluster.AddressTranslator, err = addressTranslator.GetTranslator(&cfg)
		if err != nil {
			return err
		}
	}

	return nil
}

// parseHosts returns parses a list of hosts separated by comma
func parseHosts(input string) []string {
	var hosts []string
	for h := range strings.SplitSeq(input, ",") {
		if host := strings.TrimSpace(h); len(host) > 0 {
			hosts = append(hosts, host)
		}
	}
	return hosts
}
