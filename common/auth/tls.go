package auth

type (
	// TLS describe TLS configuration (for Kafka, Cassandra, SQL)
	TLS struct {
		Enabled bool `yaml:"enabled"`

		// CertPath and KeyPath are optional depending on server
		// config, but both fields must be omitted to avoid using a
		// client certificate
		CertFile string `yaml:"certFile"`
		KeyFile  string `yaml:"keyFile"`

		CaFile string `yaml:"caFile"` //optional depending on server config
		// If you want to verify the hostname and server cert (like a wildcard for cass cluster) then you should turn this on
		// This option is basically the inverse of InSecureSkipVerify
		// See InSecureSkipVerify in http://golang.org/pkg/crypto/tls/ for more info
		EnableHostVerification bool `yaml:"enableHostVerification"`

		ServerName string `yaml:"serverName"`
	}
)
