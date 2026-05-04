package auth

type (
	// TLS describe TLS configuration (for Cassandra, SQL)
	TLS struct {
		Enabled bool `yaml:"enabled"`

		// CertPath and KeyPath are optional depending on server
		// config, but both fields must be omitted to avoid using a
		// client certificate
		CertFile string `yaml:"certFile"`
		KeyFile  string `yaml:"keyFile"`
		CaFile   string `yaml:"caFile"` // optional depending on server config

		// If you want to verify the hostname and server cert (like a wildcard for cass cluster) then you should turn this on
		// This option is basically the inverse of InSecureSkipVerify
		// See InSecureSkipVerify in http://golang.org/pkg/crypto/tls/ for more info
		EnableHostVerification bool `yaml:"enableHostVerification"`

		ServerName string `yaml:"serverName"`

		// Base64 equivalents of the above artifacts.
		// You cannot specify both a Data and a File for the same artifact (e.g. setting CertFile and CertData)
		CertData string `yaml:"certData"`
		KeyData  string `yaml:"keyData"`
		CaData   string `yaml:"caData"` // optional depending on server config

		// CipherSuites is an optional list of TLS cipher suite names to restrict TLS 1.0–1.2 connections.
		// When empty, Go's default secure cipher suites are used (3DES and other weak ciphers are excluded).
		// Cipher suite names follow Go's naming convention (e.g. "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256").
		// This field has no effect on TLS 1.3, whose cipher suites are not configurable in Go.
		CipherSuites []string `yaml:"cipherSuites"`
	}
)
