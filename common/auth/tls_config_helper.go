package auth

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"os"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

var ErrTLSConfig = errors.New("unable to config TLS")

// Helper methods for creating tls.Config structs to ensure MinVersion is 1.3

func NewEmptyTLSConfig() *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		NextProtos: []string{
			"h2",
		},
	}
}

func NewTLSConfigForServer(
	serverName string,
	enableHostVerification bool,
) *tls.Config {
	c := NewEmptyTLSConfig()
	c.ServerName = serverName
	c.InsecureSkipVerify = !enableHostVerification
	return c
}

// NewTLSConfigForServerWithRootCAProvider creates a client TLS configuration
// that resolves its root CA pool through getRootCAs on every new handshake
// instead of freezing a pool at construction time. Go has no client-side
// per-handshake RootCAs hook, so host-verifying configurations disable the
// built-in verifier and perform the equivalent chain and identity checks in
// VerifyConnection with the freshly resolved pool.
//
// Contract:
//   - enableHostVerification=false returns the legacy no-verification config;
//     getRootCAs is ignored entirely.
//   - a nil getRootCAs fails every host-verifying handshake (fail closed).
//   - a nil pool returned by getRootCAs falls back to the system trust store,
//     matching the prior static-config behavior.
//   - ConnectionState.VerifiedChains is not populated for these connections;
//     the chain built in VerifyConnection is checked and discarded.
func NewTLSConfigForServerWithRootCAProvider(
	serverName string,
	enableHostVerification bool,
	getRootCAs func() (*x509.CertPool, error),
) *tls.Config {
	c := NewTLSConfigForServer(serverName, enableHostVerification)
	if !enableHostVerification {
		return c
	}

	// Required to bypass the frozen RootCAs field. VerifyConnection below performs
	// the full chain and identity verification using the current cached CA pool.
	// Note: with InsecureSkipVerify set, crypto/tls does not populate
	// ConnectionState.VerifiedChains, so downstream consumers must not rely on
	// it; the chain built in VerifyConnection is checked and discarded.
	c.InsecureSkipVerify = true
	c.VerifyConnection = func(state tls.ConnectionState) error {
		if len(state.PeerCertificates) == 0 {
			return errors.New("tls: server presented no certificates")
		}
		if getRootCAs == nil {
			return errors.New("tls: root CA provider is not configured")
		}

		rootCAs, err := getRootCAs()
		if err != nil {
			return fmt.Errorf("tls: failed to load root CAs for verification: %w", err)
		}

		verifyName := state.ServerName
		if verifyName == "" {
			verifyName = serverName
		}
		if verifyName == "" {
			return errors.New("tls: cannot verify server identity: connection provided no SNI and no ServerName is configured")
		}

		opts := x509.VerifyOptions{
			Roots:         rootCAs,
			Intermediates: x509.NewCertPool(),
			DNSName:       verifyName,
		}
		for _, cert := range state.PeerCertificates[1:] {
			opts.Intermediates.AddCert(cert)
		}

		_, err = state.PeerCertificates[0].Verify(opts)
		return err
	}

	return c
}

func NewDynamicTLSClientConfig(
	getCert func() (*tls.Certificate, error),
	rootCAs *x509.CertPool,
	serverName string,
	enableHostVerification bool,
) *tls.Config {
	c := NewTLSConfigForServer(serverName, enableHostVerification)

	if getCert != nil {
		c.GetClientCertificate = func(info *tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return getCert()
		}
	}
	c.RootCAs = rootCAs

	return c
}

func NewTLSConfigWithCertsAndCAs(
	clientAuth tls.ClientAuthType,
	certificates []tls.Certificate,
	clientCAs *x509.CertPool,
	logger log.Logger,
) *tls.Config {
	c := NewEmptyTLSConfig()
	c.ClientAuth = clientAuth
	c.Certificates = certificates
	c.ClientCAs = clientCAs
	c.VerifyConnection = func(state tls.ConnectionState) error {
		logger.Debug("successfully established incoming TLS connection", tag.ServerName(state.ServerName), tag.Name(tlsCN(state)))
		return nil
	}
	return c
}

func tlsCN(state tls.ConnectionState) string {

	if len(state.PeerCertificates) == 0 {
		return ""
	}
	return state.PeerCertificates[0].Subject.CommonName
}

func NewTLSConfig(temporalTls *TLS) (*tls.Config, error) {
	if temporalTls == nil || !temporalTls.Enabled {
		return nil, nil
	}
	err := validateTemporalTls(temporalTls)
	if err != nil {
		return nil, err
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: !temporalTls.EnableHostVerification,
	}
	if temporalTls.ServerName != "" {
		tlsConfig.ServerName = temporalTls.ServerName
	}

	// Load CA cert
	caCertPool, err := parseCAs(temporalTls)
	if err != nil {
		return nil, err
	}
	if caCertPool != nil {
		tlsConfig.RootCAs = caCertPool
	}

	// Load client cert
	clientCert, err := parseClientCert(temporalTls)
	if err != nil {
		return nil, err
	}
	if clientCert != nil {
		tlsConfig.Certificates = []tls.Certificate{*clientCert}
	}

	return tlsConfig, nil
}

func validateTemporalTls(temporalTls *TLS) error {
	if temporalTls.CertData != "" && temporalTls.CertFile != "" {
		return fmt.Errorf("%w: %s", ErrTLSConfig, "only one of certData or certFile properties should be specified")
	}

	if temporalTls.KeyData != "" && temporalTls.KeyFile != "" {
		return fmt.Errorf("%w: %s", ErrTLSConfig, "only one of keyData or keyFile properties should be specified")
	}

	certProvided := temporalTls.CertData != "" || temporalTls.CertFile != ""
	keyProvided := temporalTls.KeyData != "" || temporalTls.KeyFile != ""
	if certProvided != keyProvided {
		return fmt.Errorf("%w: %s", ErrTLSConfig, "cert or key is missing")
	}

	if temporalTls.CaData != "" && temporalTls.CaFile != "" {
		return fmt.Errorf("%w: %s", ErrTLSConfig, "only one of caData or caFile properties should be specified")
	}
	return nil
}

func parseCAs(temporalTls *TLS) (*x509.CertPool, error) {
	var caBytes []byte
	var err error
	if temporalTls.CaFile != "" {
		caBytes, err = os.ReadFile(temporalTls.CaFile)
		if err != nil {
			return nil, fmt.Errorf("%w: %s (%w)", ErrTLSConfig, "unable to read client ca file", err)
		}
	} else if temporalTls.CaData != "" {
		caBytes, err = base64.StdEncoding.DecodeString(temporalTls.CaData)
		if err != nil {
			return nil, fmt.Errorf("%w: %s (%w)", ErrTLSConfig, "unable to decode client ca data", err)
		}
	}
	if len(caBytes) > 0 {
		caCertPool := x509.NewCertPool()
		caCerts, err := parseCertsFromPEM(caBytes)
		if len(caCerts) == 0 {
			return nil, fmt.Errorf("%w: %s (%w)", ErrTLSConfig, "unable to parse certs as PEM", err)
		}
		for _, cert := range caCerts {
			caCertPool.AddCert(cert)
		}
		if err != nil {
			return nil, fmt.Errorf("%w: %s (%w)", ErrTLSConfig, "unable to load decoded CA Cert as PEM", err)
		}
		return caCertPool, nil
	}
	return nil, nil
}

func parseCertsFromPEM(pemCerts []byte) ([]*x509.Certificate, error) {
	for len(pemCerts) > 0 {
		var block *pem.Block
		block, pemCerts = pem.Decode(pemCerts)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			continue
		}

		certBytes := block.Bytes
		return x509.ParseCertificates(certBytes)
	}
	return nil, nil
}

func parseClientCert(temporalTls *TLS) (*tls.Certificate, error) {
	var certBytes []byte
	var keyBytes []byte
	var err error
	if temporalTls.CertFile != "" {
		certBytes, err = os.ReadFile(temporalTls.CertFile)
		if err != nil {
			return nil, fmt.Errorf("%w: %s (%w)", ErrTLSConfig, "unable to read client certificate file", err)
		}
	} else if temporalTls.CertData != "" {
		certBytes, err = base64.StdEncoding.DecodeString(temporalTls.CertData)
		if err != nil {
			return nil, fmt.Errorf("%w: %s (%w)", ErrTLSConfig, "unable to decode client certificate", err)
		}
	}

	if temporalTls.KeyFile != "" {
		keyBytes, err = os.ReadFile(temporalTls.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("%w: %s (%w)", ErrTLSConfig, "unable to read client certificate private key file", err)
		}
	} else if temporalTls.KeyData != "" {
		keyBytes, err = base64.StdEncoding.DecodeString(temporalTls.KeyData)
		if err != nil {
			return nil, fmt.Errorf("%w: %s (%w)", ErrTLSConfig, "unable to decode client certificate private key", err)
		}
	}

	if len(certBytes) > 0 {
		clientCert, err := tls.X509KeyPair(certBytes, keyBytes)
		if err != nil {
			return nil, fmt.Errorf("%w: %s (%w)", ErrTLSConfig, "unable to generate x509 key pair", err)
		}

		return &clientCert, nil
	}
	return nil, nil
}
