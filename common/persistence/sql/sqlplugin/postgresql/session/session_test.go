package session

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/config"
)

func TestBuildDSNAttr_NoTLS_NoConnectAttributes(t *testing.T) {
	cfg := &config.SQL{}
	result := buildDSNAttr(cfg)
	require.Equal(t, "disable", result.Get(sslMode), "should default to sslmode=disable when no TLS and no connect attributes")
}

func TestBuildDSNAttr_TLSEnabled_NoHostVerification(t *testing.T) {
	cfg := &config.SQL{
		TLS: &auth.TLS{
			Enabled:                true,
			EnableHostVerification: false,
		},
	}
	result := buildDSNAttr(cfg)
	require.Equal(t, "require", result.Get(sslMode), "should set sslmode=require when TLS enabled without host verification")
}

func TestBuildDSNAttr_TLSEnabled_WithHostVerification(t *testing.T) {
	cfg := &config.SQL{
		TLS: &auth.TLS{
			Enabled:                true,
			EnableHostVerification: true,
		},
	}
	result := buildDSNAttr(cfg)
	require.Equal(t, "verify-full", result.Get(sslMode), "should set sslmode=verify-full when TLS enabled with host verification")
}

func TestBuildDSNAttr_TLSEnabled_WithCertificates(t *testing.T) {
	cfg := &config.SQL{
		TLS: &auth.TLS{
			Enabled:                true,
			EnableHostVerification: true,
			CaFile:                 "/path/to/ca.crt",
			CertFile:               "/path/to/client.crt",
			KeyFile:                "/path/to/client.key",
		},
	}
	result := buildDSNAttr(cfg)
	require.Equal(t, "verify-full", result.Get(sslMode))
	require.Equal(t, "/path/to/ca.crt", result.Get(sslCA))
	require.Equal(t, "/path/to/client.crt", result.Get(sslCert))
	require.Equal(t, "/path/to/client.key", result.Get(sslKey))
}

func TestBuildDSNAttr_CustomSSLMode_PreferredOverDisable(t *testing.T) {
	cfg := &config.SQL{
		ConnectAttributes: map[string]string{
			"sslmode": "verify-ca",
		},
	}
	result := buildDSNAttr(cfg)
	require.Equal(t, "verify-ca", result.Get(sslMode), "should use custom sslmode instead of default disable")
}

func TestBuildDSNAttr_TLSEnabledButCustomSSLModeInAttributes_PreferredOverDisable(t *testing.T) {
	cfg := &config.SQL{
		TLS: &auth.TLS{
			Enabled:                true,
			EnableHostVerification: true,
		},
		ConnectAttributes: map[string]string{
			"sslmode": "verify-ca",
		},
	}
	result := buildDSNAttr(cfg)
	require.Equal(t, "verify-ca", result.Get(sslMode), "should use custom sslmode instead of default disable")
}

func TestBuildDSNAttr_ConnectAttributes(t *testing.T) {
	cfg := &config.SQL{
		ConnectAttributes: map[string]string{
			"connect_timeout":  "10",
			"application_name": "temporal",
		},
	}
	result := buildDSNAttr(cfg)
	require.Equal(t, "10", result.Get("connect_timeout"))
	require.Equal(t, "temporal", result.Get("application_name"))
	require.Equal(t, "disable", result.Get(sslMode), "should still set default sslmode")
}

func TestBuildDSNAttr_ConnectAttributesWithSpaces(t *testing.T) {
	cfg := &config.SQL{
		ConnectAttributes: map[string]string{
			"  application_name  ": "  temporal  ",
		},
	}
	result := buildDSNAttr(cfg)
	require.Equal(t, "temporal", result.Get("application_name"), "should trim spaces from key and value")
}

func TestBuildDSNAttr_DuplicateConnectAttribute_Panics(t *testing.T) {
	cfg := &config.SQL{
		TLS: &auth.TLS{
			Enabled:                true,
			EnableHostVerification: true,
		},
		ConnectAttributes: map[string]string{
			"sslmode":  "require",
			"sslmode ": "verify-full",
		},
	}
	require.Panics(t, func() {
		buildDSNAttr(cfg)
	}, "Should panic when duplicate keys are detected")
}

func TestBuildDSN(t *testing.T) {
	cfg := &config.SQL{
		User:         "testuser",
		Password:     "testpass",
		ConnectAddr:  "localhost:5432",
		DatabaseName: "testdb",
		ConnectAttributes: map[string]string{
			"connect_timeout": "10",
		},
	}
	mockResolver := &mockServiceResolver{addr: "localhost:5432"}
	result := buildDSN(cfg, mockResolver)
	u, err := url.Parse(result)
	require.NoError(t, err)
	require.Equal(t, "postgres", u.Scheme)
	require.Equal(t, "testuser:testpass", u.User.String())
	require.Equal(t, "localhost:5432", u.Host)
	require.Equal(t, "/testdb", u.Path)
	require.Equal(t, "10", u.Query().Get("connect_timeout"))
	require.Equal(t, "disable", u.Query().Get("sslmode"))
}

func TestBuildDSN_PasswordEscaping(t *testing.T) {
	cfg := &config.SQL{
		User:         "testuser",
		Password:     "p@ss:w/rd&special",
		ConnectAddr:  "localhost:5432",
		DatabaseName: "testdb",
	}
	mockResolver := &mockServiceResolver{addr: "localhost:5432"}
	result := buildDSN(cfg, mockResolver)
	parsed, err := url.Parse(result)
	require.NoError(t, err)
	password, _ := parsed.User.Password()
	require.Equal(t, "p@ss:w/rd&special", password, "password should be properly escaped and parsed")
}

type mockServiceResolver struct {
	addr string
}

func (m *mockServiceResolver) Resolve(addr string) []string {
	return []string{m.addr}
}
