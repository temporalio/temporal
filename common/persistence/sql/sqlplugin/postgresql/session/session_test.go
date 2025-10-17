package session

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/config"
)

type (
	sessionTestSuite struct {
		suite.Suite
	}
)

func TestSessionTestSuite(t *testing.T) {
	s := new(sessionTestSuite)
	suite.Run(t, s)
}

func (s *sessionTestSuite) TestBuildDSNAttr_NoTLS_NoConnectAttributes() {
	cfg := &config.SQL{}

	result := buildDSNAttr(cfg)

	s.Equal("disable", result.Get(sslMode), "should default to sslmode=disable when no TLS and no connect attributes")
}

func (s *sessionTestSuite) TestBuildDSNAttr_TLSEnabled_NoHostVerification() {
	cfg := &config.SQL{
		TLS: &auth.TLS{
			Enabled:                true,
			EnableHostVerification: false,
		},
	}

	result := buildDSNAttr(cfg)

	s.Equal("require", result.Get(sslMode), "should set sslmode=require when TLS enabled without host verification")
}

func (s *sessionTestSuite) TestBuildDSNAttr_TLSEnabled_WithHostVerification() {
	cfg := &config.SQL{
		TLS: &auth.TLS{
			Enabled:                true,
			EnableHostVerification: true,
		},
	}

	result := buildDSNAttr(cfg)

	s.Equal("verify-full", result.Get(sslMode), "should set sslmode=verify-full when TLS enabled with host verification")
}

func (s *sessionTestSuite) TestBuildDSNAttr_TLSEnabled_WithCertificates() {
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

	s.Equal("verify-full", result.Get(sslMode))
	s.Equal("/path/to/ca.crt", result.Get(sslCA))
	s.Equal("/path/to/client.crt", result.Get(sslCert))
	s.Equal("/path/to/client.key", result.Get(sslKey))
}

func (s *sessionTestSuite) TestBuildDSNAttr_CustomSSLMode_PreferredOverDisable() {
	cfg := &config.SQL{
		ConnectAttributes: map[string]string{
			"sslmode": "verify-ca",
		},
	}

	result := buildDSNAttr(cfg)

	s.Equal("verify-ca", result.Get(sslMode), "should use custom sslmode instead of default disable")
}

func (s *sessionTestSuite) TestBuildDSNAttr_TLSEnabledButCustomSSLModeInAttributes_Panics() {
	// Tests that setting sslmode in ConnectAttributes when TLS is configured causes a panic
	// because TLS already sets the sslmode
	cfg := &config.SQL{
		TLS: &auth.TLS{
			Enabled:                true,
			EnableHostVerification: true,
		},
		ConnectAttributes: map[string]string{
			"sslmode": "require",
		},
	}

	// The current implementation panics when ConnectAttributes tries to set a parameter
	// that was already set by TLS configuration
	s.Panics(func() {
		buildDSNAttr(cfg)
	}, "Should panic when ConnectAttributes tries to set sslmode that was already set by TLS")
}

func (s *sessionTestSuite) TestBuildDSNAttr_ConnectAttributes() {
	cfg := &config.SQL{
		ConnectAttributes: map[string]string{
			"connect_timeout": "10",
			"application_name": "temporal",
		},
	}

	result := buildDSNAttr(cfg)

	s.Equal("10", result.Get("connect_timeout"))
	s.Equal("temporal", result.Get("application_name"))
	s.Equal("disable", result.Get(sslMode), "should still set default sslmode")
}

func (s *sessionTestSuite) TestBuildDSNAttr_ConnectAttributesWithSpaces() {
	cfg := &config.SQL{
		ConnectAttributes: map[string]string{
			"  application_name  ": "  temporal  ",
		},
	}

	result := buildDSNAttr(cfg)

	s.Equal("temporal", result.Get("application_name"), "should trim spaces from key and value")
}

func (s *sessionTestSuite) TestBuildDSNAttr_DuplicateConnectAttribute_Panics() {
	cfg := &config.SQL{
		TLS: &auth.TLS{
			Enabled:                true,
			EnableHostVerification: true,
		},
		ConnectAttributes: map[string]string{
			"sslmode": "require",
			"sslMode": "verify-full", // Different case but same after trimming
		},
	}

	// The current implementation checks for duplicates and panics
	// TLS sets sslmode first, then ConnectAttributes tries to set it again
	s.Panics(func() {
		buildDSNAttr(cfg)
	}, "Should panic when duplicate keys are detected")
}

func (s *sessionTestSuite) TestBuildDSN() {
	cfg := &config.SQL{
		User:         "testuser",
		Password:     "testpass",
		ConnectAddr:  "localhost:5432",
		DatabaseName: "testdb",
		ConnectAttributes: map[string]string{
			"connect_timeout": "10",
		},
	}

	mockResolver := &mockServiceResolver{
		addr: "localhost:5432",
	}

	result := buildDSN(cfg, mockResolver)

	// Parse the DSN to validate its structure
	s.Contains(result, "postgres://")
	s.Contains(result, "testuser:testpass@")
	s.Contains(result, "localhost:5432")
	s.Contains(result, "/testdb")
	s.Contains(result, "connect_timeout=10")
	s.Contains(result, "sslmode=disable")
}

func (s *sessionTestSuite) TestBuildDSN_PasswordEscaping() {
	cfg := &config.SQL{
		User:         "testuser",
		Password:     "p@ss:w/rd&special",
		ConnectAddr:  "localhost:5432",
		DatabaseName: "testdb",
	}

	mockResolver := &mockServiceResolver{
		addr: "localhost:5432",
	}

	result := buildDSN(cfg, mockResolver)

	// The password should be URL-escaped
	parsed, err := url.Parse(result)
	s.NoError(err)
	password, _ := parsed.User.Password()
	s.Equal("p@ss:w/rd&special", password, "password should be properly escaped and parsed")
}

// Mock service resolver for testing
type mockServiceResolver struct {
	addr string
}

func (m *mockServiceResolver) Resolve(addr string) []string {
	return []string{m.addr}
}
