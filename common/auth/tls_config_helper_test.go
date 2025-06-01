package auth

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

var validBase64CaData, invalidBase64CaData, validBase64Certificate, invalidBase64Certificate, validBase64Key, invalidBase64Key string

func readFile(path string) string {
	file, err := os.Open("testdata/" + path)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			panic(err)
		}
	}()
	data, err := io.ReadAll(file)
	if err != nil {
		panic(err)
	}
	return base64.StdEncoding.EncodeToString(data)
}

func init() {
	validBase64CaData = readFile("ca.crt")
	invalidBase64CaData = readFile("invalid_ca.crt")
	validBase64Certificate = readFile("localhost.crt")
	invalidBase64Certificate = readFile("invalid_localhost.crt")
	validBase64Key = readFile("localhost.key")
	invalidBase64Key = readFile("invalid_localhost.key")
}

// test if the input is valid
func Test_NewTLSConfig(t *testing.T) {
	tests := map[string]struct {
		cfg    *TLS
		cfgErr string
	}{
		"emptyConfig": {
			cfg: &TLS{},
		},
		"caData_good": {
			cfg: &TLS{
				Enabled: true,
				CaData:  validBase64CaData,
			},
		},
		"caData_badBase64": {
			cfg:    &TLS{Enabled: true, CaData: "this isn't base64"},
			cfgErr: "illegal base64 data at input byte",
		},
		"caData_badPEM": {
			cfg:    &TLS{Enabled: true, CaData: "dGhpcyBpc24ndCBhIFBFTSBjZXJ0"},
			cfgErr: "unable to parse certs as PEM",
		},
		"clientCert_badbase64cert": {
			cfg: &TLS{
				Enabled:  true,
				CertData: "this ain't base64",
				KeyData:  validBase64Key,
			},
			cfgErr: "illegal base64 data at input byte",
		},
		"clientCert_badbase64key": {
			cfg: &TLS{
				Enabled:  true,
				CertData: validBase64Certificate,
				KeyData:  "this ain't base64",
			},
			cfgErr: "illegal base64 data at input byte",
		},
		"clientCert_missingprivatekey": {
			cfg: &TLS{
				Enabled:  true,
				CertData: validBase64Certificate,
				KeyData:  "",
			},
			cfgErr: "unable to config TLS: cert or key is missing",
		},
		"clientCert_duplicate_cert": {
			cfg: &TLS{
				Enabled:  true,
				CertData: validBase64Certificate,
				CertFile: "/a/b/c",
			},
			cfgErr: "only one of certData or certFile properties should be specified",
		},
		"clientCert_duplicate_key": {
			cfg: &TLS{
				Enabled: true,
				KeyData: validBase64Key,
				KeyFile: "/a/b/c",
			},
			cfgErr: "only one of keyData or keyFile properties should be specified",
		},
		"clientCert_duplicate_ca": {
			cfg: &TLS{
				Enabled: true,
				CaData:  validBase64CaData,
				CaFile:  "/a/b/c",
			},
			cfgErr: "only one of caData or caFile properties should be specified",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			_, err := NewTLSConfig(tc.cfg)
			if tc.cfgErr != "" {
				assert.ErrorContains(t, err, tc.cfgErr)
			} else {
				assert.NoError(t, err)
			}

			ctrl.Finish()
		})
	}
}

func Test_ConnectToTLSServerWithCA(t *testing.T) {
	// setup server
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Hello World")
	})
	ts := httptest.NewUnstartedServer(h)
	certBytes, err := os.ReadFile("./testdata/localhost.crt")
	if err != nil {
		panic(fmt.Errorf("unable to decode certificate %w", err))
	}
	keyBytes, err := os.ReadFile("./testdata/localhost.key")
	if err != nil {
		panic(fmt.Errorf("unable to decode key %w", err))
	}
	cert, err := tls.X509KeyPair(certBytes, keyBytes)
	if err != nil {
		panic(fmt.Errorf("unable to load certificate %w", err))
	}
	ts.TLS = &tls.Config{
		Certificates: []tls.Certificate{cert},
	}
	ts.StartTLS()

	tests := map[string]struct {
		cfg           *TLS
		connectionErr string
	}{
		"caData_good": {
			cfg: &TLS{
				Enabled: true,
				CaData:  validBase64CaData,
			},
		},
		"caData_signedByWrongCA": {
			cfg: &TLS{
				Enabled:                true,
				EnableHostVerification: true,
				CaData:                 invalidBase64CaData,
			},
			connectionErr: "x509: certificate signed by unknown authority",
		},
		"caData_signedByWrongCAButNotEnableHostVerification": {
			cfg: &TLS{
				Enabled:                true,
				EnableHostVerification: false,
				CaData:                 invalidBase64CaData,
			},
		},
		"caFile_good": {
			cfg: &TLS{
				Enabled:                true,
				EnableHostVerification: true,
				CaFile:                 "testdata/ca.crt",
			},
		},
		"caFile_signedByWrongCA": {
			cfg: &TLS{
				Enabled:                true,
				EnableHostVerification: true,
				CaFile:                 "testdata/invalid_ca.crt",
			},
			connectionErr: "x509: certificate signed by unknown authority",
		},
		"caFile_signedByWrongCANotEnableHostVerification": {
			cfg: &TLS{
				Enabled:                true,
				EnableHostVerification: false,
				CaFile:                 "testdata/invalid_ca.crt",
			},
		},
		"certData_good": {
			cfg: &TLS{
				Enabled:                true,
				EnableHostVerification: true,
				CaData:                 validBase64Certificate,
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			tlsConfig, err := NewTLSConfig(tc.cfg)
			if err != nil {
				panic(err)
			}
			cl := &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig}}
			resp, err := cl.Get(ts.URL)
			if tc.connectionErr != "" {
				assert.ErrorContains(t, err, tc.connectionErr)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, 200, resp.StatusCode)
			}

			ctrl.Finish()
		})
	}
}

func Test_ConnectToTLSServerWithClientCertificate(t *testing.T) {
	// setup server
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Hello World")
	})
	ts := httptest.NewUnstartedServer(h)
	certBytes, err := os.ReadFile("./testdata/localhost.crt")
	if err != nil {
		panic(fmt.Errorf("unable to decode certificate %w", err))
	}
	keyBytes, err := os.ReadFile("./testdata/localhost.key")
	if err != nil {
		panic(fmt.Errorf("unable to decode key %w", err))
	}
	cert, err := tls.X509KeyPair(certBytes, keyBytes)
	if err != nil {
		panic(fmt.Errorf("unable to load certificate %w", err))
	}
	caBytes, _ := os.ReadFile("testdata/ca.crt")
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caBytes)
	ts.TLS = &tls.Config{
		ClientCAs:    caCertPool,
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}
	ts.StartTLS()

	tests := map[string]struct {
		cfg           *TLS
		connectionErr string
	}{
		"clientData_good": {
			cfg: &TLS{
				Enabled:                true,
				EnableHostVerification: true,
				CaData:                 validBase64CaData,
				CertData:               validBase64Certificate,
				KeyData:                validBase64Key,
			},
		},
		"clientData_certNotProvided": {
			cfg: &TLS{
				Enabled:                true,
				EnableHostVerification: true,
				CaData:                 validBase64CaData,
			},
			connectionErr: "certificate required",
		},
		"clientData_certInvalid": {
			cfg: &TLS{
				Enabled:                true,
				EnableHostVerification: true,
				CaData:                 validBase64CaData,
				CertData:               invalidBase64Certificate,
				KeyData:                invalidBase64Key,
			},
			connectionErr: "certificate required",
		},
		"certFile_good": {
			cfg: &TLS{
				Enabled:                true,
				EnableHostVerification: true,
				CaData:                 validBase64CaData,
				CertFile:               "testdata/localhost.crt",
				KeyFile:                "testdata/localhost.key",
			},
		},
		"clientFile_certInvalid": {
			cfg: &TLS{
				Enabled:                true,
				EnableHostVerification: true,
				CaData:                 validBase64CaData,
				CertFile:               "testdata/invalid_localhost.crt",
				KeyFile:                "testdata/invalid_localhost.key",
			},
			connectionErr: "certificate required",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			tlsConfig, err := NewTLSConfig(tc.cfg)
			if err != nil {
				panic(err)
			}
			cl := &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig}}
			resp, err := cl.Get(ts.URL)
			if tc.connectionErr != "" {
				assert.ErrorContains(t, err, tc.connectionErr)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, 200, resp.StatusCode)
			}

			ctrl.Finish()
		})
	}
}
