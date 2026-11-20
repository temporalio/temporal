package tdbg

import (
	"context"
	"flag"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/headers"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

type args struct {
	path string
}
type testCase struct {
	name    string
	args    args
	wantErr bool
}

type httpClientWrapper struct {
	client  *http.Client
	testUrl string
}

func (c *httpClientWrapper) Get(_ string) (resp *http.Response, err error) {
	// mock all calls to GET using the testUrl instead
	return c.client.Get(c.testUrl)
}

func Test_fetchCACertFromUrl(t *testing.T) {
	// Ultimately, any URL like:
	// https://example.com/testdata/4096b-rsa-example-cert.pem
	// Will attempt to load the local file: testdata/4096b-rsa-example-cert.pem (removing
	// the host), but "wrapped" in a httptest server
	tests := []testCase{
		{
			name:    "example cert loads correctly from URL",
			args:    args{path: "https://example.com/testdata/4096b-rsa-example-cert.pem"},
			wantErr: false,
		},
		{
			name:    "example cert that is empty file on server",
			args:    args{path: "https://example.com/testdata/cert.pem"},
			wantErr: true,
		},
		{
			name:    "example cert that does not exist on server",
			args:    args{path: "https://example.com/testdata/notfound"},
			wantErr: true,
		},
		{
			name:    "example cert that is passed over http",
			args:    args{path: "http://example.com/testdata/notfound"},
			wantErr: true,
		},
	}
	// generate a test server so we can capture and inspect the request
	testServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		file, err := os.Open(strings.TrimPrefix(req.URL.Path, "/"))
		if err != nil {
			res.WriteHeader(http.StatusInternalServerError)
			_, _ = res.Write([]byte(err.Error()))
			return
		}
		bytes, err := io.ReadAll(file)
		if err != nil {
			res.WriteHeader(http.StatusInternalServerError)
			_, _ = res.Write([]byte(err.Error()))
			return
		}
		_, err = res.Write(bytes)
		if err != nil {
			res.WriteHeader(http.StatusInternalServerError)
			_, _ = res.Write([]byte(err.Error()))
			return
		}
	}))
	defer func() { testServer.Close() }()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsedUrl, err := url.Parse(tt.args.path)
			if err != nil {
				t.Errorf("invalid path error = %v, wantErr %v", err, tt.wantErr)
			}
			testUrl := testServer.URL + parsedUrl.Path
			netClient = &httpClientWrapper{client: testServer.Client(), testUrl: testUrl}
			http.DefaultClient = testServer.Client()
			_, err = fetchCACert(tt.args.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("fetchCACert() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func Test_fetchCACertFromFile(t *testing.T) {

	tests := []testCase{
		{
			name:    "empty file shows an error",
			args:    args{path: "testdata/cert.pem"},
			wantErr: true,
		},
		{
			name: "example cert loads correctly from file",
			args: args{path: "testdata/4096b-rsa-example-cert.pem"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := fetchCACert(tt.args.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("fetchCACert() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

// metadataCapturingAdminServer records the metadata seen on DescribeMutableState, so tests can
// verify what tdbg's client actually put on the wire.
type metadataCapturingAdminServer struct {
	adminservice.UnimplementedAdminServiceServer
	md metadata.MD
}

func (s *metadataCapturingAdminServer) DescribeMutableState(
	ctx context.Context,
	_ *adminservice.DescribeMutableStateRequest,
) (*adminservice.DescribeMutableStateResponse, error) {
	s.md, _ = metadata.FromIncomingContext(ctx)
	return &adminservice.DescribeMutableStateResponse{}, nil
}

// startAdminServer serves adminServer on a local port and returns its address.
func startAdminServer(t *testing.T, adminServer adminservice.AdminServiceServer, opts ...grpc.ServerOption) string {
	server := grpc.NewServer(opts...)
	adminservice.RegisterAdminServiceServer(server, adminServer)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return listener.Addr().String()
}

// newTestCLIContext builds a *cli.Context with the same flags (and defaults) as the real
// tdbg app, so factory.go's flag lookups (e.g. TLS settings) behave the same as in production.
func newTestCLIContext(t *testing.T) *cli.Context {
	app := NewCliApp()
	set := flag.NewFlagSet("test", flag.ContinueOnError)
	for _, f := range app.Flags {
		require.NoError(t, f.Apply(set))
	}
	return cli.NewContext(app, set, nil)
}

func TestCreateGRPCConnection_TagsCallerInfoAsOperator(t *testing.T) {
	adminServer := &metadataCapturingAdminServer{}
	factory := NewClientFactory(WithFrontendAddress(startAdminServer(t, adminServer)))
	client := factory.AdminClient(newTestCLIContext(t))

	// example API call - this tag should be set for all APIs called by tdbg
	_, err := client.DescribeMutableState(context.Background(), &adminservice.DescribeMutableStateRequest{})
	require.NoError(t, err)

	require.Equal(t, []string{"tdbg"}, adminServer.md.Get(headers.CallerNameHeaderName))
	require.Equal(t, []string{headers.CallerTypeOperator}, adminServer.md.Get(headers.CallerTypeHeaderName))
}

func TestCreateGRPCConnection_APIKey(t *testing.T) {
	// The API key enables TLS, so the server needs a certificate: reuse httptest's self-signed one.
	// The client trusts no CA for it, hence --tls-disable-host-verification below.
	httpsServer := httptest.NewTLSServer(nil)
	cert := httpsServer.TLS.Certificates[0]
	httpsServer.Close()

	tests := []struct {
		name     string
		grpcMeta []string
		want     []string
	}{
		{name: "API key only", want: []string{"Bearer test-key"}},
		{name: "grpc-meta replaces API key", grpcMeta: []string{"authorization=Bearer other"}, want: []string{"Bearer other"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adminServer := &metadataCapturingAdminServer{}
			address := startAdminServer(t, adminServer, grpc.Creds(credentials.NewServerTLSFromCert(&cert)))
			c := newTestCLIContext(t)
			require.NoError(t, c.Set(FlagAPIKey, "test-key"))
			require.NoError(t, c.Set(FlagTLSDisableHostVerification, "true"))
			for _, pair := range tt.grpcMeta {
				require.NoError(t, c.Set(FlagGRPCMeta, pair))
			}

			client := NewClientFactory(WithFrontendAddress(address)).AdminClient(c)
			_, err := client.DescribeMutableState(context.Background(), &adminservice.DescribeMutableStateRequest{})
			require.NoError(t, err)

			require.Equal(t, tt.want, adminServer.md.Get("authorization"))
		})
	}
}

func TestCreateGRPCConnection_GRPCMeta(t *testing.T) {
	adminServer := &metadataCapturingAdminServer{}
	c := newTestCLIContext(t)
	require.NoError(t, c.Set(FlagGRPCMeta, "foo=bar"))
	require.NoError(t, c.Set(FlagGRPCMeta, "baz=a=b"))
	require.NoError(t, c.Set(FlagGRPCMeta, "qux=1"))
	require.NoError(t, c.Set(FlagGRPCMeta, "qux=2"))
	require.NoError(t, c.Set(FlagGRPCMeta, "list=a,b"))

	client := NewClientFactory(WithFrontendAddress(startAdminServer(t, adminServer))).AdminClient(c)
	_, err := client.DescribeMutableState(context.Background(), &adminservice.DescribeMutableStateRequest{})
	require.NoError(t, err)

	require.Equal(t, []string{"bar"}, adminServer.md.Get("foo"))
	require.Equal(t, []string{"a=b"}, adminServer.md.Get("baz"))
	require.Equal(t, []string{"2"}, adminServer.md.Get("qux"))
	require.Equal(t, []string{"a,b"}, adminServer.md.Get("list"))
}

func TestCreateGRPCConnection_InvalidGRPCMeta(t *testing.T) {
	c := newTestCLIContext(t)
	require.NoError(t, c.Set(FlagGRPCMeta, "foo"))

	factory := &clientFactory{frontendAddressProvider: staticFrontendAddress("127.0.0.1:7233")}
	_, err := factory.createGRPCConnection(c)
	require.EqualError(t, err, `invalid --grpc-meta "foo", expected KEY=VALUE`)
}
