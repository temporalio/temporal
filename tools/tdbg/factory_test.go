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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/headers"
	"google.golang.org/grpc"
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

// callerInfoCapturingAdminServer records the caller-name/caller-type headers seen on
// DescribeMutableState, so tests can verify what tdbg's client actually put on the wire.
type callerInfoCapturingAdminServer struct {
	adminservice.UnimplementedAdminServiceServer
	callerName []string
	callerType []string
}

func (s *callerInfoCapturingAdminServer) DescribeMutableState(
	ctx context.Context,
	_ *adminservice.DescribeMutableStateRequest,
) (*adminservice.DescribeMutableStateResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	s.callerName = md.Get(headers.CallerNameHeaderName)
	s.callerType = md.Get(headers.CallerTypeHeaderName)
	return &adminservice.DescribeMutableStateResponse{}, nil
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
	adminServer := &callerInfoCapturingAdminServer{}
	server := grpc.NewServer()
	adminservice.RegisterAdminServiceServer(server, adminServer)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	factory := NewClientFactory(WithFrontendAddress(listener.Addr().String()))
	client := factory.AdminClient(newTestCLIContext(t))

	_, err = client.DescribeMutableState(context.Background(), &adminservice.DescribeMutableStateRequest{})
	require.NoError(t, err)

	assert.Equal(t, []string{"tdbg"}, adminServer.callerName)
	assert.Equal(t, []string{headers.CallerTypeOperator}, adminServer.callerType)
}
