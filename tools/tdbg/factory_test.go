package tdbg

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
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
