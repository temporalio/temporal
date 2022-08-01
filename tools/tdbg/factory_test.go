// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
