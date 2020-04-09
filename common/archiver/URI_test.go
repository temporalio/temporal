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

package archiver

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	URISuite struct {
		*require.Assertions
		suite.Suite
	}
)

func TestURISuite(t *testing.T) {
	suite.Run(t, new(URISuite))
}

func (s *URISuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *URISuite) TestURI() {
	testCases := []struct {
		URIString string
		valid     bool
		scheme    string
		path      string
		hostname  string
		port      string
		username  string
		password  string
		opaque    string
		query     map[string][]string
	}{
		{
			URIString: "",
			valid:     false,
		},
		{
			URIString: "some random string",
			valid:     false,
		},
		{
			URIString: "mailto:a@b.com",
			valid:     true,
			scheme:    "mailto",
			opaque:    "a@b.com",
		},
		{
			URIString: "test://",
			valid:     true,
			scheme:    "test",
		},
		{
			URIString: "http://example.com/path",
			valid:     true,
			scheme:    "http",
			hostname:  "example.com",
			path:      "/path",
		},
		{
			URIString: "http://example.com/path with space",
			valid:     true,
			scheme:    "http",
			hostname:  "example.com",
			path:      "/path with space",
		},
		{
			URIString: "https://localhost:8080?key1=value1&key1=value2&key2=value3",
			valid:     true,
			scheme:    "https",
			hostname:  "localhost",
			port:      "8080",
			query: map[string][]string{
				"key1": []string{"value1", "value2"},
				"key2": []string{"value3"},
			},
		},
		{
			URIString: "file:///absolute/path/to/dir",
			valid:     true,
			scheme:    "file",
			path:      "/absolute/path/to/dir",
		},
		{
			URIString: "test://person:password@host/path",
			valid:     true,
			scheme:    "test",
			hostname:  "host",
			path:      "/path",
			username:  "person",
			password:  "password",
		},
		{
			URIString: "test:opaque?key1=value1&key1=value2&key2=value3",
			valid:     true,
			scheme:    "test",
			opaque:    "opaque",
			query: map[string][]string{
				"key1": []string{"value1", "value2"},
				"key2": []string{"value3"},
			},
		},
	}

	for _, tc := range testCases {
		URI, err := NewURI(tc.URIString)
		if !tc.valid {
			s.Error(err)
			continue
		}

		s.NoError(err)
		s.Equal(tc.scheme, URI.Scheme())
		s.Equal(tc.path, URI.Path())
		s.Equal(tc.hostname, URI.Hostname())
		s.Equal(tc.port, URI.Port())
		s.Equal(tc.username, URI.Username())
		s.Equal(tc.password, URI.Password())
		s.Equal(tc.opaque, URI.Opaque())
		if tc.query != nil {
			s.Equal(tc.query, URI.Query())
		}
	}
}
