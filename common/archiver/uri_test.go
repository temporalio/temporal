package archiver

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	URISuite struct {
		suite.Suite
	}
)

func TestURISuite(t *testing.T) {
	suite.Run(t, new(URISuite))
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
				"key1": {"value1", "value2"},
				"key2": {"value3"},
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
				"key1": {"value1", "value2"},
				"key2": {"value3"},
			},
		},
	}

	for _, tc := range testCases {
		URI, err := NewURI(tc.URIString)
		if !tc.valid {
			require.Error(s.T(), err)
			continue
		}

		require.NoError(s.T(), err)
		require.Equal(s.T(), tc.scheme, URI.Scheme())
		require.Equal(s.T(), tc.path, URI.Path())
		require.Equal(s.T(), tc.hostname, URI.Hostname())
		require.Equal(s.T(), tc.port, URI.Port())
		require.Equal(s.T(), tc.username, URI.Username())
		require.Equal(s.T(), tc.password, URI.Password())
		require.Equal(s.T(), tc.opaque, URI.Opaque())
		if tc.query != nil {
			require.Equal(s.T(), tc.query, URI.Query())
		}
	}
}
