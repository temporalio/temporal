package nexusrpc

import (
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
)

func TestAddLinksToHeader(t *testing.T) {
	type testcase struct {
		name   string
		input  []nexus.Link
		output http.Header
		errMsg string
	}

	cases := []testcase{
		{
			name: "single link",
			input: []nexus.Link{{
				URL: &url.URL{
					Scheme:   "https",
					Host:     "example.com",
					Path:     "/path/to/something",
					RawQuery: "param=value",
				},
				Type: "url",
			}},
			output: http.Header{
				http.CanonicalHeaderKey(headerLink): []string{
					`<https://example.com/path/to/something?param=value>; type="url"`,
				},
			},
		},
		{
			name: "multiple links",
			input: []nexus.Link{
				{
					URL: &url.URL{
						Scheme:   "https",
						Host:     "example.com",
						Path:     "/path/to/something",
						RawQuery: "param=value",
					},
					Type: "url",
				},
				{
					URL: &url.URL{
						Scheme:   "https",
						Host:     "foo.com",
						Path:     "/path/to/something",
						RawQuery: "bar=value",
					},
					Type: "url",
				},
			},
			output: http.Header{
				http.CanonicalHeaderKey(headerLink): []string{
					`<https://example.com/path/to/something?param=value>; type="url"`,
					`<https://foo.com/path/to/something?bar=value>; type="url"`,
				},
			},
		},
		{
			name: "invalid link",
			input: []nexus.Link{{
				URL: &url.URL{
					Scheme:   "https",
					Host:     "example.com",
					Path:     "/path/to/something",
					RawQuery: "param=value%",
				},
				Type: "url",
			}},
			errMsg: "failed to encode link",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			output := http.Header{}
			err := addLinksToHTTPHeader(tc.input, output)
			if tc.errMsg != "" {
				require.ErrorContains(t, err, tc.errMsg)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.output, output)
			}
		})
	}
}

func TestGetLinksFromHeader(t *testing.T) {
	type testcase struct {
		name   string
		input  http.Header
		output []nexus.Link
		errMsg string
	}

	cases := []testcase{
		{
			name: "single link",
			input: http.Header{
				http.CanonicalHeaderKey(headerLink): []string{
					`<https://example.com/path/to/something?param=value>; type="url"`,
				},
			},
			output: []nexus.Link{{
				URL: &url.URL{
					Scheme:   "https",
					Host:     "example.com",
					Path:     "/path/to/something",
					RawQuery: "param=value",
				},
				Type: "url",
			}},
		},
		{
			name: "multiple links",
			input: http.Header{
				http.CanonicalHeaderKey(headerLink): []string{
					`<https://example.com/path/to/something?param=value>; type="url"`,
					`<https://foo.com/path/to/something?bar=value>; type="url"`,
				},
			},
			output: []nexus.Link{
				{
					URL: &url.URL{
						Scheme:   "https",
						Host:     "example.com",
						Path:     "/path/to/something",
						RawQuery: "param=value",
					},
					Type: "url",
				},
				{
					URL: &url.URL{
						Scheme:   "https",
						Host:     "foo.com",
						Path:     "/path/to/something",
						RawQuery: "bar=value",
					},
					Type: "url",
				},
			},
		},
		{
			name: "multiple links single header",
			input: http.Header{
				http.CanonicalHeaderKey(headerLink): []string{
					`<https://example.com/path/to/something?param=value>; type="url", <https://foo.com/path/to/something?bar=value>; type="url"`,
				},
			},
			output: []nexus.Link{
				{
					URL: &url.URL{
						Scheme:   "https",
						Host:     "example.com",
						Path:     "/path/to/something",
						RawQuery: "param=value",
					},
					Type: "url",
				},
				{
					URL: &url.URL{
						Scheme:   "https",
						Host:     "foo.com",
						Path:     "/path/to/something",
						RawQuery: "bar=value",
					},
					Type: "url",
				},
			},
		},
		{
			name: "invalid header",
			input: http.Header{
				http.CanonicalHeaderKey(headerLink): []string{
					`<https://example.com/path?param=value> type="url"`,
				},
			},
			errMsg: "failed to parse link header",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			output, err := getLinksFromHeader(tc.input)
			if tc.errMsg != "" {
				require.ErrorContains(t, err, tc.errMsg)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.output, output)
			}
		})
	}
}

func TestEncodeLink(t *testing.T) {
	type testcase struct {
		name   string
		input  nexus.Link
		output string
		errMsg string
	}

	cases := []testcase{
		{
			name: "valid",
			input: nexus.Link{
				URL: &url.URL{
					Scheme:   "https",
					Host:     "example.com",
					Path:     "/path/to/something",
					RawQuery: "param1=value1&param2=value2",
				},
				Type: "text/plain",
			},
			output: `<https://example.com/path/to/something?param1=value1&param2=value2>; type="text/plain"`,
		},
		{
			name: "valid custom URL",
			input: nexus.Link{
				URL: &url.URL{
					Scheme:   "nexus",
					Path:     "/path/to/something",
					RawQuery: "param1=value1",
				},
				Type: "nexus.data_type",
			},
			output: `<nexus:///path/to/something?param1=value1>; type="nexus.data_type"`,
		},
		{
			name: "invalid url empty",
			input: nexus.Link{
				URL:  &url.URL{},
				Type: "text/plain",
			},
			errMsg: "failed to encode link",
		},
		{
			name: "invalid query not percent-encoded",
			input: nexus.Link{
				URL: &url.URL{
					Scheme:   "https",
					Host:     "example.com",
					Path:     "/path/to/something",
					RawQuery: "param1=value1&param2=value2;",
				},
				Type: "text/plain",
			},
			errMsg: "failed to encode link",
		},
		{
			name: "invalid type empty",
			input: nexus.Link{
				URL: &url.URL{
					Scheme:   "https",
					Host:     "example.com",
					Path:     "/path/to/something",
					RawQuery: "param1=value1&param2=value2",
				},
				Type: "",
			},
			errMsg: "failed to encode link",
		},
		{
			name: "invalid type invalid chars",
			input: nexus.Link{
				URL: &url.URL{
					Scheme:   "https",
					Host:     "example.com",
					Path:     "/path/to/something",
					RawQuery: "param1=value1&param2=value2",
				},
				Type: "text/plain;",
			},
			errMsg: "failed to encode link",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			output, err := encodeLink(tc.input)
			if tc.errMsg != "" {
				require.ErrorContains(t, err, tc.errMsg)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.output, output)
			}
		})
	}
}

func TestDecodeLink(t *testing.T) {
	type testcase struct {
		name   string
		input  string
		output nexus.Link
		errMsg string
	}

	cases := []testcase{
		{
			name:  "valid",
			input: `<https://example.com/path/to/something?param1=value1&param2=value2>; type="text/plain"`,
			output: nexus.Link{
				URL: &url.URL{
					Scheme:   "https",
					Host:     "example.com",
					Path:     "/path/to/something",
					RawQuery: "param1=value1&param2=value2",
				},
				Type: "text/plain",
			},
		},
		{
			name:  "valid multiple params",
			input: `<https://example.com/path/to/something?param1=value1&param2=value2>; type="text/plain"; Param="value"`,
			output: nexus.Link{
				URL: &url.URL{
					Scheme:   "https",
					Host:     "example.com",
					Path:     "/path/to/something",
					RawQuery: "param1=value1&param2=value2",
				},
				Type: "text/plain",
			},
		},
		{
			name:  "valid param not quoted",
			input: `<https://example.com/path/to/something?param1=value1&param2=value2>; type=text/plain`,
			output: nexus.Link{
				URL: &url.URL{
					Scheme:   "https",
					Host:     "example.com",
					Path:     "/path/to/something",
					RawQuery: "param1=value1&param2=value2",
				},
				Type: "text/plain",
			},
		},
		{
			name:  "valid custom URL",
			input: `<nexus:///path/to/something?param=value>; type="nexus.data_type"`,
			output: nexus.Link{
				URL: &url.URL{
					Scheme:   "nexus",
					Path:     "/path/to/something",
					RawQuery: "param=value",
				},
				Type: "nexus.data_type",
			},
		},
		{
			name:   "invalid url",
			input:  `<https://example.com/path/to/something%?param1=value1&param2=value2>`,
			errMsg: "failed to parse link header",
		},
		{
			name:   "invalid trailing semi-colon",
			input:  `<https://example.com/path/to/something?param1=value1&param2=value2>; type="text/plain";`,
			errMsg: "failed to parse link header",
		},
		{
			name:   "invalid empty param part",
			input:  `<https://example.com/path/to/something?param1=value1&param2=value2>; ; type="text/plain`,
			errMsg: "failed to parse link header",
		},
		{
			name:   "invalid no type param trailing semi-colon",
			input:  `<https://example.com/path/to/something?param1=value1&param2=value2>;`,
			errMsg: "failed to parse link header",
		},
		{
			name:   "invalid url not enclosed",
			input:  `https://example.com/path/to/something?param1=value1&param2=value2; type="text/plain"`,
			errMsg: "failed to parse link header",
		},
		{
			name:   "invalid url missing closing bracket",
			input:  `<https://example.com/path/to/something?param1=value1&param2=value2; type="text/plain"`,
			errMsg: "failed to parse link header",
		},
		{
			name:   "invalid url missing opening bracket",
			input:  `https://example.com/path/to/something?param1=value1&param2=value2>; type="text/plain"`,
			errMsg: "failed to parse link header",
		},
		{
			name:   "invalid param missing quote",
			input:  `https://example.com/path/to/something?param1=value1&param2=value2>; type="text/plain`,
			errMsg: "failed to parse link header",
		},
		{
			name:   "invalid multiple params missing semi-colon",
			input:  `https://example.com/path/to/something?param1=value1&param2=value2>; type="text/plain" Param=value`,
			errMsg: "failed to parse link header",
		},
		{
			name:   "invalid missing semi-colon after url",
			input:  `https://example.com/path/to/something?param1=value1&param2=value2> type="text/plain"`,
			errMsg: "failed to parse link header",
		},
		{
			name:   "invalid param missing value",
			input:  `https://example.com/path/to/something?param1=value1&param2=value2>; type`,
			errMsg: "failed to parse link header",
		},
		{
			name:   "invalid param missing value with equal sign",
			input:  `<https://example.com/path/to/something?param1=value1&param2=value2>; type=`,
			errMsg: "failed to parse link header",
		},
		{
			name:   "invalid missing type key",
			input:  `<https://example.com/path/to/something?param1=value1&param2=value2>`,
			errMsg: "failed to parse link header",
		},
		{
			name:   "invalid url empty",
			input:  `<>; type="text/plain"`,
			errMsg: "failed to parse link header",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			output, err := decodeLink(tc.input)
			if tc.errMsg != "" {
				require.ErrorContains(t, err, tc.errMsg)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.output, output)
			}
		})
	}
}

func TestParseDuration(t *testing.T) {
	_, err := ParseDuration("invalid")
	require.ErrorContains(t, err, "invalid duration:")
	d, err := ParseDuration("10ms")
	require.NoError(t, err)
	require.Equal(t, 10*time.Millisecond, d)
	d, err = ParseDuration("10.1ms")
	require.NoError(t, err)
	require.Equal(t, 10*time.Millisecond, d)
	d, err = ParseDuration("1s")
	require.NoError(t, err)
	require.Equal(t, 1*time.Second, d)
	d, err = ParseDuration("999m")
	require.NoError(t, err)
	require.Equal(t, 999*time.Minute, d)
	d, err = ParseDuration("1.3s")
	require.NoError(t, err)
	require.Equal(t, 1300*time.Millisecond, d)
}
