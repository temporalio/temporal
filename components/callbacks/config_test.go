package callbacks

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common/nexus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func Test_addressPatternToRegexp(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		want    string
	}{
		{name: "empty", pattern: "", want: "^$"},
		{name: "no_wildcard", pattern: "foo", want: "^foo$"},
		{name: "single_wildcard_only", pattern: "*", want: "^.*$"},
		{name: "leading_wildcard", pattern: "*foo", want: "^.*foo$"},
		{name: "trailing_wildcard", pattern: "foo*", want: "^foo.*$"},
		{name: "surrounded_wildcard", pattern: "*foo*", want: "^.*foo.*$"},
		{name: "middle_wildcard", pattern: "foo*bar", want: "^foo.*bar$"},
		{name: "literal_dots_around_wildcard", pattern: "foo.*bar", want: "^foo\\..*bar$"},
		{name: "prefix_subdomain", pattern: "prefix.*.domain", want: "^prefix\\..*\\.domain$"},
		{name: "leading_any_subdomain", pattern: "*.example.com", want: "^.*\\.example\\.com$"},
		{name: "host_with_port", pattern: "api.example.com:8080", want: "^api\\.example\\.com:8080$"},
		{name: "consecutive_wildcards", pattern: "a**b", want: "^a.*.*b$"},
		{name: "triple_wildcards", pattern: "a***b", want: "^a.*.*.*b$"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := addressPatternToRegexp(test.pattern)
			require.Equal(t, test.want, got)
			_, err := regexp.Compile(got)
			require.NoError(t, err)
		})
	}
}

func TestAddressMatchRules_Validate(t *testing.T) {
	type args struct {
		rawURL string
		rules  []AddressMatchRule
	}
	tests := []struct {
		name        string
		args        args
		validateErr func(t *testing.T, err error)
	}{
		{
			name: "happy path, default config: just temporal",
			args: args{
				rawURL: nexus.SystemCallbackURL,
				rules:  []AddressMatchRule{},
			},
			validateErr: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
		{
			name: "sad path incorrect scheme, default config: just temporal",
			args: args{
				rawURL: "https://system",
				rules:  []AddressMatchRule{},
			},
			validateErr: func(t *testing.T, err error) {
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
				require.ErrorContains(t, err, "invalid url: url does not match any configured callback address")
			},
		},
		{
			name: "sad path incorrect host, default config: just temporal",
			args: args{
				rawURL: "temporal://somehost.com",
				rules:  []AddressMatchRule{},
			},
			validateErr: func(t *testing.T, err error) {
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
				require.ErrorContains(t, err, "invalid url: unknown scheme")
			},
		},
		{
			name: "sad path http, default config: just temporal",
			args: args{
				rawURL: "http://localhost",
				rules:  []AddressMatchRule{},
			},
			validateErr: func(t *testing.T, err error) {
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
				require.ErrorContains(t, err, "invalid url: url does not match any configured callback address")
			},
		},
		{
			name: "sad path invalid url, default config: just temporal",
			args: args{
				rawURL: "blblbblblb",
				rules:  []AddressMatchRule{},
			},
			validateErr: func(t *testing.T, err error) {
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
				require.ErrorContains(t, err, "invalid url: unknown scheme")
			},
		},
		{
			name: "secure only passes with https",
			args: args{
				rawURL: "https://api.example.com",
				rules: []AddressMatchRule{
					func() AddressMatchRule {
						re := regexp.MustCompile(addressPatternToRegexp("api.example.com"))
						return AddressMatchRule{Regexp: re, AllowInsecure: false}
					}(),
				},
			},
			validateErr: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
		{
			name: "secure only fails with http",
			args: args{
				rawURL: "http://api.example.com",
				rules: []AddressMatchRule{
					func() AddressMatchRule {
						re := regexp.MustCompile(addressPatternToRegexp("api.example.com"))
						return AddressMatchRule{Regexp: re, AllowInsecure: false}
					}(),
				},
			},
			validateErr: func(t *testing.T, err error) {
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
				require.ErrorContains(t, err, "invalid url: callback address does not allow insecure connections")
			},
		},
		{
			name: "allow insecure passes with http",
			args: args{
				rawURL: "http://a.example.com",
				rules: []AddressMatchRule{
					func() AddressMatchRule {
						re := regexp.MustCompile(addressPatternToRegexp("*.example.com"))
						return AddressMatchRule{Regexp: re, AllowInsecure: true}
					}(),
				},
			},
			validateErr: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
		{
			name: "allow insecure passes with https",
			args: args{
				rawURL: "https://a.example.com",
				rules: []AddressMatchRule{
					func() AddressMatchRule {
						re := regexp.MustCompile(addressPatternToRegexp("*.example.com"))
						return AddressMatchRule{Regexp: re, AllowInsecure: true}
					}(),
				},
			},
			validateErr: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
		{
			name: "port must match",
			args: args{
				rawURL: "https://api.example.com:8080",
				rules: []AddressMatchRule{
					func() AddressMatchRule {
						re := regexp.MustCompile(addressPatternToRegexp("api.example.com:8080"))
						return AddressMatchRule{Regexp: re, AllowInsecure: true}
					}(),
				},
			},
			validateErr: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
		{
			name: "port mismatch fails",
			args: args{
				rawURL: "https://api.example.com:9090",
				rules: []AddressMatchRule{
					func() AddressMatchRule {
						re := regexp.MustCompile(addressPatternToRegexp("api.example.com:8080"))
						return AddressMatchRule{Regexp: re, AllowInsecure: true}
					}(),
				},
			},
			validateErr: func(t *testing.T, err error) {
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
				require.ErrorContains(t, err, "invalid url: url does not match any configured callback address")
			},
		},
		{
			name: "middle wildcard matches",
			args: args{
				rawURL: "https://foozbar.com",
				rules: []AddressMatchRule{
					func() AddressMatchRule {
						re := regexp.MustCompile(addressPatternToRegexp("foo*bar.com"))
						return AddressMatchRule{Regexp: re, AllowInsecure: true}
					}(),
				},
			},
			validateErr: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
		{
			name: "prefix subdomain matches single level",
			args: args{
				rawURL: "https://prefix.a.domain",
				rules: []AddressMatchRule{
					func() AddressMatchRule {
						re := regexp.MustCompile(addressPatternToRegexp("prefix.*.domain"))
						return AddressMatchRule{Regexp: re, AllowInsecure: true}
					}(),
				},
			},
			validateErr: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
		{
			name: "prefix subdomain matches multiple levels",
			args: args{
				rawURL: "https://prefix.a.b.domain",
				rules: []AddressMatchRule{
					func() AddressMatchRule {
						re := regexp.MustCompile(addressPatternToRegexp("prefix.*.domain"))
						return AddressMatchRule{Regexp: re, AllowInsecure: true}
					}(),
				},
			},
			validateErr: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
		{
			name: "multiple rules, second matches",
			args: args{
				rawURL: "http://a.ok.com",
				rules: []AddressMatchRule{
					func() AddressMatchRule {
						re := regexp.MustCompile(addressPatternToRegexp("no-match.com"))
						return AddressMatchRule{Regexp: re, AllowInsecure: true}
					}(),
					func() AddressMatchRule {
						re := regexp.MustCompile(addressPatternToRegexp("*.ok.com"))
						return AddressMatchRule{Regexp: re, AllowInsecure: true}
					}(),
				},
			},
			validateErr: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
		{
			name: "unknown scheme fails",
			args: args{
				rawURL: "ftp://example.com",
				rules: []AddressMatchRule{
					func() AddressMatchRule {
						re := regexp.MustCompile(addressPatternToRegexp("example.com"))
						return AddressMatchRule{Regexp: re, AllowInsecure: true}
					}(),
				},
			},
			validateErr: func(t *testing.T, err error) {
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
				require.ErrorContains(t, err, "invalid url: unknown scheme")
			},
		},
		{
			name: "invalid url",
			args: args{
				rawURL: "../..///../",
				rules: []AddressMatchRule{
					func() AddressMatchRule {
						re := regexp.MustCompile(addressPatternToRegexp("example.com"))
						return AddressMatchRule{Regexp: re, AllowInsecure: true}
					}(),
				},
			},
			validateErr: func(t *testing.T, err error) {
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
				require.ErrorContains(t, err, "invalid url: unknown scheme")
			},
		},
		{
			name: "invalid url",
			args: args{
				rawURL: "http://",
				rules: []AddressMatchRule{
					func() AddressMatchRule {
						re := regexp.MustCompile(addressPatternToRegexp("example.com"))
						return AddressMatchRule{Regexp: re, AllowInsecure: true}
					}(),
				},
			},
			validateErr: func(t *testing.T, err error) {
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
				require.ErrorContains(t, err, "invalid url: missing host")
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			rules := AddressMatchRules{Rules: tt.args.rules}
			tt.validateErr(t, rules.validate(tt.args.rawURL))
		})
	}
}

func TestValidateCallbacks(t *testing.T) {
	allowAll := AddressMatchRules{
		Rules: []AddressMatchRule{
			{Regexp: regexp.MustCompile(`.*`), AllowInsecure: true},
		},
	}

	t.Run("ValidNexusCallback", func(t *testing.T) {
		cbs := []*commonpb.Callback{
			{Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{
					Url:    "http://localhost:8080/callback",
					Header: map[string]string{"Content-Type": "application/json"},
				},
			}},
		}
		err := ValidateCallbacks(cbs, 10, 1000, 4096, allowAll, "an entity")
		require.NoError(t, err)
	})

	t.Run("TooManyCallbacks", func(t *testing.T) {
		cbs := []*commonpb.Callback{
			{Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: "http://localhost/cb1"}}},
			{Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: "http://localhost/cb2"}}},
		}
		err := ValidateCallbacks(cbs, 1, 1000, 4096, allowAll, "an entity")
		require.Error(t, err)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
		require.Contains(t, err.Error(), "cannot attach more than 1 callbacks to an entity")
	})

	t.Run("URLTooLong", func(t *testing.T) {
		cbs := []*commonpb.Callback{
			{Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{
					Url: "http://localhost/" + string(make([]byte, 51)),
				},
			}},
		}
		err := ValidateCallbacks(cbs, 10, 50, 4096, allowAll, "an entity")
		require.Error(t, err)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
		require.Contains(t, err.Error(), "url length longer than max length allowed")
	})

	t.Run("HeaderTooLarge", func(t *testing.T) {
		cbs := []*commonpb.Callback{
			{Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{
					Url:    "http://localhost:8080/callback",
					Header: map[string]string{"X-Large": string(make([]byte, 5000))},
				},
			}},
		}
		err := ValidateCallbacks(cbs, 10, 1000, 4096, allowAll, "an entity")
		require.Error(t, err)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
		require.Contains(t, err.Error(), "header size longer than max allowed size")
	})

	t.Run("HeaderKeysNormalizedToLowercase", func(t *testing.T) {
		cbs := []*commonpb.Callback{
			{Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{
					Url:    "http://localhost:8080/callback",
					Header: map[string]string{"Content-Type": "application/json", "X-Custom": "value"},
				},
			}},
		}
		err := ValidateCallbacks(cbs, 10, 1000, 4096, allowAll, "an entity")
		require.NoError(t, err)
		nexus := cbs[0].GetNexus()
		require.Equal(t, "application/json", nexus.Header["content-type"])
		require.Equal(t, "value", nexus.Header["x-custom"])
		_, hasMixed := nexus.Header["Content-Type"]
		require.False(t, hasMixed)
	})

	t.Run("URLNotInAllowlist", func(t *testing.T) {
		cbs := []*commonpb.Callback{
			{Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{
					Url: "http://localhost:8080/callback",
				},
			}},
		}
		err := ValidateCallbacks(cbs, 10, 1000, 4096, AddressMatchRules{}, "an entity")
		require.Error(t, err)
		require.Contains(t, err.Error(), "does not match any configured callback address")
	})

	t.Run("UnsupportedVariant", func(t *testing.T) {
		cbs := []*commonpb.Callback{
			{Variant: nil},
		}
		err := ValidateCallbacks(cbs, 10, 1000, 4096, allowAll, "an entity")
		require.Error(t, err)
		require.Equal(t, codes.Unimplemented, status.Code(err))
		require.Contains(t, err.Error(), "unknown callback variant")
	})

	t.Run("EmptyCallbacksNoError", func(t *testing.T) {
		err := ValidateCallbacks(nil, 10, 1000, 4096, allowAll, "an entity")
		require.NoError(t, err)
	})

	t.Run("InternalCallbackSkipped", func(t *testing.T) {
		cbs := []*commonpb.Callback{
			{Variant: &commonpb.Callback_Internal_{
				Internal: &commonpb.Callback_Internal{},
			}},
		}
		err := ValidateCallbacks(cbs, 10, 1000, 4096, allowAll, "an entity")
		require.NoError(t, err)
	})
}
