package callbacks

import (
	"net/url"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
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
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "happy path, default config: just temporal",
			args: args{
				rawURL: "temporal://system",
				rules:  []AddressMatchRule{{}},
			},
			wantErr: false,
		},
		{
			name: "sad path incorrect scheme, default config: just temporal",
			args: args{
				rawURL: "https://system",
				rules:  []AddressMatchRule{{}},
			},
			wantErr: true,
		},
		{
			name: "sad path incorrect host, default config: just temporal",
			args: args{
				rawURL: "temporal://somehost.com",
				rules:  []AddressMatchRule{{}},
			},
			wantErr: true,
		},
		{
			name: "sad path http, default config: just temporal",
			args: args{
				rawURL: "http://localhost",
				rules:  []AddressMatchRule{{}},
			},
			wantErr: true,
		},
		{
			name: "sad path invalid url, default config: just temporal",
			args: args{
				rawURL: "blblbblblb",
				rules:  []AddressMatchRule{{}},
			},
			wantErr: true,
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
			wantErr: false,
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
			wantErr: true,
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
			wantErr: false,
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
			wantErr: false,
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
			wantErr: false,
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
			wantErr: true,
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
			wantErr: false,
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
			wantErr: false,
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
			wantErr: false,
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
			wantErr: false,
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
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, err := url.Parse(tt.args.rawURL)
			if err != nil {
				t.Fatalf("failed to parse url: %v", err)
			}
			rules := AddressMatchRules{Rules: tt.args.rules}
			err = rules.Validate(u)
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.args.rawURL, err, tt.wantErr)
			}
		})
	}
}
