package callbacks

import (
	"net/url"
	"regexp"
	"strings"
	"time"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/nexus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var RequestTimeout = dynamicconfig.NewDestinationDurationSetting(
	"component.callbacks.request.timeout",
	time.Second*10,
	`RequestTimeout is the timeout for executing a single callback request.`,
)

var RetryPolicyInitialInterval = dynamicconfig.NewGlobalDurationSetting(
	"component.callbacks.retryPolicy.initialInterval",
	time.Second,
	`The initial backoff interval between every callback request attempt for a given callback.`,
)

var RetryPolicyMaximumInterval = dynamicconfig.NewGlobalDurationSetting(
	"component.callbacks.retryPolicy.maxInterval",
	time.Hour,
	`The maximum backoff interval between every callback request attempt for a given callback.`,
)

type Config struct {
	RequestTimeout dynamicconfig.DurationPropertyFnWithDestinationFilter
	RetryPolicy    func() backoff.RetryPolicy
}

func ConfigProvider(dc *dynamicconfig.Collection) *Config {
	return &Config{
		RequestTimeout: RequestTimeout.Get(dc),
		RetryPolicy: func() backoff.RetryPolicy {
			return backoff.NewExponentialRetryPolicy(
				RetryPolicyInitialInterval.Get(dc)(),
			).WithMaximumInterval(
				RetryPolicyMaximumInterval.Get(dc)(),
			).WithExpirationInterval(
				backoff.NoInterval,
			)
		},
	}
}

type AddressMatchRules struct {
	Rules []AddressMatchRule
}

func (a AddressMatchRules) validate(rawURL string) error {
	// Exact match only; no path, query, or fragment allowed for system URL
	if rawURL == nexus.SystemCallbackURL || rawURL == chasm.NexusCompletionHandlerURL {
		return nil
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid callback url: %v", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return status.Errorf(codes.InvalidArgument, "invalid url: unknown scheme: %v", u)
	}
	if u.Host == "" {
		return status.Errorf(codes.InvalidArgument, "invalid url: missing host")
	}
	for _, rule := range a.Rules {
		allow, err := rule.allow(u)
		if err != nil {
			return err
		}
		if allow {
			return nil
		}
	}
	return status.Errorf(codes.InvalidArgument, "invalid url: url does not match any configured callback address: %v", u)
}

type AddressMatchRule struct {
	Regexp        *regexp.Regexp
	AllowInsecure bool
}

// Allow validates the URL by:
// 1. true, nil if the provided url matches the rule and passed validation
// for the given rule.
// 2. false, nil if the URL does not match the rule.
// 3. It false, error if there is a match and the URL fails validation
func (a AddressMatchRule) allow(u *url.URL) (bool, error) {
	if !a.Regexp.MatchString(u.Host) {
		return false, nil
	}
	if a.AllowInsecure {
		return true, nil
	}
	if u.Scheme != "https" {
		return false,
			status.Errorf(codes.InvalidArgument,
				"invalid url: callback address does not allow insecure connections: %v", u)
	}
	return true, nil
}

func allowedAddressConverter(val any) (AddressMatchRules, error) {
	type entry struct {
		Pattern       string
		AllowInsecure bool
	}
	intermediate, err := dynamicconfig.ConvertStructure[[]entry](nil)(val)
	if err != nil {
		return AddressMatchRules{}, err
	}

	configs := []AddressMatchRule{}
	for _, e := range intermediate {
		if e.Pattern == "" {
			// Skip configs with missing / unparsable Pattern
			continue
		}
		re, err := regexp.Compile(addressPatternToRegexp(e.Pattern))
		if err != nil {
			// Skip configs with malformed Pattern
			continue
		}
		configs = append(configs, AddressMatchRule{
			Regexp:        re,
			AllowInsecure: e.AllowInsecure,
		})
	}
	return AddressMatchRules{Rules: configs}, nil
}

func addressPatternToRegexp(pattern string) string {
	var result strings.Builder
	result.WriteString("^")
	first := true
	for literal := range strings.SplitSeq(pattern, "*") {
		if !first {
			// Replace * with .*
			result.WriteString(".*")
		}
		result.WriteString(regexp.QuoteMeta(literal))
		first = false
	}
	result.WriteString("$")
	return result.String()
}
