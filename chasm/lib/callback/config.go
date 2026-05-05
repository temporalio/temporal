package callback

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

var EnableStandaloneExecutions = dynamicconfig.NewNamespaceBoolSetting(
	"callback.enableStandaloneExecutions",
	false,
	`Toggles standalone callback execution functionality on the server.`,
)

var LongPollBuffer = dynamicconfig.NewNamespaceDurationSetting(
	"callback.longPollBuffer",
	time.Second,
	`A buffer used to adjust the callback execution long-poll timeouts.
The long-poll response is sent before the caller's deadline by this amount of time.`,
)

var LongPollTimeout = dynamicconfig.NewNamespaceDurationSetting(
	"callback.longPollTimeout",
	20*time.Second,
	`Timeout for callback execution long-poll requests.`,
)

var MaxPerExecution = dynamicconfig.NewNamespaceIntSetting(
	"callback.maxPerExecution",
	2000,
	`MaxPerExecution is the maximum number of callbacks that can be attached to an execution (workflow or standalone activity).`,
)

var RequestTimeout = dynamicconfig.NewDestinationDurationSetting(
	"callback.request.timeout",
	time.Second*10,
	`RequestTimeout is the timeout for executing a single callback request.`,
)

var RetryPolicyInitialInterval = dynamicconfig.NewGlobalDurationSetting(
	"callback.retryPolicy.initialInterval",
	time.Second,
	`The initial backoff interval between every callback request attempt for a given callback.`,
)

var RetryPolicyMaximumInterval = dynamicconfig.NewGlobalDurationSetting(
	"callback.retryPolicy.maxInterval",
	time.Hour,
	`The maximum backoff interval between every callback request attempt for a given callback.`,
)

type Config struct {
	// callback.* settings.
	EnableStandaloneExecutions dynamicconfig.BoolPropertyFnWithNamespaceFilter
	LongPollBuffer             dynamicconfig.DurationPropertyFnWithNamespaceFilter
	LongPollTimeout            dynamicconfig.DurationPropertyFnWithNamespaceFilter
	RequestTimeout             dynamicconfig.DurationPropertyFnWithDestinationFilter
	RetryPolicy                func() backoff.RetryPolicy

	// Settings defined elsewhere.
	CHASMEnabled          dynamicconfig.BoolPropertyFnWithNamespaceFilter
	CHASMCallbacksEnabled dynamicconfig.BoolPropertyFnWithNamespaceFilter

	// Validation related config.
	BlobSizeLimitError dynamicconfig.IntPropertyFnWithNamespaceFilter
	BlobSizeLimitWarn  dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxIDLength        dynamicconfig.IntPropertyFn // Used to check CallbackID, RequestID, etc.

	// NOTE: The configuration setting defining the allowlist of supported callback
	// addresses is defined in components/callbacks/config.go, via AllowedAddresses.
	//
	// Similarly, MaxPerExecution is missing. It is used by `Validator` and is loaded there.
	// Once HSM callbacks (components/callbacks) are removed, the callbackValidatorProvider in
	// frontend/fx.go can be moved into this package. And at that time, we can simply have the
	// callback.Validator inject callback.Config. (And have a single location for all config.)
}

func ConfigProvider(dc *dynamicconfig.Collection) *Config {
	return &Config{
		EnableStandaloneExecutions: EnableStandaloneExecutions.Get(dc),
		LongPollBuffer:             LongPollBuffer.Get(dc),
		LongPollTimeout:            LongPollTimeout.Get(dc),
		RequestTimeout:             RequestTimeout.Get(dc),
		RetryPolicy: func() backoff.RetryPolicy {
			return backoff.NewExponentialRetryPolicy(
				RetryPolicyInitialInterval.Get(dc)(),
			).WithMaximumInterval(
				RetryPolicyMaximumInterval.Get(dc)(),
			).WithExpirationInterval(
				backoff.NoInterval,
			)
		},

		CHASMEnabled:          dynamicconfig.EnableChasm.Get(dc),
		CHASMCallbacksEnabled: dynamicconfig.EnableCHASMCallbacks.Get(dc),

		MaxIDLength:        dynamicconfig.MaxIDLengthLimit.Get(dc),
		BlobSizeLimitError: dynamicconfig.BlobSizeLimitError.Get(dc),
		BlobSizeLimitWarn:  dynamicconfig.BlobSizeLimitWarn.Get(dc),
	}
}

type AddressMatchRules struct {
	Rules []AddressMatchRule
}

func (a AddressMatchRules) Validate(rawURL string) error {
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
		allow, err := rule.Allow(u)
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
func (a AddressMatchRule) Allow(u *url.URL) (bool, error) {
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
