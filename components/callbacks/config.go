// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package callbacks

import (
	"regexp"
	"strings"
	"time"

	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
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

var AllowedAddresses = dynamicconfig.NewNamespaceTypedSettingWithConverter(
	"component.callbacks.allowedAddresses",
	allowedAddressConverter,
	[]AddressMatchRule(nil),
	`The per-namespace list of addresses that are allowed for callbacks and whether secure connections (https) are required.
URLs are checked against each in order when starting a workflow with attached callbacks and only need to match one to pass validation.
Default is no address rules, meaning all callbacks will be rejected. Any invalid entries are ignored. Each entry is a map with possible values:
	 - "Pattern":string (required) the host:port pattern to which this config applies.
		Wildcards, '*', are supported and can match any number of characters (e.g. '*' matches everything, 'prefix.*.domain' matches 'prefix.a.domain' as well as 'prefix.a.b.domain').
	 - "AllowInsecure":bool (optional, default=false) indicates whether https is required`)

type AddressMatchRule struct {
	Regexp        *regexp.Regexp
	AllowInsecure bool
}

func allowedAddressConverter(val any) ([]AddressMatchRule, error) {
	type entry struct {
		Pattern       string
		AllowInsecure bool
	}
	intermediate, err := dynamicconfig.ConvertStructure([]entry{})(val)
	if err != nil {
		return nil, err
	}

	var configs []AddressMatchRule
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
	return configs, nil
}

func addressPatternToRegexp(pattern string) string {
	var result strings.Builder
	result.WriteString("^")
	for i, literal := range strings.Split(pattern, "*") {
		if i > 0 {
			// Replace * with .*
			result.WriteString(".*")
		}
		result.WriteString(regexp.QuoteMeta(literal))
	}
	result.WriteString("$")
	return result.String()
}
