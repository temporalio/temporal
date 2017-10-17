// Copyright (c) 2017 Uber Technologies, Inc.
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

package config

import (
	"github.com/cactus/go-statsd-client/statsd"
	"github.com/uber-go/tally"
	tallystatsdreporter "github.com/uber-go/tally/statsd"
	statsdreporter "github.com/uber/cadence/common/metrics/tally/statsd"
	"log"
	"time"
)

// NewScope builds a new tally scope
// for this metrics configuration
//
// If the underlying configuration is
// valid for multiple reporter types,
// only one of them will be used for
// reporting. Currently, m3 is preferred
// over statsd
func (c *Metrics) NewScope() tally.Scope {
	if c.M3 != nil {
		return c.newM3Scope()
	}
	if c.Statsd != nil {
		return c.newStatsdScope()
	}
	return tally.NoopScope
}

// newM3Scope returns a new m3 scope with
// a default reporting interval of a second
func (c *Metrics) newM3Scope() tally.Scope {
	reporter, err := c.M3.NewReporter()
	if err != nil {
		log.Fatalf("error creating m3 reporter, err=%v", err)
	}
	scopeOpts := tally.ScopeOptions{
		Tags:           c.Tags,
		CachedReporter: reporter,
	}
	scope, _ := tally.NewRootScope(scopeOpts, time.Second)
	return scope
}

// newM3Scope returns a new statsd scope with
// a default reporting interval of a second
func (c *Metrics) newStatsdScope() tally.Scope {
	config := c.Statsd
	if len(config.HostPort) == 0 {
		return tally.NoopScope
	}
	statter, err := statsd.NewBufferedClient(config.HostPort, config.Prefix, config.FlushInterval, config.FlushBytes)
	if err != nil {
		log.Fatalf("error creating statsd client, err=%v", err)
	}
	//NOTE: according to ( https://github.com/uber-go/tally )Tally's statsd implementation doesn't support tagging.
	// Therefore, we implement Tally interface to have a statsd reporter that can support tagging
	reporter := statsdreporter.NewReporter(statter, tallystatsdreporter.Options{})
	scopeOpts := tally.ScopeOptions{
		Tags:     c.Tags,
		Reporter: reporter,
	}
	scope, _ := tally.NewRootScope(scopeOpts, time.Second)
	return scope
}
