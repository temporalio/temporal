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

package scheduler

import (
	"time"

	"go.temporal.io/server/common/dynamicconfig"
)

type (
	Tweakables struct {
		DefaultCatchupWindow              time.Duration // Default for catchup window
		MinCatchupWindow                  time.Duration // Minimum for catchup window
		MaxBufferSize                     int           // MaxBufferSize limits the number of buffered actions pending execution in total
		BackfillsPerIteration             int           // How many backfilled actions to buffer per iteration (implies rate limit since min sleep is 1s)
		CanceledTerminatedCountAsFailures bool          // Whether cancelled+terminated count for pause-on-failure

		// TODO - incomplete tweakables list
	}

	// V2 Scheduler dynamic config, shared among all sub state machines.
	Config struct {
		Tweakables       dynamicconfig.TypedPropertyFnWithNamespaceFilter[Tweakables]
		ExecutionTimeout dynamicconfig.DurationPropertyFnWithNamespaceFilter
	}
)

var (
	// TODO - fix namespaces after removal of prototype
	CurrentTweakables = dynamicconfig.NewNamespaceTypedSetting(
		"component.scheduler.tweakables",
		DefaultTweakables,
		"A set of tweakable parameters for the CHASM Scheduler")

	ExecutionTimeout = dynamicconfig.NewNamespaceDurationSetting(
		"component.scheduler.executionTimeout",
		time.Second*10,
		`ExecutionTimeout is the timeout for executing a single scheduler task.`,
	)

	DefaultTweakables = Tweakables{
		DefaultCatchupWindow:              365 * 24 * time.Hour,
		MinCatchupWindow:                  10 * time.Second,
		MaxBufferSize:                     1000,
		BackfillsPerIteration:             10,
		CanceledTerminatedCountAsFailures: false,
	}
)

func ConfigProvider(dc *dynamicconfig.Collection) *Config {
	return &Config{
		Tweakables:       CurrentTweakables.Get(dc),
		ExecutionTimeout: ExecutionTimeout.Get(dc),
	}
}
