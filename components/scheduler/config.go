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

package scheduler

import (
	"time"

	"go.temporal.io/server/common/dynamicconfig"
)

var UseExperimentalHsmScheduler = dynamicconfig.NewNamespaceBoolSetting(
	"scheduler.use-experimental-hsm-scheduler",
	false,
	"When true, use the experimental scheduler implemented using the HSM framework instead of workflows")

const (
	RecentActionCount = 10
)

type Tweakables struct {
	DefaultCatchupWindow              time.Duration // Default for catchup window
	MinCatchupWindow                  time.Duration // Minimum for catchup window
	MaxBufferSize                     int           // MaxBufferSize limits the number of buffered starts and backfills
	BackfillsPerIteration             int           // How many backfilled actions to take per iteration (implies rate limit since min sleep is 1s)
	CanceledTerminatedCountAsFailures bool          // Whether cancelled+terminated count for pause-on-failure

}

var DefaultTweakables = Tweakables{
	DefaultCatchupWindow:              365 * 24 * time.Hour,
	MinCatchupWindow:                  10 * time.Second,
	MaxBufferSize:                     1000,
	BackfillsPerIteration:             10,
	CanceledTerminatedCountAsFailures: false,
}

var CurrentTweakables = dynamicconfig.NewNamespaceTypedSetting[Tweakables](
	"component.scheduler.tweakables",
	DefaultTweakables,
	"A set of tweakable parameters for the schedulers")

var ExecutionTimeout = dynamicconfig.NewNamespaceDurationSetting(
	"component.scheduler.executionTimeout",
	time.Second*10,
	`ExecutionTimeout is the timeout for executing a single scheduler task.`,
)

type Config struct {
	Tweakables       dynamicconfig.TypedPropertyFnWithNamespaceFilter[Tweakables]
	ExecutionTimeout dynamicconfig.DurationPropertyFnWithNamespaceFilter
}

func ConfigProvider(dc *dynamicconfig.Collection) *Config {
	return &Config{
		Tweakables:       CurrentTweakables.Get(dc),
		ExecutionTimeout: ExecutionTimeout.Get(dc),
	}
}
