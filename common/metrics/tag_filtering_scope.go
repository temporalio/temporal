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

package metrics

import (
	"time"
)

type (
	TagFilteringScope struct {
		TagsToFilter map[string]map[string]struct{}
		Impl         internalScope
	}

	TemporalScope struct {
		Impl      internalScope
		RootScope internalScope
	}

	TagFilteringScopeConfig struct {
		TagsToFilter map[string]map[string]struct{}
	}
)

func NewTagFilteringScopeConfig(exclusions map[string][]string) TagFilteringScopeConfig {
	tagsToFilter := make(map[string]map[string]struct{})
	for key, val := range exclusions {
		exclusions := make(map[string]struct{})
		for _, val := range val {
			exclusions[val] = struct{}{}
		}
		tagsToFilter[key] = exclusions
	}
	return TagFilteringScopeConfig{
		TagsToFilter: tagsToFilter,
	}
}

func NewTagFilteringScope(config TagFilteringScopeConfig, impl internalScope) internalScope {
	return &TagFilteringScope{
		TagsToFilter: config.TagsToFilter,
		Impl:         impl,
	}
}

func (tfs *TagFilteringScope) IncCounter(counter int) {
	tfs.Impl.IncCounter(counter)
}

func (tfs *TagFilteringScope) AddCounter(counter int, delta int64) {
	tfs.Impl.AddCounter(counter, delta)
}

func (tfs *TagFilteringScope) StartTimer(timer int) Stopwatch {
	return tfs.Impl.StartTimer(timer)
}

func (tfs *TagFilteringScope) RecordTimer(timer int, d time.Duration) {
	tfs.Impl.RecordTimer(timer, d)
}

func (tfs *TagFilteringScope) RecordDistribution(id int, d int) {
	tfs.Impl.RecordDistribution(id, d)
}

func (tfs *TagFilteringScope) UpdateGauge(id int, value float64) {
	tfs.Impl.UpdateGauge(id, value)
}

func (tfs *TagFilteringScope) Tagged(tags ...Tag) Scope {
	return tfs.TaggedInternal(tags...)
}

func (tfs *TagFilteringScope) TaggedInternal(tags ...Tag) internalScope {
	newTags := make([]Tag, len(tags))
	for i, tag := range tags {
		newTags[i] = tag

		if val, ok := tfs.TagsToFilter[tag.Key()]; ok {
			if _, ok := val[tag.Value()]; !ok {
				newTags[i] = newValueOverrideTag(tag.Key())
			}
		}
	}
	return &TagFilteringScope{
		TagsToFilter: tfs.TagsToFilter,
		Impl:         tfs.Impl.TaggedInternal(newTags...),
	}
}

func (tfs *TagFilteringScope) AddCounterInternal(name string, delta int64) {
	tfs.Impl.AddCounterInternal(name, delta)
}

func (tfs *TagFilteringScope) StartTimerInternal(timer string) Stopwatch {
	return tfs.Impl.StartTimerInternal(timer)
}

func (tfs *TagFilteringScope) RecordTimerInternal(timer string, d time.Duration) {
	tfs.RecordTimerInternal(timer, d)
}

func (tfs *TagFilteringScope) RecordDistributionInternal(id string, d int) {
	tfs.RecordDistributionInternal(id, d)
}
