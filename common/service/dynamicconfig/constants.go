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

package dynamicconfig

// Key represents a key/property stored in dynamic config
type Key int

func (k Key) String() string {
	if k <= unknownKey || int(k) >= len(keys) {
		return keys[unknownKey]
	}
	return keys[k]
}

const (
	_matchingRoot         = "matching."
	_matchingTaskListRoot = _matchingRoot + "taskList."
	_historyRoot          = "history."
)

var keys = []string{
	"unknownKey",
	_matchingTaskListRoot + "MinTaskThrottlingBurstSize",
	_matchingTaskListRoot + "MaxTaskBatchSize",
	_matchingTaskListRoot + "LongPollExpirationInterval",
	_historyRoot + "LongPollExpirationInterval",
}

const (
	// The order of constants is important. It should match the order in the keys array above.
	unknownKey Key = iota
	// Matching keys

	// MinTaskThrottlingBurstSize is the minimum burst size for task list throttling
	MinTaskThrottlingBurstSize
	// MaxTaskBatchSize is the maximum batch size to fetch from the task buffer
	MaxTaskBatchSize
	// MatchingLongPollExpirationInterval is the long poll expiration interval in the matching service
	MatchingLongPollExpirationInterval
	// HistoryLongPollExpirationInterval is the long poll expiration interval in the history service
	HistoryLongPollExpirationInterval
)

// Filter represents a filter on the dynamic config key
type Filter int

func (k Filter) String() string {
	keys := []string{
		"unknownFilter",
		"domainName",
		"taskListName",
	}
	if k <= unknownFilter || k > TaskListName {
		return keys[unknownFilter]
	}
	return keys[k]
}

const (
	unknownFilter Filter = iota
	// DomainName is the domain name
	DomainName
	// TaskListName is the tasklist name
	TaskListName
)
