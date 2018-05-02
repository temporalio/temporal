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

package history

import (
	"github.com/uber/cadence/common/persistence"
)

// verifyTimerTaskVersion, when not encounter, will return false if true if failover version check is successful
func verifyTransferTaskVersion(shard ShardContext, domainID string, version int64, task *persistence.TransferTaskInfo) (bool, error) {
	if !shard.GetService().GetClusterMetadata().IsGlobalDomainEnabled() {
		return true, nil
	}

	// the first return value is whether this task is valid for further processing
	domainEntry, err := shard.GetDomainCache().GetDomainByID(domainID)
	if err != nil {
		return false, err
	}
	if !domainEntry.IsGlobalDomain() {
		return true, nil
	} else if version != task.GetVersion() {
		return false, nil
	}
	return true, nil
}

// verifyTimerTaskVersion, when not encounter, will return false if true if failover version check is successful
func verifyTimerTaskVersion(shard ShardContext, domainID string, version int64, task *persistence.TimerTaskInfo) (bool, error) {
	if !shard.GetService().GetClusterMetadata().IsGlobalDomainEnabled() {
		return true, nil
	}

	// the first return value is whether this task is valid for further processing
	domainEntry, err := shard.GetDomainCache().GetDomainByID(domainID)
	if err != nil {
		return false, err
	}
	if !domainEntry.IsGlobalDomain() {
		return true, nil
	} else if version != task.GetVersion() {
		return false, nil
	}
	return true, nil
}
