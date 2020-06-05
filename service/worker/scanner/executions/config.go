// The MIT License (MIT)
//
// Copyright (c) 2017-2020 Uber Technologies Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package executions

import "github.com/uber/cadence/common/service/dynamicconfig"

type (
	// ScannerWorkflowDynamicConfig is the dynamic config for scanner workflow
	ScannerWorkflowDynamicConfig struct {
		Enabled                           dynamicconfig.BoolPropertyFn
		Concurrency                       dynamicconfig.IntPropertyFn
		ExecutionsPageSize                dynamicconfig.IntPropertyFn
		BlobstoreFlushThreshold           dynamicconfig.IntPropertyFn
		ActivityBatchSize                 dynamicconfig.IntPropertyFn
		DynamicConfigInvariantCollections DynamicConfigInvariantCollections
	}

	// DynamicConfigInvariantCollections is the portion of ScannerWorkflowDynamicConfig
	// which indicates which collections of invariants should be run
	DynamicConfigInvariantCollections struct {
		InvariantCollectionMutableState dynamicconfig.BoolPropertyFn
		InvariantCollectionHistory      dynamicconfig.BoolPropertyFn
	}

	// ScannerWorkflowConfigOverwrites enables overwriting the values in dynamic config.
	// If provided workflow will favor overwrites over dynamic config.
	// Any overwrites that are left as nil will fall back to using dynamic config.
	ScannerWorkflowConfigOverwrites struct {
		Enabled                 *bool
		Concurrency             *int
		ExecutionsPageSize      *int
		BlobstoreFlushThreshold *int
		ActivityBatchSize       *int
		InvariantCollections    *InvariantCollections
	}

	// ResolvedScannerWorkflowConfig is the resolved config after reading dynamic config
	// and applying overwrites.
	ResolvedScannerWorkflowConfig struct {
		Enabled                 bool
		Concurrency             int
		ExecutionsPageSize      int
		BlobstoreFlushThreshold int
		ActivityBatchSize       int
		InvariantCollections    InvariantCollections
	}

	// InvariantCollections represents the resolved set of invariant collections
	// that scanner workflow should run
	InvariantCollections struct {
		InvariantCollectionMutableState bool
		InvariantCollectionHistory      bool
	}

	// FixerWorkflowConfigOverwrites enables overwriting the default values.
	// If provided workflow will favor overwrites over defaults.
	// Any overwrites that are left as nil will fall back to defaults.
	FixerWorkflowConfigOverwrites struct {
		Concurrency             *int
		BlobstoreFlushThreshold *int
		ActivityBatchSize       *int
		InvariantCollections    *InvariantCollections
	}

	// ResolvedFixerWorkflowConfig is the resolved config after reading defaults and applying overwrites.
	ResolvedFixerWorkflowConfig struct {
		Concurrency             int
		BlobstoreFlushThreshold int
		ActivityBatchSize       int
		InvariantCollections    InvariantCollections
	}
)
