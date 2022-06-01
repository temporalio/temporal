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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination interface_mock.go

package common

import (
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/server/common/namespace"
)

type (
	// WorkerComponent represents a type of work needed for worker role
	WorkerComponent interface {
		// Register registers Workflow and Activity types provided by this worker component.
		Register(sdkworker.Worker)
		// DedicatedWorkerOptions returns a DedicatedWorkerOptions for this worker component.
		// Return nil to use default worker instance.
		DedicatedWorkerOptions() *DedicatedWorkerOptions
	}

	DedicatedWorkerOptions struct {
		// TaskQueue is optional
		TaskQueue string
		// How many worker nodes should run a worker per namespace
		NumWorkers int
		// Other worker options
		Options sdkworker.Options
	}

	// PerNSWorkerComponent represents a per-namespace worker needed for worker role
	PerNSWorkerComponent interface {
		// Register registers Workflow and Activity types provided by this worker component.
		// The namespace that this worker is running in is also provided.
		Register(sdkworker.Worker, *namespace.Namespace)
		// DedicatedWorkerOptions returns a PerNSDedicatedWorkerOptions for this worker component.
		DedicatedWorkerOptions(*namespace.Namespace) *PerNSDedicatedWorkerOptions
	}

	PerNSDedicatedWorkerOptions struct {
		// Set this to false to disable a worker for this namespace
		Enabled bool
		// TaskQueue is required
		TaskQueue string
		// How many worker nodes should run a worker per namespace
		NumWorkers int
		// Other worker options
		Options sdkworker.Options
	}
)
