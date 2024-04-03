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
		// RegisterWorkflow registers Workflow types provided by this worker component.
		// Local Activities should also be registered here because they are executed on the workflow worker.
		RegisterWorkflow(registry sdkworker.Registry)
		// DedicatedWorkflowWorkerOptions returns a DedicatedWorkerOptions for this worker component.
		// Return nil to use default worker instance.
		DedicatedWorkflowWorkerOptions() *DedicatedWorkerOptions
		// RegisterActivities registers remote Activity types provided by this worker component.
		// Local Activities should be registered in RegisterWorkflow
		RegisterActivities(registry sdkworker.Registry)
		// DedicatedActivityWorkerOptions returns a DedicatedWorkerOptions for this worker component.
		// Return nil to use default worker instance.
		DedicatedActivityWorkerOptions() *DedicatedWorkerOptions
	}

	DedicatedWorkerOptions struct {
		// TaskQueue is optional
		TaskQueue string
		// How many worker nodes should run a worker per namespace
		NumWorkers int
		// Other worker options
		Options sdkworker.Options
	}

	ActivityWorkerLimitsConfig struct {
		// copy of relevant remote activity controls from sdkworker.Options
		MaxConcurrentActivityExecutionSize int
		TaskQueueActivitiesPerSecond       float64
		WorkerActivitiesPerSecond          float64
		MaxConcurrentActivityTaskPollers   int
	}

	// PerNSWorkerComponent represents a per-namespace worker needed for worker role
	PerNSWorkerComponent interface {
		// Register registers Workflow and Activity types provided by this worker component.
		// The namespace that this worker is running in is also provided.
		Register(sdkworker.Registry, *namespace.Namespace, RegistrationDetails)
		// DedicatedWorkerOptions returns a PerNSDedicatedWorkerOptions for this worker component.
		DedicatedWorkerOptions(*namespace.Namespace) *PerNSDedicatedWorkerOptions
	}

	PerNSDedicatedWorkerOptions struct {
		// Set this to false to disable this worker for this namespace
		Enabled bool
	}

	RegistrationDetails struct {
		// TotalWorkers is the number of requested per-namespace workers for this namespace.
		TotalWorkers int
		// Multiplicity is the number of those workers that this particular sdkworker.Worker
		// represents. It may be more than one if the requested number is more than the total
		// number of worker nodes or if consistent hashing decided to place more than one on
		// the same node.
		Multiplicity int
	}
)
