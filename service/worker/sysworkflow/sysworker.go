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

package sysworkflow

import (
	"context"
	"github.com/uber-go/tally"
	"github.com/uber/cadence/client/frontend"
	"github.com/uber/cadence/common/archival"
	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/worker"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"
)

type (
	// Config for SysWorker
	Config struct{}
	// SysWorker is the cadence client worker responsible for running system workflows
	SysWorker struct {
		worker worker.Worker
	}
)

func init() {
	workflow.RegisterWithOptions(SystemWorkflow, workflow.RegisterOptions{Name: SystemWorkflowFnName})
	activity.RegisterWithOptions(ArchivalActivity, activity.RegisterOptions{Name: ArchivalActivityFnName})
}

// NewSysWorker returns a new SysWorker
func NewSysWorker(frontendClient frontend.Client, scope tally.Scope, archivalClient archival.Client) *SysWorker {
	logger, _ := zap.NewProduction()
	actCtx := context.WithValue(context.Background(), archivalClientKey, archivalClient)
	actCtx = context.WithValue(context.Background(), frontendClientKey, frontendClient)
	wo := worker.Options{
		Logger:                    logger,
		MetricsScope:              scope.SubScope(SystemWorkflowScope),
		BackgroundActivityContext: actCtx,
	}
	return &SysWorker{
		// TODO: after we do task list fan out workers should listen on all task lists
		worker: worker.New(frontendClient, Domain, DecisionTaskList, wo),
	}
}

// Start the SysWorker
func (w *SysWorker) Start() error {
	if err := w.worker.Start(); err != nil {
		w.worker.Stop()
		return err
	}
	return nil
}

// Stop the SysWorker
func (w *SysWorker) Stop() {
	w.worker.Stop()
}
