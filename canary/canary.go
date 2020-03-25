// Copyright (c) 2019 Uber Technologies, Inc.
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

package canary

import (
	"context"

	"github.com/opentracing/opentracing-go"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/serviceerror"
	"go.temporal.io/temporal/activity"
	"go.temporal.io/temporal/worker"
	"go.temporal.io/temporal/workflow"
	"go.uber.org/zap"
)

type (
	// Runnable is an interface for anything that exposes a Run method
	Runnable interface {
		Run() error
	}

	registrar interface {
		RegisterWorkflowWithOptions(w interface{}, options workflow.RegisterOptions)
		RegisterActivityWithOptions(a interface{}, options activity.RegisterOptions)
		RegisterActivity(a interface{})
	}

	canaryImpl struct {
		canaryClient   cadenceClient
		canaryDomain   string
		archivalClient cadenceClient
		systemClient   cadenceClient
		runtime        *RuntimeContext
	}

	activityContext struct {
		cadence cadenceClient
	}
)

type contextKey string

const (
	// this context key should be the same as the one defined in
	// internal_worker.go, cadence go client
	testTagsContextKey contextKey = "cadence-testTags"
)

// new returns a new instance of Canary runnable
func newCanary(domain string, rc *RuntimeContext) (Runnable, error) {
	canaryClient, err := newCadenceClient(domain, rc)
	if err != nil {
		return nil, err
	}
	archivalClient, err := newCadenceClient(archivalDomain, rc)
	if err != nil {
		return nil, err
	}
	systemClient, err := newCadenceClient(systemDomain, rc)
	if err != nil {
		return nil, err
	}
	return &canaryImpl{
		canaryClient:   canaryClient,
		canaryDomain:   domain,
		archivalClient: archivalClient,
		systemClient:   systemClient,
		runtime:        rc,
	}, nil
}

// Run runs the canary
func (c *canaryImpl) Run() error {
	var err error
	log := c.runtime.logger

	if err = c.createDomain(); err != nil {
		log.Error("createDomain failed", zap.Error(err))
		return err
	}

	if err = c.createArchivalDomain(); err != nil {
		log.Error("createArchivalDomain failed", zap.Error(err))
		return err
	}

	// start the initial cron workflow
	c.startCronWorkflow()

	err = c.startWorker()
	if err != nil {
		log.Error("start worker failed", zap.Error(err))
		return err
	}
	return nil
}

func (c *canaryImpl) startWorker() error {
	options := worker.Options{
		Logger:                             c.runtime.logger,
		BackgroundActivityContext:          c.newActivityContext(),
		MaxConcurrentActivityExecutionSize: activityWorkerMaxExecutors,
	}

	archivalWorker := worker.New(c.archivalClient.Client, archivalTaskListName, options)
	registerHistoryArchival(archivalWorker)

	defer archivalWorker.Stop()
	if err := archivalWorker.Start(); err != nil {
		return err
	}

	canaryWorker := worker.New(c.canaryClient.Client, taskListName, options)
	registerBatch(canaryWorker)
	registerCancellation(canaryWorker)
	registerConcurrentExec(canaryWorker)
	registerCron(canaryWorker)
	registerEcho(canaryWorker)
	registerLocalActivity(canaryWorker)
	registerQuery(canaryWorker)
	registerReset(canaryWorker)
	registerRetry(canaryWorker)
	registerSanity(canaryWorker)
	registerSearchAttributes(canaryWorker)
	registerSignal(canaryWorker)
	registerTimeout(canaryWorker)
	registerVisibility(canaryWorker)
	return canaryWorker.Run()
}

func (c *canaryImpl) startCronWorkflow() {
	wfID := "cadence.canary.cron"
	opts := newWorkflowOptions(wfID, cronWFExecutionTimeout)
	opts.CronSchedule = "@every 30s" // run every 30s
	// create the cron workflow span
	ctx := context.Background()
	span := opentracing.StartSpan("start-cron-workflow-span")
	defer span.Finish()
	ctx = opentracing.ContextWithSpan(ctx, span)
	_, err := c.canaryClient.ExecuteWorkflow(ctx, opts, cronWorkflow, c.canaryDomain, wfTypeSanity)
	if err != nil {
		if _, ok := err.(*serviceerror.WorkflowExecutionAlreadyStarted); !ok {
			c.runtime.logger.Error("error starting cron workflow", zap.Error(err))
		}
	}
}

// newActivityContext builds an activity context containing
// logger, metricsClient and cadenceClient
func (c *canaryImpl) newActivityContext() context.Context {
	ctx := context.WithValue(context.Background(), ctxKeyActivityRuntime, &activityContext{cadence: c.canaryClient})
	ctx = context.WithValue(ctx, ctxKeyActivityArchivalRuntime, &activityContext{cadence: c.archivalClient})
	ctx = context.WithValue(ctx, ctxKeyActivitySystemClient, &activityContext{cadence: c.systemClient})
	return overrideWorkerOptions(ctx)
}

func (c *canaryImpl) createDomain() error {
	name := c.canaryDomain
	desc := "Domain for running cadence canary workflows"
	owner := "canary"
	return c.canaryClient.createDomain(name, desc, owner, enums.ArchivalStatusDisabled)
}

func (c *canaryImpl) createArchivalDomain() error {
	name := archivalDomain
	desc := "Domain used by cadence canary workflows to verify archival"
	owner := "canary"
	archivalStatus := enums.ArchivalStatusEnabled
	return c.archivalClient.createDomain(name, desc, owner, archivalStatus)
}

// Override worker options to create large number of pollers to improve the chances of activities getting sync matched
//nolint:unused
func overrideWorkerOptions(ctx context.Context) context.Context {
	optionsOverride := make(map[string]map[string]string)
	optionsOverride["worker-options"] = map[string]string{
		"ConcurrentPollRoutineSize": "20",
	}

	return context.WithValue(ctx, testTagsContextKey, optionsOverride)
}
