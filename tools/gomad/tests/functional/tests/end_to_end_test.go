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

package tests_test

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/durationpb"

	"go.temporal.io/server/common/membership/static"

	"go.temporal.io/server/common/config"
	logger "go.temporal.io/server/common/log"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/temporal"

	SIMLIB "go.temporal.io/server/tools/gomad/api/lib"
	_ "go.temporal.io/server/tools/gomad/api/runtime"
)

const (
	taskQueue  = "MY-TASKQUEUE"
	workflowID = "MY-WORKFLOW"
)

func TestEndToEnd(t *testing.T) {
	// TODO
	SIMLIB.WriteFile("/executable", []byte(""), 0775)

	stopServer := startServer()
	c, err := client.Dial(client.Options{})
	if err != nil {
		panic(err)
	}
	defer c.Close()
	done := starter(c)

	stopWorker := startWorker(c)

	<-done

	stopWorker()
	stopServer()
}

func startServer() func() {
	ready := make(chan bool)
	stop := make(chan bool)
	stopped := make(chan bool)

	fmt.Println("[[ SERVER STARTING ]]", time.Now())

	go func() {
		srcConfigDir := filepath.Join("..", "test-conf")
		targetConfigDir := filepath.Join("gomad", "test-conf")
		SIMLIB.LoadFileFromDisk(srcConfigDir, targetConfigDir, "dev.yaml")
		SIMLIB.LoadFileFromDisk(srcConfigDir, targetConfigDir, "dynconfig.yaml")

		cfg, err := config.LoadConfig("dev", targetConfigDir, "")
		if err != nil {
			panic(err)
		}
		cfg.DynamicConfigClient.Filepath = filepath.Join(targetConfigDir, cfg.DynamicConfigClient.Filepath)

		s, err := temporal.NewServer(
			temporal.ForServices(
				[]string{
					string(primitives.FrontendService),
					string(primitives.HistoryService),
					string(primitives.MatchingService),
					//string(primitives.WorkerService),
				}),
			temporal.WithStaticHosts(map[primitives.ServiceName]static.Hosts{
				primitives.FrontendService: static.SingleLocalHost(
					fmt.Sprintf("127.0.0.1:%v", cfg.Services[string(primitives.FrontendService)].RPC.GRPCPort)),
				primitives.MatchingService: static.SingleLocalHost(
					fmt.Sprintf("127.0.0.1:%v", cfg.Services[string(primitives.MatchingService)].RPC.GRPCPort)),
				primitives.HistoryService: static.SingleLocalHost(
					fmt.Sprintf("127.0.0.1:%v", cfg.Services[string(primitives.HistoryService)].RPC.GRPCPort)),
				primitives.WorkerService: static.SingleLocalHost(
					fmt.Sprintf("127.0.0.1:%v", cfg.Services[string(primitives.WorkerService)].RPC.GRPCPort)),
			}),
			temporal.WithConfig(cfg),
			temporal.WithLogger(logger.NewZapLogger(logger.BuildZapLogger(logger.Config{Format: "console", Level: "ERROR"}))))

		if err != nil {
			panic(err)
		}

		defer func() {
			fmt.Println("[[ SERVER STOPPING ]]", time.Now())
			s.Stop()
			stopped <- true
		}()
		err = s.Start()
		if err != nil {
			panic(err)
		}

		ready <- true
		<-stop
	}()

	<-ready
	fmt.Println("[[ SERVER STARTED ]]")

	// create default namespace
	nsClient, err := client.NewNamespaceClient(client.Options{})
	if err != nil {
		panic(err)
	}
	err = nsClient.Register(context.Background(), &workflowservice.RegisterNamespaceRequest{
		Namespace:                        "default",
		WorkflowExecutionRetentionPeriod: durationpb.New(7 * time.Hour * 24),
	})
	if err != nil {
		panic(err)
	}

	fmt.Println("[[ NAMESPACE CREATED ]]")

	return func() {
		stop <- true
		<-stopped
	}
}

func starter(c client.Client) chan bool {
	done := make(chan bool)

	go func() {
		fmt.Println("[[ STARTER START ]]")
		workflowOptions := client.StartWorkflowOptions{
			ID:               workflowID,
			TaskQueue:        taskQueue,
			EnableEagerStart: true,
		}
		we, err := c.ExecuteWorkflow(context.Background(), workflowOptions, Workflow)
		if err != nil {
			panic(err)
		}

		if err = c.SignalWorkflow(context.Background(), we.GetID(), we.GetRunID(), "SIGNAL", "hello"); err != nil {
			fmt.Println("Signal error", err)
		}

		var res string
		if err = we.Get(context.Background(), &res); err != nil {
			fmt.Println("Workflow error", err)
		}
		fmt.Println("Result: " + res)

		fmt.Println("[[ STARTER END ]]")

		done <- true
	}()

	return done
}

func Workflow(ctx workflow.Context) (string, error) {
	fmt.Println("[[ WORKFLOW START ]]")

	var input string
	_ = workflow.GetSignalChannel(ctx, "SIGNAL").Receive(ctx, &input)

	fmt.Println("[[ WORKFLOW END ]]")
	return strings.ToUpper(input), ctx.Err()
}

func startWorker(c client.Client) func() {
	ready := make(chan bool)
	stop := make(chan bool)
	stopped := make(chan bool)

	go func() {
		w := worker.New(c, taskQueue, worker.Options{})
		w.RegisterWorkflow(Workflow)

		defer func() {
			fmt.Println("[[ WORKER STOPPING ]]")
			w.Stop()
			stopped <- true
		}()

		err := w.Start()
		if err != nil {
			panic(err)
		}

		ready <- true
		<-stop
	}()

	<-ready
	fmt.Println("[[ WORKER STARTED ]]")

	return func() {
		stop <- true
		<-stopped
	}
}
