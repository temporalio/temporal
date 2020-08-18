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

package host

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"

	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/rpc"
)

type (
	clientIntegrationSuite struct {
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		IntegrationBase
		hostPort  string
		sdkClient sdkclient.Client
		worker    worker.Worker
		taskQueue string
	}
)

var (
	ErrEncodingIsNotSet       = errors.New("payload encoding metadata is not set")
	ErrEncodingIsNotSupported = errors.New("payload encoding is not supported")
)

func TestClientIntegrationSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(clientIntegrationSuite))
}

func (s *clientIntegrationSuite) SetupSuite() {
	s.setupSuite("testdata/clientintegrationtestcluster.yaml")

	s.hostPort = "127.0.0.1:7134"
	if TestFlags.FrontendAddr != "" {
		s.hostPort = TestFlags.FrontendAddr
	}

	var err error
	s.sdkClient, err = sdkclient.NewClient(sdkclient.Options{
		HostPort:  s.hostPort,
		Namespace: s.namespace,
		ConnectionOptions: sdkclient.ConnectionOptions{
			DisableHealthCheck: true,
		},
	})
	if err != nil {
		s.Logger.Fatal("Error when creating SDK client", tag.Error(err))
	}

	s.taskQueue = "client-integration-test-taskqueue"
	s.worker = worker.New(s.sdkClient, s.taskQueue, worker.Options{})

	s.worker.RegisterWorkflow(testDataConverterWorkflow)
	s.worker.RegisterWorkflow(testParentWorkflow)
	s.worker.RegisterActivity(testActivity)
	s.worker.RegisterWorkflow(testChildWorkflow)

	if err := s.worker.Start(); err != nil {
		s.Logger.Fatal("Error when start worker", tag.Error(err))
	}
}

func (s *clientIntegrationSuite) TearDownSuite() {
	s.sdkClient.Close()
	s.tearDownSuite()
}

func (s *clientIntegrationSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
}

// testDataConverter implements encoded.DataConverter using gob
type testDataConverter struct {
	NumOfCallToPayloads   int // for testing to know testDataConverter is called as expected
	NumOfCallFromPayloads int
}

func (tdc *testDataConverter) ToPayloads(values ...interface{}) (*commonpb.Payloads, error) {
	tdc.NumOfCallToPayloads++
	result := &commonpb.Payloads{}
	for i, value := range values {
		p, err := tdc.ToPayload(value)
		if err != nil {
			return nil, fmt.Errorf(
				"args[%d], %T: %w", i, value, err)
		}
		result.Payloads = append(result.Payloads, p)
	}
	return result, nil
}

func (tdc *testDataConverter) FromPayloads(payloads *commonpb.Payloads, valuePtrs ...interface{}) error {
	tdc.NumOfCallFromPayloads++
	for i, p := range payloads.GetPayloads() {
		err := tdc.FromPayload(p, valuePtrs[i])
		if err != nil {
			return fmt.Errorf("args[%d]: %w", i, err)
		}
	}
	return nil
}

func (tdc *testDataConverter) ToPayload(value interface{}) (*commonpb.Payload, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(value); err != nil {
		return nil, err
	}
	p := &commonpb.Payload{
		Metadata: map[string][]byte{
			"encoding": []byte("gob"),
		},
		Data: buf.Bytes(),
	}
	return p, nil
}

func (tdc *testDataConverter) FromPayload(payload *commonpb.Payload, valuePtr interface{}) error {
	encoding, ok := payload.GetMetadata()["encoding"]
	if !ok {
		return ErrEncodingIsNotSet
	}

	e := string(encoding)
	if e != "gob" {
		return ErrEncodingIsNotSupported
	}

	return decodeGob(payload, valuePtr)
}

func (tdc *testDataConverter) ToStrings(payloads *commonpb.Payloads) []string {
	var result []string
	for _, p := range payloads.GetPayloads() {
		result = append(result, tdc.ToString(p))
	}

	return result
}

func decodeGob(payload *commonpb.Payload, valuePtr interface{}) error {
	dec := gob.NewDecoder(bytes.NewBuffer(payload.GetData()))
	return dec.Decode(valuePtr)
}

func (tdc *testDataConverter) ToString(payload *commonpb.Payload) string {
	encoding, ok := payload.GetMetadata()["encoding"]
	if !ok {
		return ErrEncodingIsNotSet.Error()
	}

	e := string(encoding)
	if e != "gob" {
		return ErrEncodingIsNotSupported.Error()
	}

	var value interface{}
	err := decodeGob(payload, &value)
	if err != nil {
		return err.Error()
	}

	return fmt.Sprintf("%+v", value)
}

func newTestDataConverter() converter.DataConverter {
	return &testDataConverter{}
}

func testActivity(ctx context.Context, msg string) (string, error) {
	return "hello_" + msg, nil
}

func testDataConverterWorkflow(ctx workflow.Context, tl string) (string, error) {
	ao := workflow.ActivityOptions{
		ScheduleToStartTimeout: 20 * time.Second,
		StartToCloseTimeout:    40 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	var result string
	err := workflow.ExecuteActivity(ctx, testActivity, "world").Get(ctx, &result)
	if err != nil {
		return "", err
	}

	// use another converter to run activity,
	// with new taskQueue so that worker with same data converter can properly process tasks.
	var result1 string
	ctx1 := workflow.WithDataConverter(ctx, newTestDataConverter())
	ctx1 = workflow.WithTaskQueue(ctx1, tl)
	err1 := workflow.ExecuteActivity(ctx1, testActivity, "world1").Get(ctx1, &result1)
	if err1 != nil {
		return "", err1
	}
	return result + "," + result1, nil
}

func (s *clientIntegrationSuite) startWorkerWithDataConverter(tl string, dataConverter converter.DataConverter) (sdkclient.Client, worker.Worker) {
	sdkClient, err := sdkclient.NewClient(sdkclient.Options{
		HostPort:      s.hostPort,
		Namespace:     s.namespace,
		DataConverter: dataConverter,
		ConnectionOptions: sdkclient.ConnectionOptions{
			DisableHealthCheck: true,
		},
	})
	if err != nil {
		s.Logger.Fatal("Error when creating SDK client", tag.Error(err))
	}

	worker := worker.New(sdkClient, tl, worker.Options{})
	worker.RegisterActivity(testActivity)
	worker.RegisterWorkflow(testChildWorkflow)

	if err := worker.Start(); err != nil {
		s.Logger.Fatal("Error when start worker with data converter", tag.Error(err))
	}
	return sdkClient, worker
}

func (s *clientIntegrationSuite) TestClientDataConverter() {
	tl := "client-integration-data-converter-activity-taskqueue"
	dc := newTestDataConverter()
	sdkClient, worker := s.startWorkerWithDataConverter(tl, dc)
	defer func() {
		worker.Stop()
		sdkClient.Close()
	}()

	id := "client-integration-data-converter-workflow"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 id,
		TaskQueue:          s.taskQueue,
		WorkflowRunTimeout: time.Minute,
	}
	ctx, cancel := rpc.NewContextWithTimeoutAndHeaders(time.Minute)
	defer cancel()
	we, err := s.sdkClient.ExecuteWorkflow(ctx, workflowOptions, testDataConverterWorkflow, tl)
	if err != nil {
		s.Logger.Fatal("Start workflow with err", tag.Error(err))
	}
	s.NotNil(we)
	s.True(we.GetRunID() != "")

	var res string
	err = we.Get(ctx, &res)
	s.NoError(err)
	s.Equal("hello_world,hello_world1", res)

	// to ensure custom data converter is used, this number might be different if client changed.
	d := dc.(*testDataConverter)
	s.Equal(1, d.NumOfCallToPayloads)
	s.Equal(1, d.NumOfCallFromPayloads)
}

func (s *clientIntegrationSuite) TestClientDataConverter_Failed() {
	tl := "client-integration-data-converter-activity-failed-taskqueue"
	sdkClient, worker := s.startWorkerWithDataConverter(tl, nil) // mismatch of data converter
	defer func() {
		worker.Stop()
		sdkClient.Close()
	}()

	id := "client-integration-data-converter-failed-workflow"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 id,
		TaskQueue:          s.taskQueue,
		WorkflowRunTimeout: time.Minute,
	}
	ctx, cancel := rpc.NewContextWithTimeoutAndHeaders(time.Minute)
	defer cancel()
	we, err := s.sdkClient.ExecuteWorkflow(ctx, workflowOptions, testDataConverterWorkflow, tl)
	if err != nil {
		s.Logger.Fatal("Start workflow with err", tag.Error(err))
	}
	s.NotNil(we)
	s.True(we.GetRunID() != "")

	var res string
	err = we.Get(ctx, &res)
	s.Error(err)

	// Get history to make sure only the 2nd activity is failed because of mismatch of data converter
	iter := s.sdkClient.GetWorkflowHistory(ctx, id, we.GetRunID(), false, 0)
	completedAct := 0
	failedAct := 0
	for iter.HasNext() {
		event, err := iter.Next()
		s.NoError(err)
		if event.GetEventType() == enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED {
			completedAct++
		}
		if event.GetEventType() == enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED {
			failedAct++
			s.NotNil(event.GetActivityTaskFailedEventAttributes().GetFailure().GetApplicationFailureInfo())
			s.True(strings.HasPrefix(event.GetActivityTaskFailedEventAttributes().GetFailure().GetMessage(), "unable to decode the activity function input payload with error"))
		}
	}
	s.Equal(1, completedAct)
	s.Equal(1, failedAct)
}

var childTaskQueue = "client-integration-data-converter-child-taskqueue"

func testParentWorkflow(ctx workflow.Context) (string, error) {
	logger := workflow.GetLogger(ctx)
	execution := workflow.GetInfo(ctx).WorkflowExecution
	childID := fmt.Sprintf("child_workflow:%v", execution.RunID)
	cwo := workflow.ChildWorkflowOptions{
		WorkflowID:         childID,
		WorkflowRunTimeout: time.Minute,
	}
	ctx = workflow.WithChildOptions(ctx, cwo)
	var result string
	err := workflow.ExecuteChildWorkflow(ctx, testChildWorkflow, 0, 3).Get(ctx, &result)
	if err != nil {
		logger.Error("Parent execution received child execution failure", zap.Error(err))
		return "", err
	}

	childID1 := fmt.Sprintf("child_workflow1:%v", execution.RunID)
	cwo1 := workflow.ChildWorkflowOptions{
		WorkflowID:         childID1,
		WorkflowRunTimeout: time.Minute,
		TaskQueue:          childTaskQueue,
	}
	ctx1 := workflow.WithChildOptions(ctx, cwo1)
	ctx1 = workflow.WithDataConverter(ctx1, newTestDataConverter())
	var result1 string
	err1 := workflow.ExecuteChildWorkflow(ctx1, testChildWorkflow, 0, 2).Get(ctx1, &result1)
	if err1 != nil {
		logger.Error("Parent execution received child execution 1 failure", zap.Error(err1))
		return "", err1
	}

	res := fmt.Sprintf("Complete child1 %s times, complete child2 %s times", result, result1)
	logger.Info("Parent execution completed", zap.String("Result", res))
	return res, nil
}

func testChildWorkflow(ctx workflow.Context, totalCount, runCount int) (string, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Child workflow execution started")
	if runCount <= 0 {
		logger.Error("Invalid valid for run count", zap.Int("RunCount", runCount))
		return "", errors.New("invalid run count")
	}

	totalCount++
	runCount--
	if runCount == 0 {
		result := fmt.Sprintf("Child workflow execution completed after %v runs", totalCount)
		logger.Info("Child workflow completed", zap.String("Result", result))
		return strconv.Itoa(totalCount), nil
	}

	logger.Info("Child workflow starting new run", zap.Int("RunCount", runCount), zap.Int("TotalCount",
		totalCount))
	return "", workflow.NewContinueAsNewError(ctx, testChildWorkflow, totalCount, runCount)
}

func (s *clientIntegrationSuite) TestClientDataConverter_WithChild() {
	dc := newTestDataConverter()
	sdkClient, worker := s.startWorkerWithDataConverter(childTaskQueue, dc)
	defer func() {
		worker.Stop()
		sdkClient.Close()
	}()

	id := "client-integration-data-converter-with-child-workflow"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 id,
		TaskQueue:          s.taskQueue,
		WorkflowRunTimeout: time.Minute,
	}
	ctx, cancel := rpc.NewContextWithTimeoutAndHeaders(time.Minute)
	defer cancel()
	we, err := s.sdkClient.ExecuteWorkflow(ctx, workflowOptions, testParentWorkflow)
	if err != nil {
		s.Logger.Fatal("Start workflow with err", tag.Error(err))
	}
	s.NotNil(we)
	s.True(we.GetRunID() != "")

	var res string
	err = we.Get(ctx, &res)
	s.NoError(err)
	s.Equal("Complete child1 3 times, complete child2 2 times", res)

	// to ensure custom data converter is used, this number might be different if client changed.
	d := dc.(*testDataConverter)
	s.Equal(3, d.NumOfCallToPayloads)
	s.Equal(2, d.NumOfCallFromPayloads)
}
