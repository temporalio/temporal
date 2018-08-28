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

package host

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"flag"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/client"
	"go.uber.org/cadence/encoded"
	cworker "go.uber.org/cadence/worker"
	"go.uber.org/cadence/workflow"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/tchannel"
	"go.uber.org/zap"
)

type (
	clientIntegrationSuite struct {
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		suite.Suite
		IntegrationBase
		domainName string
		domainID   string
		wfService  workflowserviceclient.Interface
		wfClient   client.Client
		worker     cworker.Worker
		taskList   string
	}
)

func TestClientIntegrationSuite(t *testing.T) {
	flag.Parse()
	if *integration {
		s := new(clientIntegrationSuite)
		suite.Run(t, s)
	} else {
		t.Skip()
	}
}

func (s *clientIntegrationSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}

	logger := log.New()
	formatter := &log.TextFormatter{}
	formatter.FullTimestamp = true
	logger.Formatter = formatter
	//logger.Level = log.DebugLevel
	s.logger = bark.NewLoggerFromLogrus(logger)
	s.setupSuite(false, false)

	var err error
	s.wfService, err = s.buildServiceClient()
	if err != nil {
		s.logger.Fatalf("Error when build service client: %v", err)
	}
	s.wfClient = client.NewClient(s.wfService, s.domainName, nil)

	s.taskList = "client-integration-test-tasklist"
	s.worker = cworker.New(s.wfService, s.domainName, s.taskList, cworker.Options{})
	if err := s.worker.Start(); err != nil {
		s.logger.Fatalf("Error when start worker: %v", err)
	}
}

func (s *clientIntegrationSuite) buildServiceClient() (workflowserviceclient.Interface, error) {
	cadenceClientName := "cadence-client"
	cadenceFrontendService := common.FrontendServiceName
	hostPort := "127.0.0.1:7104"

	ch, err := tchannel.NewChannelTransport(
		tchannel.ServiceName(cadenceClientName), tchannel.ListenAddr("127.0.0.1:0"))
	if err != nil {
		s.logger.Fatalf("Failed to create transport channel: %v", err)
	}

	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name: cadenceClientName,
		Outbounds: yarpc.Outbounds{
			cadenceFrontendService: {Unary: ch.NewSingleOutbound(hostPort)},
		},
	})
	if dispatcher == nil {
		s.logger.Fatal("No RPC dispatcher provided to create a connection to Cadence Service")
	}
	if err := dispatcher.Start(); err != nil {
		s.logger.Fatalf("Failed to create outbound transport channel: %v", err)
	}

	return workflowserviceclient.New(dispatcher.ClientConfig(cadenceFrontendService)), nil
}

func (s *clientIntegrationSuite) TearDownSuite() {
	s.host.Stop()
	s.host = nil
	s.worker.Stop()
	s.TearDownWorkflowStore()
}

func (s *clientIntegrationSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
}

func (s *clientIntegrationSuite) TearDownTest() {

}

func (s *clientIntegrationSuite) setupSuite(enableGlobalDomain bool, isMasterCluster bool) {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
	options := persistence.TestBaseOptions{}
	options.ClusterHost = "127.0.0.1"
	options.DropKeySpace = true
	options.SchemaDir = ".."
	options.EnableGlobalDomain = enableGlobalDomain
	options.IsMasterCluster = isMasterCluster
	s.SetupWorkflowStoreWithOptions(options, nil)

	s.setupShards()

	// TODO: Use mock messaging client until we support kafka setup onebox to write end-to-end integration test
	s.mockProducer = &mocks.KafkaProducer{}
	s.mockMessagingClient = mocks.NewMockMessagingClient(s.mockProducer, nil)

	s.host = NewCadence(s.ClusterMetadata, s.mockMessagingClient, s.MetadataProxy, s.MetadataManagerV2, s.ShardMgr, s.HistoryMgr, s.ExecutionMgrFactory, s.TaskMgr,
		s.VisibilityMgr, testNumberOfHistoryShards, testNumberOfHistoryHosts, s.logger, 0, false)
	s.host.Start()

	s.engine = s.host.GetFrontendClient()
	s.domainName = "integration-test-domain"
	s.domainID = uuid.New()

	s.MetadataManager.CreateDomain(&persistence.CreateDomainRequest{
		Info: &persistence.DomainInfo{
			ID:          s.domainID,
			Name:        s.domainName,
			Status:      persistence.DomainStatusRegistered,
			Description: "Test domain for integration test",
		},
		Config: &persistence.DomainConfig{
			Retention:  1,
			EmitMetric: false,
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{},
	})

	workflow.Register(testDataConverterWorkflow)
	activity.Register(testActivity)
	workflow.Register(testParentWorkflow)
	workflow.Register(testChildWorkflow)
}

// testDataConverter implements encoded.DataConverter using gob
type testDataConverter struct {
	NumOfCallToData   int // for testing to know testDataConverter is called as expected
	NumOfCallFromData int
}

func (tdc *testDataConverter) ToData(value ...interface{}) ([]byte, error) {
	tdc.NumOfCallToData++
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	for i, obj := range value {
		if err := enc.Encode(obj); err != nil {
			return nil, fmt.Errorf(
				"unable to encode argument: %d, %v, with gob error: %v", i, reflect.TypeOf(obj), err)
		}
	}
	return buf.Bytes(), nil
}

func (tdc *testDataConverter) FromData(input []byte, valuePtr ...interface{}) error {
	tdc.NumOfCallFromData++
	dec := gob.NewDecoder(bytes.NewBuffer(input))
	for i, obj := range valuePtr {
		if err := dec.Decode(obj); err != nil {
			return fmt.Errorf(
				"unable to decode argument: %d, %v, with gob error: %v", i, reflect.TypeOf(obj), err)
		}
	}
	return nil
}

func newTestDataConverter() encoded.DataConverter {
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
	// with new taskList so that worker with same data converter can properly process tasks.
	var result1 string
	ctx1 := workflow.WithDataConverter(ctx, newTestDataConverter())
	ctx1 = workflow.WithTaskList(ctx1, tl)
	err1 := workflow.ExecuteActivity(ctx1, testActivity, "world1").Get(ctx1, &result1)
	if err1 != nil {
		return "", err1
	}
	return result + "," + result1, nil
}

func (s *clientIntegrationSuite) startWorkerWithDataConverter(tl string, dataConverter encoded.DataConverter) cworker.Worker {
	opts := cworker.Options{}
	if dataConverter != nil {
		opts.DataConverter = dataConverter
	}
	worker := cworker.New(s.wfService, s.domainName, tl, opts)
	if err := worker.Start(); err != nil {
		s.logger.Fatalf("Error when start worker with data converter: %v", err)
	}
	return worker
}

func (s *clientIntegrationSuite) TestClientDataConverter() {
	tl := "client-integration-data-converter-activity-tasklist"
	dc := newTestDataConverter()
	worker := s.startWorkerWithDataConverter(tl, dc)
	defer worker.Stop()

	id := "client-integration-data-converter-workflow"
	workflowOptions := client.StartWorkflowOptions{
		ID:                           id,
		TaskList:                     s.taskList,
		ExecutionStartToCloseTimeout: 60 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	we, err := s.wfClient.ExecuteWorkflow(ctx, workflowOptions, testDataConverterWorkflow, tl)
	if err != nil {
		s.logger.Fatalf("Start workflow with err: %v", err)
	}
	s.NotNil(we)
	s.True(we.GetRunID() != "")

	var res string
	err = we.Get(ctx, &res)
	s.NoError(err)
	s.Equal("hello_world,hello_world1", res)

	// to ensure custom data converter is used, this number might be different if client changed.
	d := dc.(*testDataConverter)
	s.Equal(1, d.NumOfCallToData)
	s.Equal(1, d.NumOfCallFromData)
}

func (s *clientIntegrationSuite) TestClientDataConverter_Failed() {
	tl := "client-integration-data-converter-activity-failed-tasklist"
	worker := s.startWorkerWithDataConverter(tl, nil) // mismatch of data converter
	defer worker.Stop()

	id := "client-integration-data-converter-failed-workflow"
	workflowOptions := client.StartWorkflowOptions{
		ID:                           id,
		TaskList:                     s.taskList,
		ExecutionStartToCloseTimeout: 60 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	we, err := s.wfClient.ExecuteWorkflow(ctx, workflowOptions, testDataConverterWorkflow, tl)
	if err != nil {
		s.logger.Fatalf("Start workflow with err: %v", err)
	}
	s.NotNil(we)
	s.True(we.GetRunID() != "")

	var res string
	err = we.Get(ctx, &res)
	s.Error(err)

	// Get history to make sure only the 2nd activity is failed because of mismatch of data converter
	iter := s.wfClient.GetWorkflowHistory(ctx, id, we.GetRunID(), false, 0)
	completedAct := 0
	failedAct := 0
	for iter.HasNext() {
		event, _ := iter.Next()
		if event.GetEventType() == shared.EventTypeActivityTaskCompleted {
			completedAct++
		}
		if event.GetEventType() == shared.EventTypeActivityTaskFailed {
			failedAct++
			attr := event.ActivityTaskFailedEventAttributes
			s.True(strings.HasPrefix(string(attr.Details), "unable to decode the activity function input bytes with error"))
		}
	}
	s.Equal(1, completedAct)
	s.Equal(1, failedAct)
}

var childTaskList = "client-integration-data-converter-child-tasklist"

func testParentWorkflow(ctx workflow.Context) (string, error) {
	logger := workflow.GetLogger(ctx)
	execution := workflow.GetInfo(ctx).WorkflowExecution
	childID := fmt.Sprintf("child_workflow:%v", execution.RunID)
	cwo := workflow.ChildWorkflowOptions{
		WorkflowID:                   childID,
		ExecutionStartToCloseTimeout: time.Minute,
	}
	ctx = workflow.WithChildOptions(ctx, cwo)
	var result string
	err := workflow.ExecuteChildWorkflow(ctx, testChildWorkflow, 0, 3).Get(ctx, &result)
	if err != nil {
		logger.Error("Parent execution received child execution failure.", zap.Error(err))
		return "", err
	}

	childID1 := fmt.Sprintf("child_workflow1:%v", execution.RunID)
	cwo1 := workflow.ChildWorkflowOptions{
		WorkflowID:                   childID1,
		ExecutionStartToCloseTimeout: time.Minute,
		TaskList:                     childTaskList,
	}
	ctx1 := workflow.WithChildOptions(ctx, cwo1)
	ctx1 = workflow.WithDataConverter(ctx1, newTestDataConverter())
	var result1 string
	err1 := workflow.ExecuteChildWorkflow(ctx1, testChildWorkflow, 0, 2).Get(ctx1, &result1)
	if err1 != nil {
		logger.Error("Parent execution received child execution 1 failure.", zap.Error(err1))
		return "", err1
	}

	res := fmt.Sprintf("Complete child1 %s times, complete child2 %s times", result, result1)
	logger.Info("Parent execution completed.", zap.String("Result", res))
	return res, nil
}

func testChildWorkflow(ctx workflow.Context, totalCount, runCount int) (string, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Child workflow execution started.")
	if runCount <= 0 {
		logger.Error("Invalid valid for run count.", zap.Int("RunCount", runCount))
		return "", errors.New("invalid run count")
	}

	totalCount++
	runCount--
	if runCount == 0 {
		result := fmt.Sprintf("Child workflow execution completed after %v runs", totalCount)
		logger.Info("Child workflow completed.", zap.String("Result", result))
		return strconv.Itoa(totalCount), nil
	}

	logger.Info("Child workflow starting new run.", zap.Int("RunCount", runCount), zap.Int("TotalCount",
		totalCount))
	return "", workflow.NewContinueAsNewError(ctx, testChildWorkflow, totalCount, runCount)
}

func (s *clientIntegrationSuite) TestClientDataConverter_WithChild() {
	dc := newTestDataConverter()
	worker := s.startWorkerWithDataConverter(childTaskList, dc)
	defer worker.Stop()

	id := "client-integration-data-converter-with-child-workflow"
	workflowOptions := client.StartWorkflowOptions{
		ID:                           id,
		TaskList:                     s.taskList,
		ExecutionStartToCloseTimeout: 60 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	we, err := s.wfClient.ExecuteWorkflow(ctx, workflowOptions, testParentWorkflow)
	if err != nil {
		s.logger.Fatalf("Start workflow with err: %v", err)
	}
	s.NotNil(we)
	s.True(we.GetRunID() != "")

	var res string
	err = we.Get(ctx, &res)
	s.NoError(err)
	s.Equal("Complete child1 3 times, complete child2 2 times", res)

	// to ensure custom data converter is used, this number might be different if client changed.
	d := dc.(*testDataConverter)
	s.Equal(3, d.NumOfCallToData)
	s.Equal(2, d.NumOfCallFromData)
}
