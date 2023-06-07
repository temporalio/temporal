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

package tests

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"gopkg.in/yaml.v3"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	namespacepb "go.temporal.io/api/namespace/v1"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/environment"
)

type (
	// IntegrationBase is a base struct for integration tests
	IntegrationBase struct {
		suite.Suite

		testCluster            *TestCluster
		testClusterConfig      *TestClusterConfig
		engine                 FrontendClient
		adminClient            AdminClient
		operatorClient         operatorservice.OperatorServiceClient
		Logger                 log.Logger
		namespace              string
		foreignNamespace       string
		archivalNamespace      string
		dynamicConfigOverrides map[dynamicconfig.Key]interface{}
	}
)

func (s *IntegrationBase) setupSuite(defaultClusterConfigFile string) {
	s.setupLogger()

	clusterConfig, err := GetTestClusterConfig(defaultClusterConfigFile)
	s.Require().NoError(err)
	clusterConfig.DynamicConfigOverrides = s.dynamicConfigOverrides
	s.testClusterConfig = clusterConfig

	if clusterConfig.FrontendAddress != "" {
		s.Logger.Info("Running integration test against specified frontend", tag.Address(TestFlags.FrontendAddr))

		connection, err := rpc.Dial(TestFlags.FrontendAddr, nil, s.Logger)
		if err != nil {
			s.Require().NoError(err)
		}

		s.engine = NewFrontendClient(connection)
		s.adminClient = NewAdminClient(connection)
		s.operatorClient = operatorservice.NewOperatorServiceClient(connection)
	} else {
		s.Logger.Info("Running integration test against test cluster")
		cluster, err := NewCluster(clusterConfig, s.Logger)
		s.Require().NoError(err)
		s.testCluster = cluster
		s.engine = s.testCluster.GetFrontendClient()
		s.adminClient = s.testCluster.GetAdminClient()
		s.operatorClient = s.testCluster.GetOperatorClient()
	}

	s.namespace = s.randomizeStr("integration-test-namespace")
	s.Require().NoError(s.registerNamespace(s.namespace, 24*time.Hour, enumspb.ARCHIVAL_STATE_DISABLED, "", enumspb.ARCHIVAL_STATE_DISABLED, ""))

	s.foreignNamespace = s.randomizeStr("integration-foreign-test-namespace")
	s.Require().NoError(s.registerNamespace(s.foreignNamespace, 24*time.Hour, enumspb.ARCHIVAL_STATE_DISABLED, "", enumspb.ARCHIVAL_STATE_DISABLED, ""))

	if clusterConfig.EnableArchival {
		s.archivalNamespace = s.randomizeStr("integration-archival-enabled-namespace")
		s.Require().NoError(s.registerArchivalNamespace(s.archivalNamespace))
	}

	// For tests using SQL visibility, we need to wait for search attributes to be available as part of the ns config
	// TODO: remove after https://github.com/temporalio/temporal/issues/4017 is resolved
	time.Sleep(2 * NamespaceCacheRefreshInterval)
}

// setupLogger sets the Logger for the test suite.
// If the Logger is already set, this method does nothing.
// If the Logger is not set, this method creates a new log.TestLogger which logs to stdout and stderr.
func (s *IntegrationBase) setupLogger() {
	if s.Logger == nil {
		s.Logger = log.NewTestLogger()
	}
}

// GetTestClusterConfig return test cluster config
func GetTestClusterConfig(configFile string) (*TestClusterConfig, error) {
	environment.SetupEnv()

	configLocation := configFile
	if TestFlags.TestClusterConfigFile != "" {
		configLocation = TestFlags.TestClusterConfigFile
	}
	// This is just reading a config so it's less of a security concern
	// #nosec
	confContent, err := os.ReadFile(configLocation)
	if err != nil {
		return nil, fmt.Errorf("failed to read test cluster config file %v: %v", configLocation, err)
	}
	confContent = []byte(os.ExpandEnv(string(confContent)))
	var options TestClusterConfig
	if err := yaml.Unmarshal(confContent, &options); err != nil {
		return nil, fmt.Errorf("failed to decode test cluster config %v", err)
	}

	options.FrontendAddress = TestFlags.FrontendAddr
	return &options, nil
}

func (s *IntegrationBase) tearDownSuite() {
	s.Require().NoError(s.markNamespaceAsDeleted(s.namespace))
	s.Require().NoError(s.markNamespaceAsDeleted(s.foreignNamespace))
	if s.archivalNamespace != "" {
		s.Require().NoError(s.markNamespaceAsDeleted(s.archivalNamespace))
	}

	if s.testCluster != nil {
		s.NoError(s.testCluster.TearDownCluster())
		s.testCluster = nil
	}

	s.engine = nil
	s.adminClient = nil
}

func (s *IntegrationBase) registerNamespace(
	namespace string,
	retention time.Duration,
	historyArchivalState enumspb.ArchivalState,
	historyArchivalURI string,
	visibilityArchivalState enumspb.ArchivalState,
	visibilityArchivalURI string,
) error {
	ctx, cancel := rpc.NewContextWithTimeoutAndVersionHeaders(10000 * time.Second)
	defer cancel()
	_, err := s.engine.RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		Description:                      namespace,
		WorkflowExecutionRetentionPeriod: &retention,
		HistoryArchivalState:             historyArchivalState,
		HistoryArchivalUri:               historyArchivalURI,
		VisibilityArchivalState:          visibilityArchivalState,
		VisibilityArchivalUri:            visibilityArchivalURI,
	})

	if err != nil {
		return err
	}

	// Set up default alias for custom search attributes.
	_, err = s.engine.UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: namespace,
		Config: &namespacepb.NamespaceConfig{
			CustomSearchAttributeAliases: map[string]string{
				"Bool01":     "CustomBoolField",
				"Datetime01": "CustomDatetimeField",
				"Double01":   "CustomDoubleField",
				"Int01":      "CustomIntField",
				"Keyword01":  "CustomKeywordField",
				"Text01":     "CustomTextField",
			},
		},
	})

	return err
}

func (s *IntegrationBase) markNamespaceAsDeleted(
	namespace string,
) error {
	ctx, cancel := rpc.NewContextWithTimeoutAndVersionHeaders(10000 * time.Second)
	defer cancel()
	_, err := s.engine.UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: namespace,
		UpdateInfo: &namespacepb.UpdateNamespaceInfo{
			State: enumspb.NAMESPACE_STATE_DELETED,
		},
	})

	return err
}

func (s *IntegrationBase) randomizeStr(id string) string {
	return fmt.Sprintf("%v-%v", id, uuid.New())
}

func (s *IntegrationBase) printWorkflowHistory(namespace string, execution *commonpb.WorkflowExecution) {
	events := s.getHistory(namespace, execution)
	_, _ = fmt.Println(s.formatHistory(&historypb.History{Events: events}))
}

//lint:ignore U1000 used for debugging.
func (s *IntegrationBase) printWorkflowHistoryCompact(namespace string, execution *commonpb.WorkflowExecution) {
	events := s.getHistory(namespace, execution)
	_, _ = fmt.Println(s.formatHistoryCompact(&historypb.History{Events: events}))
}

func (s *IntegrationBase) getHistory(namespace string, execution *commonpb.WorkflowExecution) []*historypb.HistoryEvent {
	historyResponse, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace:       namespace,
		Execution:       execution,
		MaximumPageSize: 5, // Use small page size to force pagination code path
	})
	s.Require().NoError(err)

	events := historyResponse.History.Events
	for historyResponse.NextPageToken != nil {
		historyResponse, err = s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace:     namespace,
			Execution:     execution,
			NextPageToken: historyResponse.NextPageToken,
		})
		s.Require().NoError(err)
		events = append(events, historyResponse.History.Events...)
	}

	return events
}

func (s *IntegrationBase) getLastEvent(namespace string, execution *commonpb.WorkflowExecution) *historypb.HistoryEvent {
	events := s.getHistory(namespace, execution)
	s.Require().NotEmpty(events)
	return events[len(events)-1]
}

func (s *IntegrationBase) decodePayloadsString(ps *commonpb.Payloads) string {
	s.T().Helper()
	var r string
	s.NoError(payloads.Decode(ps, &r))
	return r
}

func (s *IntegrationBase) decodePayloadsInt(ps *commonpb.Payloads) int {
	s.T().Helper()
	var r int
	s.NoError(payloads.Decode(ps, &r))
	return r
}

func (s *IntegrationBase) decodePayloadsByteSliceInt32(ps *commonpb.Payloads) (r int32) {
	s.T().Helper()
	var buf []byte
	s.NoError(payloads.Decode(ps, &buf))
	s.NoError(binary.Read(bytes.NewReader(buf), binary.LittleEndian, &r))
	return
}

func (s *IntegrationBase) DurationNear(value, target, tolerance time.Duration) {
	s.T().Helper()
	s.Greater(value, target-tolerance)
	s.Less(value, target+tolerance)
}

// To register archival namespace we can't use frontend API as the retention period is set to 0 for testing,
// and request will be rejected by frontend. Here we make a call directly to persistence to register
// the namespace.
func (s *IntegrationBase) registerArchivalNamespace(archivalNamespace string) error {
	currentClusterName := s.testCluster.testBase.ClusterMetadata.GetCurrentClusterName()
	namespaceRequest := &persistence.CreateNamespaceRequest{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:    uuid.New(),
				Name:  archivalNamespace,
				State: enumspb.NAMESPACE_STATE_REGISTERED,
			},
			Config: &persistencespb.NamespaceConfig{
				Retention:               timestamp.DurationFromDays(0),
				HistoryArchivalState:    enumspb.ARCHIVAL_STATE_ENABLED,
				HistoryArchivalUri:      s.testCluster.archiverBase.historyURI,
				VisibilityArchivalState: enumspb.ARCHIVAL_STATE_ENABLED,
				VisibilityArchivalUri:   s.testCluster.archiverBase.visibilityURI,
				BadBinaries:             &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
			},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: currentClusterName,
				Clusters: []string{
					currentClusterName,
				},
			},

			FailoverVersion: common.EmptyVersion,
		},
		IsGlobalNamespace: false,
	}
	response, err := s.testCluster.testBase.MetadataManager.CreateNamespace(context.Background(), namespaceRequest)

	s.Logger.Info("Register namespace succeeded",
		tag.WorkflowNamespace(archivalNamespace),
		tag.WorkflowNamespaceID(response.ID),
	)
	return err
}

func (s *IntegrationBase) formatHistoryCompact(history *historypb.History) string {
	s.T().Helper()
	var sb strings.Builder
	for _, event := range history.Events {
		_, _ = sb.WriteString(fmt.Sprintf("%3d %s\n", event.GetEventId(), event.GetEventType()))
	}
	if sb.Len() > 0 {
		return sb.String()[:sb.Len()-1]
	}
	return ""
}

func (s *IntegrationBase) formatHistory(history *historypb.History) string {
	s.T().Helper()
	var sb strings.Builder
	for _, event := range history.Events {
		eventAttrs := reflect.ValueOf(event.Attributes).Elem().Field(0).Elem().Interface()
		eventAttrsMap := s.structToMap(eventAttrs)
		eventAttrsJson, err := json.Marshal(eventAttrsMap)
		s.NoError(err)
		_, _ = sb.WriteString(fmt.Sprintf("%3d %s %s\n", event.GetEventId(), event.GetEventType(), string(eventAttrsJson)))
	}
	if sb.Len() > 0 {
		return sb.String()[:sb.Len()-1]
	}
	return ""
}

func (s *IntegrationBase) structToMap(strct any) map[string]any {

	strctV := reflect.ValueOf(strct)
	strctT := strctV.Type()

	ret := map[string]any{}

	for i := 0; i < strctV.NumField(); i++ {
		field := strctV.Field(i)
		var fieldData any

		if field.Kind() == reflect.Pointer && field.IsNil() {
			continue
		} else if field.Kind() == reflect.Pointer && field.Elem().Kind() == reflect.Struct {
			fieldData = s.structToMap(field.Elem().Interface())
		} else if field.Kind() == reflect.Struct {
			fieldData = s.structToMap(field.Interface())
		} else {
			fieldData = field.Interface()
		}
		ret[strctT.Field(i).Name] = fieldData
	}

	return ret
}

func (s *IntegrationBase) EqualHistoryEvents(expectedHistory string, actualHistoryEvents []*historypb.HistoryEvent) {
	s.T().Helper()
	s.EqualHistory(expectedHistory, &historypb.History{Events: actualHistoryEvents})
}

func (s *IntegrationBase) EqualHistory(expectedHistory string, actualHistory *historypb.History) {
	s.T().Helper()
	expectedCompactHistory, expectedEventsAttributes := s.parseHistory(expectedHistory)
	actualCompactHistory := s.formatHistoryCompact(actualHistory)
	s.Equal(expectedCompactHistory, actualCompactHistory)
	for _, actualHistoryEvent := range actualHistory.Events {
		if expectedEventAttributes, ok := expectedEventsAttributes[actualHistoryEvent.EventId]; ok {
			actualEventAttributes := reflect.ValueOf(actualHistoryEvent.Attributes).Elem().Field(0).Elem()
			s.equalStructToMap(expectedEventAttributes, actualEventAttributes, actualHistoryEvent.EventId, "")
		}
	}
}

func (s *IntegrationBase) equalStructToMap(expectedMap map[string]any, actualStructV reflect.Value, eventID int64, attrPrefix string) {
	s.T().Helper()

	for attrName, expectedValue := range expectedMap {
		actualFieldV := actualStructV.FieldByName(attrName)
		if actualFieldV.Kind() == reflect.Invalid {
			s.Failf("", "Expected property %s%s wasn't found for EventID=%v", attrPrefix, attrName, eventID)
		}

		if ep, ok := expectedValue.(map[string]any); ok {
			if actualFieldV.IsNil() {
				s.Failf("", "Value of property %s%s for EventID=%v expected to be struct but was nil", attrPrefix, attrName, eventID)
			}
			if actualFieldV.Kind() == reflect.Pointer {
				actualFieldV = actualFieldV.Elem()
			}
			if actualFieldV.Kind() != reflect.Struct {
				s.Failf("", "Value of property %s%s for EventID=%v expected to be struct but was of type %s", attrPrefix, attrName, eventID, actualFieldV.Type().String())
			}
			s.equalStructToMap(ep, actualFieldV, eventID, attrPrefix+attrName+".")
			continue
		}
		actualFieldValue := actualFieldV.Interface()
		s.EqualValues(expectedValue, actualFieldValue, "Values of %s%s property are not equal for EventID=%v", attrPrefix, attrName, eventID)
	}
}

// parseHistory accept history in a formatHistory format and returns compact history string and map of eventID to map of event attributes.
func (s *IntegrationBase) parseHistory(expectedHistory string) (string, map[int64]map[string]any) {
	s.T().Helper()
	h := &historypb.History{}
	eventsAttrs := make(map[int64]map[string]any)
	prevEventID := 0
	for lineNum, eventLine := range strings.Split(expectedHistory, "\n") {
		fields := strings.Fields(eventLine)
		if len(fields) == 0 {
			continue
		}
		if len(fields) < 2 {
			s.FailNowf("", "Not enough fields on line %d", lineNum+1)
		}
		eventID, err := strconv.Atoi(fields[0])
		if err != nil {
			s.FailNowf(err.Error(), "Failed to parse EventID on line %d", lineNum+1)
		}
		if eventID != prevEventID+1 && prevEventID != 0 {
			s.FailNowf("", "Wrong EventID sequence after EventID %d on line %d", prevEventID, lineNum+1)
		}
		prevEventID = eventID
		eventType, ok := enumspb.EventType_value[fields[1]]
		if !ok {
			s.FailNowf("", "Unknown event type %s for EventID=%d", fields[1], lineNum+1)
		}
		h.Events = append(h.Events, &historypb.HistoryEvent{
			EventId:   int64(eventID),
			EventType: enumspb.EventType(eventType),
		})
		var jb strings.Builder
		for i := 2; i < len(fields); i++ {
			if strings.HasPrefix(fields[i], "//") {
				break
			}
			_, _ = jb.WriteString(fields[i])
		}
		if jb.Len() > 0 {
			var eventAttrs map[string]any
			err := json.Unmarshal([]byte(jb.String()), &eventAttrs)
			if err != nil {
				s.FailNowf(err.Error(), "Failed to unmarshal attributes %q for EventID=%d", jb.String(), lineNum+1)
			}
			eventsAttrs[int64(eventID)] = eventAttrs
		}
	}
	return s.formatHistoryCompact(h), eventsAttrs
}
