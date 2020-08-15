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

package cli

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/gocql/gocql"
	"github.com/urfave/cli"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"gopkg.in/yaml.v2"

	"go.temporal.io/server/api/adminservice/v1"
	indexerspb "go.temporal.io/server/api/indexer/v1"
	"go.temporal.io/server/api/persistenceblobs/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/codec"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log/loggerimpl"
	"go.temporal.io/server/common/messaging"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/cassandra"
	"go.temporal.io/server/common/service/dynamicconfig"
	"go.temporal.io/server/service/history"
)

type filterFn func(*replicationspb.ReplicationTask) bool
type filterFnForVisibility func(*indexerspb.Message) bool

type kafkaMessageType int

const (
	kafkaMessageTypeReplicationTask kafkaMessageType = iota
	kafkaMessageTypeVisibilityMsg
)

const (
	bufferSize                       = 8192
	preambleVersion0            byte = 0x59
	malformedMessage                 = "Input was malformed"
	chanBufferSize                   = 10000
	maxRereplicateEventID            = 999999
	defaultResendContextTimeout      = 30 * time.Second
)

var (
	r = regexp.MustCompile(`Partition: .*?, Offset: .*?, Key: .*?`)
)

type writerChannel struct {
	Type                   kafkaMessageType
	ReplicationTaskChannel chan *replicationspb.ReplicationTask
	VisibilityMsgChannel   chan *indexerspb.Message
}

func newWriterChannel(messageType kafkaMessageType) *writerChannel {
	ch := &writerChannel{
		Type: messageType,
	}
	switch messageType {
	case kafkaMessageTypeReplicationTask:
		ch.ReplicationTaskChannel = make(chan *replicationspb.ReplicationTask, chanBufferSize)
	case kafkaMessageTypeVisibilityMsg:
		ch.VisibilityMsgChannel = make(chan *indexerspb.Message, chanBufferSize)
	}
	return ch
}

func (ch *writerChannel) Close() {
	if ch.ReplicationTaskChannel != nil {
		close(ch.ReplicationTaskChannel)
	}
	if ch.VisibilityMsgChannel != nil {
		close(ch.VisibilityMsgChannel)
	}
}

// AdminKafkaParse parses the output of k8read and outputs replication tasks
func AdminKafkaParse(c *cli.Context) {
	inputFile := getInputFile(c.String(FlagInputFile))
	outputFile := getOutputFile(c.String(FlagOutputFilename))

	defer inputFile.Close()
	defer outputFile.Close()

	readerCh := make(chan []byte, chanBufferSize)
	writerCh := newWriterChannel(kafkaMessageType(c.Int(FlagMessageType)))
	doneCh := make(chan struct{})

	var skippedCount int32
	skipErrMode := c.Bool(FlagSkipErrorMode)

	go startReader(inputFile, readerCh)
	go startParser(readerCh, writerCh, skipErrMode, &skippedCount)
	go startWriter(outputFile, writerCh, doneCh, &skippedCount, c)

	<-doneCh

	if skipErrMode {
		fmt.Printf("%v messages were skipped due to errors in parsing", atomic.LoadInt32(&skippedCount))
	}
}

func buildFilterFn(workflowID, runID string) filterFn {
	return func(task *replicationspb.ReplicationTask) bool {
		if len(workflowID) != 0 || len(runID) != 0 {
			if task.GetHistoryTaskAttributes() == nil {
				return false
			}
		}
		if len(workflowID) != 0 && task.GetHistoryTaskAttributes().GetWorkflowId() != workflowID {
			return false
		}
		if len(runID) != 0 && task.GetHistoryTaskAttributes().GetRunId() != runID {
			return false
		}
		return true
	}
}

func buildFilterFnForVisibility(workflowID, runID string) filterFnForVisibility {
	return func(msg *indexerspb.Message) bool {
		if len(workflowID) != 0 && msg.GetWorkflowId() != workflowID {
			return false
		}
		if len(runID) != 0 && msg.GetRunId() != runID {
			return false
		}
		return true
	}
}

func getInputFile(inputFile string) *os.File {
	if len(inputFile) == 0 {
		info, err := os.Stdin.Stat()
		if err != nil {
			ErrorAndExit("Failed to stat stdin file handle", err)
		}
		if info.Mode()&os.ModeCharDevice != 0 || info.Size() <= 0 {
			fmt.Println("Misuse of pipe mode")
			os.Exit(1)
		}
		return os.Stdin
	}
	// This code is executed from the CLI. All user input is from a CLI user.
	// #nosec
	f, err := os.Open(inputFile)
	if err != nil {
		ErrorAndExit(fmt.Sprintf("Failed to open input file for reading: %v", inputFile), err)
	}
	return f
}

func getOutputFile(outputFile string) *os.File {
	if len(outputFile) == 0 {
		return os.Stdout
	}
	f, err := os.Create(outputFile)
	if err != nil {
		ErrorAndExit("failed to create output file", err)
	}
	return f
}

func startReader(file *os.File, readerCh chan<- []byte) {
	defer close(readerCh)
	reader := bufio.NewReader(file)

	for {
		buf := make([]byte, bufferSize)
		n, err := reader.Read(buf)
		if err != nil {
			if err != io.EOF {
				ErrorAndExit("Failed to read from reader", err)
			} else {
				break
			}

		}
		buf = buf[:n]
		readerCh <- buf
	}
}

func startParser(readerCh <-chan []byte, writerCh *writerChannel, skipErrors bool, skippedCount *int32) {
	defer writerCh.Close()

	var buffer []byte
Loop:
	for {
		select {
		case data, ok := <-readerCh:
			if !ok {
				break Loop
			}
			buffer = append(buffer, data...)
			data, nextBuffer := splitBuffer(buffer)
			buffer = nextBuffer
			parse(data, skipErrors, skippedCount, writerCh)
		}
	}
	parse(buffer, skipErrors, skippedCount, writerCh)
}

func startWriter(
	outputFile *os.File,
	writerCh *writerChannel,
	doneCh chan struct{},
	skippedCount *int32,
	c *cli.Context,
) {

	defer close(doneCh)

	skipErrMode := c.Bool(FlagSkipErrorMode)
	headerMode := c.Bool(FlagHeadersMode)

	switch writerCh.Type {
	case kafkaMessageTypeReplicationTask:
		writeReplicationTask(outputFile, writerCh, skippedCount, skipErrMode, headerMode, c)
	case kafkaMessageTypeVisibilityMsg:
		writeVisibilityMessage(outputFile, writerCh, skippedCount, skipErrMode, headerMode, c)
	}
}

func writeReplicationTask(
	outputFile *os.File,
	writerCh *writerChannel,
	skippedCount *int32,
	skipErrMode bool,
	headerMode bool,
	c *cli.Context,
) {
	filter := buildFilterFn(c.String(FlagWorkflowID), c.String(FlagRunID))
	encoder := codec.NewJSONPBEncoder()
Loop:
	for {
		select {
		case task, ok := <-writerCh.ReplicationTaskChannel:
			if !ok {
				break Loop
			}
			if filter(task) {
				jsonStr, err := encoder.Encode(task)
				if err != nil {
					if !skipErrMode {
						ErrorAndExit(malformedMessage, fmt.Errorf("failed to encode into json, err: %v", err))
					} else {
						atomic.AddInt32(skippedCount, 1)
						continue Loop
					}
				}

				var outStr string
				if !headerMode {
					outStr = string(jsonStr)
				} else {
					outStr = fmt.Sprintf(
						"%v, %v, %v, %v, %v",
						task.GetHistoryTaskAttributes().GetNamespaceId(),
						task.GetHistoryTaskAttributes().GetWorkflowId(),
						task.GetHistoryTaskAttributes().GetRunId(),
						task.GetHistoryTaskAttributes().GetFirstEventId(),
						task.GetHistoryTaskAttributes().GetNextEventId(),
					)
				}
				_, err = outputFile.WriteString(fmt.Sprintf("%v\n", outStr))
				if err != nil {
					ErrorAndExit("Failed to write to file", fmt.Errorf("err: %v", err))
				}
			}
		}
	}
}

func writeVisibilityMessage(
	outputFile *os.File,
	writerCh *writerChannel,
	skippedCount *int32,
	skipErrMode bool,
	headerMode bool,
	c *cli.Context,
) {
	filter := buildFilterFnForVisibility(c.String(FlagWorkflowID), c.String(FlagRunID))
	encoder := codec.NewJSONPBEncoder()
Loop:
	for {
		select {
		case msg, ok := <-writerCh.VisibilityMsgChannel:
			if !ok {
				break Loop
			}
			if filter(msg) {
				jsonStr, err := encoder.Encode(msg)
				if err != nil {
					if !skipErrMode {
						ErrorAndExit(malformedMessage, fmt.Errorf("failed to encode into json, err: %v", err))
					} else {
						atomic.AddInt32(skippedCount, 1)
						continue Loop
					}
				}

				var outStr string
				if !headerMode {
					outStr = string(jsonStr)
				} else {
					outStr = fmt.Sprintf(
						"%v, %v, %v, %v, %v",
						msg.GetNamespaceId(),
						msg.GetWorkflowId(),
						msg.GetRunId(),
						msg.GetMessageType().String(),
						msg.GetVersion(),
					)
				}
				_, err = outputFile.WriteString(fmt.Sprintf("%v\n", outStr))
				if err != nil {
					ErrorAndExit("Failed to write to file", fmt.Errorf("err: %v", err))
				}
			}
		}
	}
}

func splitBuffer(buffer []byte) ([]byte, []byte) {
	matches := r.FindAllIndex(buffer, -1)
	if len(matches) == 0 {
		ErrorAndExit(malformedMessage, errors.New("header not found, did you generate dump with -v"))
	}
	splitIndex := matches[len(matches)-1][0]
	return buffer[:splitIndex], buffer[splitIndex:]
}

func parse(bytes []byte, skipErrors bool, skippedCount *int32, writerCh *writerChannel) {
	messages, skippedGetMsgCount := getMessages(bytes, skipErrors)
	switch writerCh.Type {
	case kafkaMessageTypeReplicationTask:
		msgs, skippedDeserializeCount := deserializeMessages(messages, skipErrors)
		atomic.AddInt32(skippedCount, skippedGetMsgCount+skippedDeserializeCount)
		for _, msg := range msgs {
			writerCh.ReplicationTaskChannel <- msg
		}
	case kafkaMessageTypeVisibilityMsg:
		msgs, skippedDeserializeCount := deserializeVisibilityMessages(messages, skipErrors)
		atomic.AddInt32(skippedCount, skippedGetMsgCount+skippedDeserializeCount)
		for _, msg := range msgs {
			writerCh.VisibilityMsgChannel <- msg
		}
	}
}

func getMessages(data []byte, skipErrors bool) ([][]byte, int32) {
	str := string(data)
	messagesWithHeaders := r.Split(str, -1)
	if len(messagesWithHeaders[0]) != 0 {
		ErrorAndExit(malformedMessage, errors.New("got data chunk to handle that does not start with valid header"))
	}
	messagesWithHeaders = messagesWithHeaders[1:]
	var rawMessages [][]byte
	var skipped int32
	for _, m := range messagesWithHeaders {
		if len(m) == 0 {
			ErrorAndExit(malformedMessage, errors.New("got empty message between valid headers"))
		}
		curr := []byte(m)
		messageStart := bytes.Index(curr, []byte{preambleVersion0})
		if messageStart == -1 {
			if !skipErrors {
				ErrorAndExit(malformedMessage, errors.New("failed to find message preamble"))
			} else {
				skipped++
				continue
			}
		}
		rawMessages = append(rawMessages, curr[messageStart:])
	}
	return rawMessages, skipped
}

func deserializeMessages(messages [][]byte, skipErrors bool) ([]*replicationspb.ReplicationTask, int32) {
	var replicationTasks []*replicationspb.ReplicationTask
	var skipped int32
	for _, m := range messages {
		var task replicationspb.ReplicationTask
		err := decode(m, &task)
		if err != nil {
			if !skipErrors {
				ErrorAndExit(malformedMessage, err)
			} else {
				skipped++
				continue
			}
		}
		replicationTasks = append(replicationTasks, &task)
	}
	return replicationTasks, skipped
}

func decode(message []byte, val *replicationspb.ReplicationTask) error {
	// TODO (shtin): Current proto implementation is most likely broken. It used to be:
	// reader := bytes.NewReader(message[1:])
	// wireVal, err := protocol.Binary.Decode(reader, wire.TStruct)
	// if err != nil {
	// 	return err
	// }
	// return val.FromWire(wireVal)

	return val.Unmarshal(message)
}

func deserializeVisibilityMessages(messages [][]byte, skipErrors bool) ([]*indexerspb.Message, int32) {
	var visibilityMessages []*indexerspb.Message
	var skipped int32
	for _, m := range messages {
		var msg indexerspb.Message
		err := decodeVisibility(m, &msg)
		if err != nil {
			if !skipErrors {
				ErrorAndExit(malformedMessage, err)
			} else {
				skipped++
				continue
			}
		}
		visibilityMessages = append(visibilityMessages, &msg)
	}
	return visibilityMessages, skipped
}

func decodeVisibility(message []byte, val *indexerspb.Message) error {
	// TODO (shtin): Current proto implementation is most likely broken. It used to be:
	// reader := bytes.NewReader(message[1:])
	// wireVal, err := protocol.Binary.Decode(reader, wire.TStruct)
	// if err != nil {
	// 	return err
	// }
	// return val.FromWire(wireVal)
	return val.Unmarshal(message)
}

// ClustersConfig describes the kafka clusters
type ClustersConfig struct {
	Clusters map[string]messaging.ClusterConfig
	TLS      auth.TLS
}

func doRereplicate(
	ctx context.Context,
	shardID int,
	namespaceID string,
	wid string,
	rid string,
	minID int64,
	maxID int64,
	startVersion int64,
	targets []string,
	producer messaging.Producer,
	session *gocql.Session,
	adminClient adminservice.AdminServiceClient,
) {

	if minID <= 0 {
		minID = 1
	}
	if maxID == 0 {
		maxID = maxRereplicateEventID
	}

	histV2 := cassandra.NewHistoryV2PersistenceFromSession(session, loggerimpl.NewNopLogger())
	historyV2Mgr := persistence.NewHistoryV2ManagerImpl(histV2, loggerimpl.NewNopLogger(), dynamicconfig.GetIntPropertyFn(common.DefaultTransactionSizeLimit))

	exeM, _ := cassandra.NewWorkflowExecutionPersistence(shardID, session, loggerimpl.NewNopLogger())
	exeMgr := persistence.NewExecutionManagerImpl(exeM, loggerimpl.NewNopLogger())

	for {
		fmt.Printf("Start rereplicate for wid: %v, rid:%v \n", wid, rid)
		resp, err := exeMgr.GetWorkflowExecution(&persistence.GetWorkflowExecutionRequest{
			NamespaceID: namespaceID,
			Execution: commonpb.WorkflowExecution{
				WorkflowId: wid,
				RunId:      rid,
			},
		})
		if err != nil {
			ErrorAndExit("GetWorkflowExecution error", err)
		}

		versionHistories := resp.State.VersionHistories
		if versionHistories != nil {
			if startVersion == common.EmptyVersion {
				ErrorAndExit("Use input file to resend NDC workflow is not support", nil)
			}
			if _, err := adminClient.ResendReplicationTasks(
				ctx,
				&adminservice.ResendReplicationTasksRequest{
					NamespaceId:   namespaceID,
					WorkflowId:    wid,
					RunId:         rid,
					RemoteCluster: targets[0],
					StartVersion:  startVersion,
				},
			); err != nil {
				ErrorAndExit("Failed to resend ndc workflow", err)
			}
			return
		}

		currVersion := resp.State.ReplicationState.CurrentVersion
		repInfo := map[string]*replicationspb.ReplicationInfo{
			"": {
				Version:     currVersion,
				LastEventId: 0,
			},
		}

		exeInfo := resp.State.ExecutionInfo
		taskTemplate := &persistenceblobs.ReplicationTaskInfo{
			NamespaceId:         namespaceID,
			WorkflowId:          wid,
			RunId:               rid,
			Version:             currVersion,
			LastReplicationInfo: repInfo,
			BranchToken:         exeInfo.BranchToken,
		}

		_, historyBatches, err := history.GetAllHistory(historyV2Mgr, nil, true,
			minID, maxID, exeInfo.BranchToken, convert.IntPtr(shardID))

		if err != nil {
			ErrorAndExit("GetAllHistory error", err)
		}

		continueAsNew := false
		var newRunID string
		for _, batch := range historyBatches {

			events := batch.Events
			firstEvent := events[0]
			lastEvent := events[len(events)-1]
			if lastEvent.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW {
				continueAsNew = true
				newRunID = lastEvent.GetWorkflowExecutionContinuedAsNewEventAttributes().GetNewExecutionRunId()
				resp, err := exeMgr.GetWorkflowExecution(&persistence.GetWorkflowExecutionRequest{
					NamespaceID: namespaceID,
					Execution: commonpb.WorkflowExecution{
						WorkflowId: wid,
						RunId:      newRunID,
					},
				})
				if err != nil {
					ErrorAndExit("GetWorkflowExecution error", err)
				}
				taskTemplate.NewRunBranchToken = resp.State.ExecutionInfo.BranchToken
			}
			taskTemplate.Version = firstEvent.GetVersion()
			taskTemplate.FirstEventId = firstEvent.GetEventId()
			taskTemplate.NextEventId = lastEvent.GetEventId() + 1
			task, _, err := history.GenerateReplicationTask(targets, taskTemplate, historyV2Mgr, nil, batch, convert.IntPtr(shardID))
			if err != nil {
				ErrorAndExit("GenerateReplicationTask error", err)
			}
			err = producer.Publish(task)
			if err != nil {
				ErrorAndExit("Publish task error", err)
			}
			fmt.Printf("publish task successfully firstEventId %v, lastEventId %v \n", firstEvent.GetEventId(), lastEvent.GetEventId())
		}

		fmt.Printf("Done rereplicate for wid: %v, rid:%v \n", wid, rid)
		runtime.GC()
		if continueAsNew {
			rid = newRunID
			minID = 1
			maxID = maxRereplicateEventID
		} else {
			break
		}
	}
}

// AdminRereplicate parses will re-publish replication tasks to topic
func AdminRereplicate(c *cli.Context) {
	numberOfShards := c.Int(FlagNumberOfShards)
	if numberOfShards <= 0 {
		ErrorAndExit("numberOfShards is must be > 0", nil)
		return
	}
	target := getRequiredOption(c, FlagTargetCluster)
	targets := []string{target}

	session := connectToCassandra(c)
	adminClient := cFactory.AdminClient(c)
	var startVersion int64
	var producer messaging.Producer
	if c.IsSet(FlagStartEventVersion) {
		startVersion = c.Int64(FlagStartEventVersion)
	} else {
		startVersion = common.EmptyVersion
		producer = newKafkaProducer(c)
	}

	contextTimeout := defaultResendContextTimeout
	if c.GlobalIsSet(FlagContextTimeout) {
		contextTimeout = time.Duration(c.GlobalInt(FlagContextTimeout)) * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
	defer cancel()

	if c.IsSet(FlagInputFile) {
		inFile := c.String(FlagInputFile)
		// This code is executed from the CLI. All user input is from a CLI user.
		// parse namespaceID,workflowID,runID,minEventID,maxEventID
		// #nosec
		file, err := os.Open(inFile)
		if err != nil {
			ErrorAndExit("Open failed", err)
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		idx := 0
		for scanner.Scan() {
			idx++
			line := strings.TrimSpace(scanner.Text())
			if len(line) == 0 {
				fmt.Printf("line %v is empty, skipped\n", idx)
				continue
			}
			cols := strings.Split(line, ",")
			if len(cols) < 3 {
				ErrorAndExit("Split failed", fmt.Errorf("line %v has less than 3 cols separated by comma, only %v ", idx, len(cols)))
			}
			fmt.Printf("Start processing line %v ...\n", idx)
			namespaceID := strings.TrimSpace(cols[0])
			wid := strings.TrimSpace(cols[1])
			rid := strings.TrimSpace(cols[2])
			var minID, maxID int64
			if len(cols) >= 4 {
				i, err := strconv.Atoi(strings.TrimSpace(cols[3]))
				if err != nil {
					ErrorAndExit(fmt.Sprintf("Atoi failed at lne %v", idx), err)
				}
				minID = int64(i)
			}
			if len(cols) >= 5 {
				i, err := strconv.Atoi(strings.TrimSpace(cols[4]))
				if err != nil {
					ErrorAndExit(fmt.Sprintf("Atoi failed at lne %v", idx), err)
				}
				maxID = int64(i)
			}

			shardID := common.WorkflowIDToHistoryShard(namespaceID, wid, numberOfShards)
			doRereplicate(ctx, shardID, namespaceID, wid, rid, minID, maxID, startVersion, targets, producer, session, adminClient)
			fmt.Printf("Done processing line %v ...\n", idx)
		}
		if err := scanner.Err(); err != nil {
			ErrorAndExit("scanner failed", err)
		}
	} else {
		namespaceID := getRequiredOption(c, FlagNamespaceID)
		wid := getRequiredOption(c, FlagWorkflowID)
		rid := getRequiredOption(c, FlagRunID)
		minID := c.Int64(FlagMinEventID)
		maxID := c.Int64(FlagMaxEventID)
		shardID := common.WorkflowIDToHistoryShard(namespaceID, wid, numberOfShards)

		doRereplicate(ctx, shardID, namespaceID, wid, rid, minID, maxID, startVersion, targets, producer, session, adminClient)
	}
}

func newKafkaProducer(c *cli.Context) messaging.Producer {
	hostFile := getRequiredOption(c, FlagHostFile)
	destCluster := getRequiredOption(c, FlagCluster)
	destTopic := getRequiredOption(c, FlagTopic)

	// initialize kafka producer
	destBrokers, tlsConfig, err := loadBrokerConfig(hostFile, destCluster)
	if err != nil {
		ErrorAndExit("", err)
	}

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	if tlsConfig != nil {
		config.Net.TLS.Config = tlsConfig
		config.Net.TLS.Enable = true
	}
	sproducer, err := sarama.NewSyncProducer(destBrokers, config)
	if err != nil {
		ErrorAndExit("", err)
	}
	logger := loggerimpl.NewNopLogger()

	producer := messaging.NewKafkaProducer(destTopic, sproducer, logger)
	return producer
}

// AdminPurgeTopic is used to purge kafka topic
func AdminPurgeTopic(c *cli.Context) {
	hostFile := getRequiredOption(c, FlagHostFile)
	topic := getRequiredOption(c, FlagTopic)
	cluster := getRequiredOption(c, FlagCluster)
	group := getRequiredOption(c, FlagGroup)
	brokers, tlsConfig, err := loadBrokerConfig(hostFile, cluster)

	consumer := createConsumerAndWaitForReady(brokers, tlsConfig, group, topic)

	highWaterMarks, ok := consumer.HighWaterMarks()[topic]
	if !ok {
		ErrorAndExit("", fmt.Errorf("cannot find high watermark"))
	}
	fmt.Printf("Topic high watermark %v.\n", highWaterMarks)
	for partition, hi := range highWaterMarks {
		consumer.MarkPartitionOffset(topic, partition, hi-1, "")
		fmt.Printf("set partition offset %v:%v \n", partition, hi)
	}
	err = consumer.CommitOffsets()
	if err != nil {
		ErrorAndExit("fail to commit offset", err)
	}

	consumer = createConsumerAndWaitForReady(brokers, tlsConfig, group, topic)
	msg, ok := <-consumer.Messages()
	if !ok {
		fmt.Println("consumer channel is closed")
	}
	fmt.Printf("current offset sample: %v: %v \n", msg.Partition, msg.Offset)
}

// AdminMergeDLQ publish replication tasks from DLQ or JSON file
func AdminMergeDLQ(c *cli.Context) {
	hostFile := getRequiredOption(c, FlagHostFile)
	producer := newKafkaProducer(c)

	var err error
	var inFile string
	var tasks []*replicationspb.ReplicationTask
	if c.IsSet(FlagInputFile) && (c.IsSet(FlagInputCluster) || c.IsSet(FlagInputTopic) || c.IsSet(FlagStartOffset)) {
		ErrorAndExit("", fmt.Errorf("ONLY Either from JSON file or from DLQ topic"))
	}

	if c.IsSet(FlagInputFile) {
		inFile = c.String(FlagInputFile)
		// parse json input as replicaiton tasks
		tasks, err = parseReplicationTask(inFile)
		if err != nil {
			ErrorAndExit("", err)
		}
		// publish to topic
		for idx, t := range tasks {
			err := producer.Publish(t)
			if err != nil {
				fmt.Printf("cannot publish task %v to topic \n", idx)
				ErrorAndExit("", err)
			} else {
				fmt.Printf("replication task sent: %v firstId %v, nextId %v \n", idx, t.GetHistoryTaskAttributes().GetFirstEventId(), t.GetHistoryTaskAttributes().GetNextEventId())
			}
		}
	} else {
		fromTopic := getRequiredOption(c, FlagInputTopic)
		fromCluster := getRequiredOption(c, FlagInputCluster)
		startOffset := c.Int64(FlagStartOffset)
		group := getRequiredOption(c, FlagGroup)

		fromBrokers, tlsConfig, err := loadBrokerConfig(hostFile, fromCluster)
		if err != nil {
			ErrorAndExit("", err)
		}

		consumer := createConsumerAndWaitForReady(fromBrokers, tlsConfig, group, fromTopic)

		highWaterMarks, ok := consumer.HighWaterMarks()[fromTopic]
		if !ok {
			ErrorAndExit("", fmt.Errorf("cannot find high watermark"))
		}
		fmt.Printf("Topic high watermark %v.\n", highWaterMarks)
		for partition := range highWaterMarks {
			consumer.MarkPartitionOffset(fromTopic, partition, startOffset, "")
			fmt.Printf("reset offset %v:%v \n", partition, startOffset)
		}
		err = consumer.CommitOffsets()
		if err != nil {
			ErrorAndExit("fail to commit offset", err)
		}
		// create consumer again to make sure MarkPartitionOffset works
		consumer = createConsumerAndWaitForReady(fromBrokers, tlsConfig, group, fromTopic)

		for {
			select {
			case msg, ok := <-consumer.Messages():
				if !ok {
					return
				}
				if msg.Offset < startOffset {
					fmt.Printf("Wrong Message [%v],[%v] \n", msg.Partition, msg.Offset)
					ErrorAndExit("", fmt.Errorf("offset is not correct"))
					continue
				} else {
					var task replicationspb.ReplicationTask
					err := decode(msg.Value, &task)
					if err != nil {
						ErrorAndExit("failed to deserialize message due to error", err)
					}

					err = producer.Publish(&task)

					if err != nil {
						fmt.Printf("[Error] Message [%v],[%v] failed: %v\n", msg.Partition, msg.Offset, err)
					} else {
						fmt.Printf("Message [%v],[%v] succeeded\n", msg.Partition, msg.Offset)
					}
				}
				consumer.MarkOffset(msg, "")
			case <-time.After(time.Second * 5):
				fmt.Println("heartbeat: waiting for more messages, Ctrl+C to stop any time...")
			}
		}
	}
}

// AdminListDLQ outputs a list of a tasks for given Shard and Task Type
func AdminListDLQ(c *cli.Context) {
	cluster := getRequiredOption(c, FlagCluster)
	sid := getRequiredIntOption(c, FlagShardID)

	pFactory := CreatePersistenceFactory(c)
	executionManager, err := pFactory.NewExecutionManager(sid)
	if err != nil {
		ErrorAndExit("Failed to initialize execution manager", err)
	}

	taskReq := persistence.GetReplicationTasksRequest{}
	req := &persistence.GetReplicationTasksFromDLQRequest{SourceClusterName: cluster, GetReplicationTasksRequest: taskReq}
	paginationFunc := func(paginationToken []byte) ([]interface{}, []byte, error) {
		req.GetReplicationTasksRequest.NextPageToken = paginationToken
		response, err := executionManager.GetReplicationTasksFromDLQ(req)
		if err != nil {
			return nil, nil, err
		}
		token := response.NextPageToken

		var items []interface{}
		for _, task := range response.Tasks {
			items = append(items, task)
		}
		return items, token, nil
	}
	paginate(c, paginationFunc)
}

func createConsumerAndWaitForReady(brokers []string, tlsConfig *tls.Config, group, fromTopic string) *cluster.Consumer {
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	if tlsConfig != nil {
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = tlsConfig
	}

	config.Group.Return.Notifications = true

	client, err := cluster.NewClient(brokers, config)
	if err != nil {
		ErrorAndExit("", err)
	}

	consumer, err := cluster.NewConsumerFromClient(client, group, []string{fromTopic})
	if err != nil {
		ErrorAndExit("", err)
	}

	for ntf := range consumer.Notifications() {
		time.Sleep(time.Second)
		if partitions := ntf.Current[fromTopic]; len(partitions) > 0 && ntf.Type == cluster.RebalanceOK {
			break
		}
		fmt.Println("Waiting for consumer ready...")
	}
	return consumer
}

func parseReplicationTask(in string) (tasks []*replicationspb.ReplicationTask, err error) {
	// This code is executed from the CLI. All user input is from a CLI user.
	// #nosec
	file, err := os.Open(in)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewReaderSize(file, 1024*1024*3)
	idx := 0
	encoder := codec.NewJSONPBEncoder()
	for {
		idx++
		line, _, err := reader.ReadLine()
		if err != nil {
			if err != io.EOF {
				ErrorAndExit("Failed to read line", err)
			}
			break
		}

		if len(line) == 0 {
			fmt.Printf("line %v is empty, skipped\n", idx)
			continue
		}

		t := &replicationspb.ReplicationTask{}
		err = encoder.Decode(line, t)
		if err != nil {
			fmt.Printf("line %v cannot be deserialized to replicaiton task: %v.\n", idx, string(line))
			return nil, err
		}
		tasks = append(tasks, t)
	}

	return tasks, nil
}

func loadBrokerConfig(hostFile string, cluster string) ([]string, *tls.Config, error) {
	// This code is executed from the CLI and is only used to load config files
	// #nosec
	contents, err := ioutil.ReadFile(hostFile)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load kafka cluster info from %v., error: %v", hostFile, err)
	}
	clustersConfig := ClustersConfig{}
	if err := yaml.Unmarshal(contents, &clustersConfig); err != nil {
		return nil, nil, err
	}
	if len(clustersConfig.Clusters) != 0 {
		config, ok := clustersConfig.Clusters[cluster]
		if ok {
			brs := config.Brokers
			for i, b := range brs {
				if !strings.Contains(b, ":") {
					b += ":9092"
					brs[i] = b
				}
			}
			tlsConfig, err := messaging.CreateTLSConfig(clustersConfig.TLS)
			if err != nil {
				return nil, nil, fmt.Errorf(fmt.Sprintf("Error creating Kafka TLS config %v", err))
			}
			return brs, tlsConfig, nil
		}
	}
	return nil, nil, fmt.Errorf("failed to load broker for cluster %v", cluster)
}
