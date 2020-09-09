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

package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
	"github.com/urfave/cli"
	"go.uber.org/thriftrw/protocol"
	"go.uber.org/thriftrw/wire"

	"github.com/uber/cadence/.gen/go/admin"
	serverAdmin "github.com/uber/cadence/.gen/go/admin/adminserviceclient"
	"github.com/uber/cadence/.gen/go/indexer"
	"github.com/uber/cadence/.gen/go/replicator"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/auth"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/cassandra"
)

type (
	filterFn              func(*replicator.ReplicationTask) bool
	filterFnForVisibility func(*indexer.Message) bool

	kafkaMessageType int

	historyV2Task struct {
		Task         *replicator.ReplicationTask
		Events       []*shared.HistoryEvent
		NewRunEvents []*shared.HistoryEvent
	}
)

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
	ReplicationTaskChannel chan *replicator.ReplicationTask
	VisibilityMsgChannel   chan *indexer.Message
}

func newWriterChannel(messageType kafkaMessageType) *writerChannel {
	ch := &writerChannel{
		Type: messageType,
	}
	switch messageType {
	case kafkaMessageTypeReplicationTask:
		ch.ReplicationTaskChannel = make(chan *replicator.ReplicationTask, chanBufferSize)
	case kafkaMessageTypeVisibilityMsg:
		ch.VisibilityMsgChannel = make(chan *indexer.Message, chanBufferSize)
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
	serializer := persistence.NewPayloadSerializer()

	var skippedCount int32
	skipErrMode := c.Bool(FlagSkipErrorMode)

	go startReader(inputFile, readerCh)
	go startParser(readerCh, writerCh, skipErrMode, &skippedCount)
	go startWriter(outputFile, writerCh, doneCh, &skippedCount, serializer, c)

	<-doneCh

	if skipErrMode {
		fmt.Printf("%v messages were skipped due to errors in parsing", atomic.LoadInt32(&skippedCount))
	}
}

func buildFilterFn(workflowID, runID string) filterFn {
	return func(task *replicator.ReplicationTask) bool {
		if len(workflowID) != 0 || len(runID) != 0 {
			if task.GetHistoryTaskAttributes() == nil {
				return false
			}
		}
		if len(workflowID) != 0 && *task.HistoryTaskAttributes.WorkflowId != workflowID {
			return false
		}
		if len(runID) != 0 && *task.HistoryTaskAttributes.RunId != runID {
			return false
		}
		return true
	}
}

func buildFilterFnForVisibility(workflowID, runID string) filterFnForVisibility {
	return func(msg *indexer.Message) bool {
		if len(workflowID) != 0 && msg.GetWorkflowID() != workflowID {
			return false
		}
		if len(runID) != 0 && msg.GetRunID() != runID {
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
	serializer persistence.PayloadSerializer,
	c *cli.Context,
) {

	defer close(doneCh)

	skipErrMode := c.Bool(FlagSkipErrorMode)
	headerMode := c.Bool(FlagHeadersMode)

	switch writerCh.Type {
	case kafkaMessageTypeReplicationTask:
		writeReplicationTask(outputFile, writerCh, skippedCount, skipErrMode, headerMode, serializer, c)
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
	serializer persistence.PayloadSerializer,
	c *cli.Context,
) {
	filter := buildFilterFn(c.String(FlagWorkflowID), c.String(FlagRunID))
Loop:
	for {
		select {
		case task, ok := <-writerCh.ReplicationTaskChannel:
			if !ok {
				break Loop
			}
			if filter(task) {
				jsonStr, err := decodeReplicationTask(task, serializer)
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
						*task.HistoryTaskAttributes.DomainId,
						*task.HistoryTaskAttributes.WorkflowId,
						*task.HistoryTaskAttributes.RunId,
						*task.HistoryTaskAttributes.FirstEventId,
						*task.HistoryTaskAttributes.NextEventId,
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
Loop:
	for {
		select {
		case msg, ok := <-writerCh.VisibilityMsgChannel:
			if !ok {
				break Loop
			}
			if filter(msg) {
				jsonStr, err := json.Marshal(msg)
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
						msg.GetDomainID(),
						msg.GetWorkflowID(),
						msg.GetRunID(),
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

func deserializeMessages(messages [][]byte, skipErrors bool) ([]*replicator.ReplicationTask, int32) {
	var replicationTasks []*replicator.ReplicationTask
	var skipped int32
	for _, m := range messages {
		var task replicator.ReplicationTask
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

func decode(message []byte, val *replicator.ReplicationTask) error {
	reader := bytes.NewReader(message[1:])
	wireVal, err := protocol.Binary.Decode(reader, wire.TStruct)
	if err != nil {
		return err
	}
	return val.FromWire(wireVal)
}

func deserializeVisibilityMessages(messages [][]byte, skipErrors bool) ([]*indexer.Message, int32) {
	var visibilityMessages []*indexer.Message
	var skipped int32
	for _, m := range messages {
		var msg indexer.Message
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

func decodeVisibility(message []byte, val *indexer.Message) error {
	reader := bytes.NewReader(message[1:])
	wireVal, err := protocol.Binary.Decode(reader, wire.TStruct)
	if err != nil {
		return err
	}
	return val.FromWire(wireVal)
}

// ClustersConfig describes the kafka clusters
type ClustersConfig struct {
	Clusters map[string]messaging.ClusterConfig
	TLS      auth.TLS
}

func doRereplicate(
	ctx context.Context,
	shardID int,
	domainID string,
	wid string,
	rid string,
	endEventID int64,
	endEventVersion int64,
	sourceCluster string,
	session *gocql.Session,
	adminClient serverAdmin.Interface,
) {

	exeM, _ := cassandra.NewWorkflowExecutionPersistence(shardID, session, loggerimpl.NewNopLogger())
	exeMgr := persistence.NewExecutionManagerImpl(exeM, loggerimpl.NewNopLogger())

	fmt.Printf("Start rereplicate for wid: %v, rid:%v \n", wid, rid)
	resp, err := exeMgr.GetWorkflowExecution(&persistence.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: shared.WorkflowExecution{
			WorkflowId: common.StringPtr(wid),
			RunId:      common.StringPtr(rid),
		},
	})
	if err != nil {
		ErrorAndExit("GetWorkflowExecution error", err)
	}

	versionHistories := resp.State.VersionHistories
	if versionHistories == nil {
		ErrorAndExit("The workflow is not a NDC workflow", nil)
	}
	if err := adminClient.ResendReplicationTasks(
		ctx,
		&admin.ResendReplicationTasksRequest{
			DomainID:      common.StringPtr(domainID),
			WorkflowID:    common.StringPtr(wid),
			RunID:         common.StringPtr(rid),
			RemoteCluster: common.StringPtr(sourceCluster),
			EndEventID:    common.Int64Ptr(endEventID + 1),
			EndVersion:    common.Int64Ptr(endEventVersion),
		},
	); err != nil {
		ErrorAndExit("Failed to resend ndc workflow", err)
	}
	fmt.Printf("Done rereplicate for wid: %v, rid:%v \n", wid, rid)
}

// AdminRereplicate parses will re-publish replication tasks to topic
func AdminRereplicate(c *cli.Context) {
	numberOfShards := c.Int(FlagNumberOfShards)
	if numberOfShards <= 0 {
		ErrorAndExit("numberOfShards is must be > 0", nil)
		return
	}
	sourceCluster := getRequiredOption(c, FlagSourceCluster)
	if !c.IsSet(FlagMaxEventID) {
		ErrorAndExit("End event ID is not defined", nil)
	}
	if !c.IsSet(FlagEndEventVersion) {
		ErrorAndExit("End event version is not defined", nil)
	}

	session := connectToCassandra(c)
	adminClient := cFactory.ServerAdminClient(c)
	endEventID := c.Int64(FlagMaxEventID)
	endVersion := c.Int64(FlagEndEventVersion)
	domainID := getRequiredOption(c, FlagDomainID)
	wid := getRequiredOption(c, FlagWorkflowID)
	rid := getRequiredOption(c, FlagRunID)
	shardID := common.WorkflowIDToHistoryShard(wid, numberOfShards)
	contextTimeout := defaultResendContextTimeout
	if c.GlobalIsSet(FlagContextTimeout) {
		contextTimeout = time.Duration(c.GlobalInt(FlagContextTimeout)) * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
	defer cancel()

	doRereplicate(
		ctx,
		shardID,
		domainID,
		wid,
		rid,
		endEventID,
		endVersion,
		sourceCluster,
		session,
		adminClient,
	)
}

func decodeReplicationTask(
	task *replicator.ReplicationTask,
	serializer persistence.PayloadSerializer,
) ([]byte, error) {

	switch task.GetTaskType() {
	case replicator.ReplicationTaskTypeHistoryV2:
		historyV2 := task.GetHistoryTaskV2Attributes()
		events, err := serializer.DeserializeBatchEvents(
			persistence.NewDataBlobFromThrift(historyV2.Events),
		)
		if err != nil {
			return nil, err
		}
		var newRunEvents []*shared.HistoryEvent
		if historyV2.IsSetNewRunEvents() {
			newRunEvents, err = serializer.DeserializeBatchEvents(
				persistence.NewDataBlobFromThrift(historyV2.NewRunEvents),
			)
			if err != nil {
				return nil, err
			}
		}
		historyV2.Events = nil
		historyV2.NewRunEvents = nil
		historyV2Attributes := &historyV2Task{
			Task:         task,
			Events:       events,
			NewRunEvents: newRunEvents,
		}
		return json.Marshal(historyV2Attributes)
	default:
		return json.Marshal(task)
	}
}
