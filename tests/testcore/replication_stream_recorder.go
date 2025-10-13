package testcore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Message direction constants
const (
	DirectionSend       = "send"
	DirectionRecv       = "recv"
	DirectionServerSend = "server_send"
	DirectionServerRecv = "server_recv"
)

// ReplicationStreamRecorder captures replication stream messages for testing
type ReplicationStreamRecorder struct {
	mu               sync.RWMutex
	capturedMessages []CapturedReplicationMessage
	outputFile       *os.File
	outputFilePath   string
}

// CapturedReplicationMessage represents a captured replication message
type CapturedReplicationMessage struct {
	Timestamp     string          `json:"timestamp"`
	Method        string          `json:"method"`
	Direction     string          `json:"direction"`
	ClusterName   string          `json:"clusterName"`
	TargetAddress string          `json:"targetAddress"`
	MessageType   string          `json:"messageType"`
	IsStreamCall  bool            `json:"isStreamCall"`
	Request       proto.Message   `json:"-"` // Don't marshal directly
	Response      proto.Message   `json:"-"` // Don't marshal directly
	Message       json.RawMessage `json:"message,omitempty"`
}

func NewReplicationStreamRecorder() *ReplicationStreamRecorder {
	return &ReplicationStreamRecorder{
		capturedMessages: make([]CapturedReplicationMessage, 0),
	}
}

// SetOutputFile sets the file path for writing captured messages on-demand
func (r *ReplicationStreamRecorder) SetOutputFile(filePath string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.outputFilePath = filePath
}

// WriteToLog writes all captured messages to the configured output file
func (r *ReplicationStreamRecorder) WriteToLog() error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.outputFilePath == "" {
		return errors.New("output file path not set")
	}

	// Create or truncate the output file
	f, err := os.Create(r.outputFilePath)
	if err != nil {
		return fmt.Errorf("failed to create output file %s: %w", r.outputFilePath, err)
	}
	defer func() {
		_ = f.Close()
	}()

	// Write all captured messages
	for _, captured := range r.capturedMessages {
		formattedMsg := r.formatCapturedMessage(captured)
		if _, err := f.WriteString(formattedMsg + "\n"); err != nil {
			return fmt.Errorf("failed to write message: %w", err)
		}
	}

	return f.Sync()
}

func (r *ReplicationStreamRecorder) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.capturedMessages = make([]CapturedReplicationMessage, 0)
}

func (r *ReplicationStreamRecorder) GetMessages() []CapturedReplicationMessage {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]CapturedReplicationMessage, len(r.capturedMessages))
	copy(result, r.capturedMessages)
	return result
}

func (r *ReplicationStreamRecorder) recordMessage(method string, msg proto.Message, direction string, clusterName string, targetAddr string, isStreamCall bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	captured := CapturedReplicationMessage{
		Method:        method,
		Direction:     direction,
		ClusterName:   clusterName,
		Timestamp:     time.Now().Format(time.RFC3339Nano),
		MessageType:   string(msg.ProtoReflect().Descriptor().FullName()),
		TargetAddress: targetAddr,
		IsStreamCall:  isStreamCall,
	}

	// Store the message reference directly without cloning for performance
	if direction == DirectionSend || direction == DirectionServerSend {
		captured.Request = msg
	} else {
		captured.Response = msg
	}

	r.capturedMessages = append(r.capturedMessages, captured)
}

// formatCapturedMessage formats a single captured message as JSON for output
func (r *ReplicationStreamRecorder) formatCapturedMessage(captured CapturedReplicationMessage) string {
	// Get the appropriate proto message based on direction
	var msg proto.Message
	if captured.Direction == DirectionSend || captured.Direction == DirectionServerSend {
		msg = captured.Request
	} else {
		msg = captured.Response
	}

	// Marshal the proto message to JSON and attach to Message field
	if msg != nil {
		marshaler := protojson.MarshalOptions{
			Multiline: false,
			Indent:    "",
		}
		jsonBytes, err := marshaler.Marshal(msg)
		if err == nil {
			captured.Message = jsonBytes
		}
	}

	// Marshal the entire struct to pretty JSON
	jsonOutput, err := json.MarshalIndent(captured, "", "  ")
	if err != nil {
		return fmt.Sprintf(`{"error": "failed to marshal output: %v"}`, err)
	}

	return string(jsonOutput)
}

// UnaryInterceptor returns a gRPC unary client interceptor that captures messages
func (r *ReplicationStreamRecorder) UnaryInterceptor(clusterName string) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		target := cc.Target()

		// Capture outgoing request if it's a replication-related call
		if isReplicationMethod(method) {
			if protoReq, ok := req.(proto.Message); ok {
				r.recordMessage(method, protoReq, DirectionSend, clusterName, target, false)
			}
		}

		err := invoker(ctx, method, req, reply, cc, opts...)

		// Capture incoming response if successful
		if err == nil && isReplicationMethod(method) {
			if protoReply, ok := reply.(proto.Message); ok {
				r.recordMessage(method, protoReply, DirectionRecv, clusterName, target, false)
			}
		}

		return err
	}
}

// StreamInterceptor returns a gRPC stream client interceptor that captures stream messages
func (r *ReplicationStreamRecorder) StreamInterceptor(clusterName string) grpc.StreamClientInterceptor {
	return func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		stream, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			return nil, err
		}

		if isReplicationMethod(method) {
			return &recordingClientStream{
				ClientStream:  stream,
				recorder:      r,
				method:        method,
				clusterName:   clusterName,
				targetAddress: cc.Target(),
			}, nil
		}

		return stream, nil
	}
}

// recordingClientStream wraps a grpc.ClientStream to record messages
type recordingClientStream struct {
	grpc.ClientStream
	recorder      *ReplicationStreamRecorder
	method        string
	clusterName   string
	targetAddress string
}

func (s *recordingClientStream) SendMsg(m interface{}) error {
	if msg, ok := m.(proto.Message); ok {
		// SendMsg means this cluster is SENDING a message (could be request or ack)
		s.recorder.recordMessage(s.method, msg, DirectionSend, s.clusterName, s.targetAddress, true)
	}
	return s.ClientStream.SendMsg(m)
}

func (s *recordingClientStream) RecvMsg(m interface{}) error {
	err := s.ClientStream.RecvMsg(m)
	if err == nil {
		if msg, ok := m.(proto.Message); ok {
			// RecvMsg means this cluster is RECEIVING a message (could be request or data)
			s.recorder.recordMessage(s.method, msg, DirectionRecv, s.clusterName, s.targetAddress, true)
		}
	}
	return err
}

// UnaryServerInterceptor returns a gRPC unary server interceptor that captures messages
func (r *ReplicationStreamRecorder) UnaryServerInterceptor(clusterName string) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		// Capture incoming request if it's a replication-related call
		if isReplicationMethod(info.FullMethod) {
			if protoReq, ok := req.(proto.Message); ok {
				r.recordMessage(info.FullMethod, protoReq, DirectionServerRecv, clusterName, "server", false)
			}
		}

		resp, err := handler(ctx, req)

		// Capture outgoing response if successful
		if err == nil && isReplicationMethod(info.FullMethod) {
			if protoResp, ok := resp.(proto.Message); ok {
				r.recordMessage(info.FullMethod, protoResp, DirectionServerSend, clusterName, "server", false)
			}
		}

		return resp, err
	}
}

// StreamServerInterceptor returns a gRPC stream server interceptor that captures stream messages
func (r *ReplicationStreamRecorder) StreamServerInterceptor(clusterName string) grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		if isReplicationMethod(info.FullMethod) {
			wrappedStream := &recordingServerStream{
				ServerStream: ss,
				recorder:     r,
				method:       info.FullMethod,
				clusterName:  clusterName,
			}
			return handler(srv, wrappedStream)
		}

		return handler(srv, ss)
	}
}

// recordingServerStream wraps a grpc.ServerStream to record messages
type recordingServerStream struct {
	grpc.ServerStream
	recorder    *ReplicationStreamRecorder
	method      string
	clusterName string
}

func (s *recordingServerStream) SendMsg(m interface{}) error {
	if msg, ok := m.(proto.Message); ok {
		// Server SendMsg means this server is SENDING a message to the client
		s.recorder.recordMessage(s.method, msg, DirectionServerSend, s.clusterName, "server", true)
	}
	return s.ServerStream.SendMsg(m)
}

func (s *recordingServerStream) RecvMsg(m interface{}) error {
	err := s.ServerStream.RecvMsg(m)
	if err == nil {
		if msg, ok := m.(proto.Message); ok {
			// Server RecvMsg means this server is RECEIVING a message from the client
			s.recorder.recordMessage(s.method, msg, DirectionServerRecv, s.clusterName, "server", true)
		}
	}
	return err
}

func isReplicationMethod(method string) bool {
	// Capture StreamWorkflowReplicationMessages from both history and admin services
	// - Sender (active) uses history service to respond to receiver
	// - Receiver (standby) uses admin service to call sender
	return method == "/temporal.server.api.historyservice.v1.HistoryService/StreamWorkflowReplicationMessages" ||
		method == "/temporal.server.api.adminservice.v1.AdminService/StreamWorkflowReplicationMessages"
}
