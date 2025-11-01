package testcore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// MatchingStreamRecorder captures matching service messages for testing
type MatchingStreamRecorder struct {
	mu               sync.RWMutex
	capturedMessages []CapturedMatchingMessage
	outputFile       *os.File
	outputFilePath   string
}

// CapturedMatchingMessage represents a captured matching service message
type CapturedMatchingMessage struct {
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

func NewMatchingStreamRecorder() *MatchingStreamRecorder {
	return &MatchingStreamRecorder{
		capturedMessages: make([]CapturedMatchingMessage, 0),
	}
}

// SetOutputFile sets the file path for writing captured messages on-demand
func (r *MatchingStreamRecorder) SetOutputFile(filePath string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.outputFilePath = filePath
}

// WriteToLog writes all captured messages to the configured output file
func (r *MatchingStreamRecorder) WriteToLog() error {
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

func (r *MatchingStreamRecorder) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.capturedMessages = make([]CapturedMatchingMessage, 0)
}

func (r *MatchingStreamRecorder) GetMessages() []CapturedMatchingMessage {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]CapturedMatchingMessage, len(r.capturedMessages))
	copy(result, r.capturedMessages)
	return result
}

func (r *MatchingStreamRecorder) recordMessage(method string, msg proto.Message, direction string, clusterName string, targetAddr string, isStreamCall bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	captured := CapturedMatchingMessage{
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
func (r *MatchingStreamRecorder) formatCapturedMessage(captured CapturedMatchingMessage) string {
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
func (r *MatchingStreamRecorder) UnaryInterceptor(clusterName string) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		target := cc.Target()

		// Capture outgoing request if it's a matching-related call
		if isMatchingMethod(method) {
			if protoReq, ok := req.(proto.Message); ok {
				r.recordMessage(method, protoReq, DirectionSend, clusterName, target, false)
			}
		}

		err := invoker(ctx, method, req, reply, cc, opts...)

		// Capture incoming response if successful
		if err == nil && isMatchingMethod(method) {
			if protoReply, ok := reply.(proto.Message); ok {
				r.recordMessage(method, protoReply, DirectionRecv, clusterName, target, false)
			}
		}

		return err
	}
}

// StreamInterceptor returns a gRPC stream client interceptor that captures stream messages
func (r *MatchingStreamRecorder) StreamInterceptor(clusterName string) grpc.StreamClientInterceptor {
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

		if isMatchingMethod(method) {
			return &recordingMatchingClientStream{
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

// recordingMatchingClientStream wraps a grpc.ClientStream to record messages
type recordingMatchingClientStream struct {
	grpc.ClientStream
	recorder      *MatchingStreamRecorder
	method        string
	clusterName   string
	targetAddress string
}

func (s *recordingMatchingClientStream) SendMsg(m interface{}) error {
	if msg, ok := m.(proto.Message); ok {
		// SendMsg means this cluster is SENDING a message (could be request or ack)
		s.recorder.recordMessage(s.method, msg, DirectionSend, s.clusterName, s.targetAddress, true)
	}
	return s.ClientStream.SendMsg(m)
}

func (s *recordingMatchingClientStream) RecvMsg(m interface{}) error {
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
func (r *MatchingStreamRecorder) UnaryServerInterceptor(clusterName string) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		// Capture incoming request if it's a matching-related call
		if isMatchingMethod(info.FullMethod) {
			if protoReq, ok := req.(proto.Message); ok {
				r.recordMessage(info.FullMethod, protoReq, DirectionServerRecv, clusterName, "server", false)
			}
		}

		resp, err := handler(ctx, req)

		// Capture outgoing response if successful
		if err == nil && isMatchingMethod(info.FullMethod) {
			if protoResp, ok := resp.(proto.Message); ok {
				r.recordMessage(info.FullMethod, protoResp, DirectionServerSend, clusterName, "server", false)
			}
		}

		return resp, err
	}
}

// StreamServerInterceptor returns a gRPC stream server interceptor that captures stream messages
func (r *MatchingStreamRecorder) StreamServerInterceptor(clusterName string) grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		if isMatchingMethod(info.FullMethod) {
			wrappedStream := &recordingMatchingServerStream{
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

// recordingMatchingServerStream wraps a grpc.ServerStream to record messages
type recordingMatchingServerStream struct {
	grpc.ServerStream
	recorder    *MatchingStreamRecorder
	method      string
	clusterName string
}

func (s *recordingMatchingServerStream) SendMsg(m interface{}) error {
	if msg, ok := m.(proto.Message); ok {
		// Server SendMsg means this server is SENDING a message to the client
		s.recorder.recordMessage(s.method, msg, DirectionServerSend, s.clusterName, "server", true)
	}
	return s.ServerStream.SendMsg(m)
}

func (s *recordingMatchingServerStream) RecvMsg(m interface{}) error {
	err := s.ServerStream.RecvMsg(m)
	if err == nil {
		if msg, ok := m.(proto.Message); ok {
			// Server RecvMsg means this server is RECEIVING a message from the client
			s.recorder.recordMessage(s.method, msg, DirectionServerRecv, s.clusterName, "server", true)
		}
	}
	return err
}

func isMatchingMethod(method string) bool {
	// Capture all matching service calls
	return strings.Contains(method, ".matchingservice.v1.MatchingService/")
}
