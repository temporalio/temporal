// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package hsm

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"google.golang.org/protobuf/proto"
)

const (
	defaultMaxOperations = 10000
	defaultPruneTimeout  = 5 * time.Minute
	defaultPruneInterval = 1 * time.Hour
	maxResultsLimit      = 10000
)

// OpLogger handles persistence and retrieval of state machine operations
type OpLogger interface {
	// LogOperation records a state machine operation
	LogOperation(ctx context.Context, op *persistencespb.StateMachineOperation) error

	// GetOperations retrieves operations for a given path
	GetOperations(ctx context.Context, path []string) ([]*persistencespb.StateMachineOperation, error)

	// PruneOperations removes operations older than the given duration
	PruneOperations(ctx context.Context, retention time.Duration) error

	// Close stops the background pruning
	Close() error
}

type OpLog struct {
	sync.RWMutex
	operations    []*persistencespb.StateMachineOperation
	maxOperations int
	retention     time.Duration
	pruneInterval time.Duration
	stopCh        chan struct{}
	closed        bool
	logger        log.Logger
	wg            sync.WaitGroup
}

type Config struct {
	MaxOperations int
	Retention     time.Duration
	PruneInterval time.Duration
	Logger        log.Logger
}

func NewOpLog(config *Config) (*OpLog, error) {
	if config == nil {
		return nil, fmt.Errorf("config must be provided")
	}

	if config.MaxOperations <= 0 {
		config.MaxOperations = defaultMaxOperations
	}
	if config.Retention <= 0 {
		config.Retention = 7 * 24 * time.Hour // 1 week default
	}
	if config.PruneInterval <= 0 {
		config.PruneInterval = defaultPruneInterval
	}
	if config.Logger == nil {
		return nil, fmt.Errorf("logger must be provided")
	}

	l := &OpLog{
		operations:    make([]*persistencespb.StateMachineOperation, 0),
		maxOperations: config.MaxOperations,
		retention:     config.Retention,
		pruneInterval: config.PruneInterval,
		stopCh:        make(chan struct{}),
		logger:        config.Logger,
	}

	ready := make(chan struct{})
	errCh := make(chan error, 1) // Buffer of 1 to avoid goroutine leak if initialization fails

	go func() {
		defer close(errCh)
		close(ready)
		l.pruneLoop()
	}()

	<-ready

	config.Logger.Debug("Initializing operation log",
		tag.Name("oplog"),
		tag.Value("initialize"))

	return l, nil
}

func (l *OpLog) LogOperation(ctx context.Context, op *persistencespb.StateMachineOperation) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	l.Lock()
	defer l.Unlock()

	if l.closed {
		return fmt.Errorf("operation log is closed")
	}

	l.logger.Debug("Logging operation",
		tag.Name("oplog"),
		tag.Value(op.OperationType.String()))

	opCopy := proto.Clone(op).(*persistencespb.StateMachineOperation)

	// If we're at capacity, remove oldest entries
	if len(l.operations) >= l.maxOperations {
		l.logger.Warn("Operation log at capacity, dropping oldest entry",
			tag.Name("oplog"),
			tag.Value("capacity_reached"))
		l.operations = l.operations[1:]
	}

	l.operations = append(l.operations, opCopy)
	return nil
}

// TODO: Consider memory allocation optimizations:
// - Consider returning an iterator instead
func (l *OpLog) GetOperations(ctx context.Context, path []string) ([]*persistencespb.StateMachineOperation, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	l.RLock()
	defer l.RUnlock()

	if l.closed {
		return nil, fmt.Errorf("operation log is closed")
	}

	l.logger.Debug("Fetching operations",
		tag.Name("oplog"),
		tag.Value(fmt.Sprintf("path: %v", path)))

	// Pre-allocate with reasonable size
	matches := make([]*persistencespb.StateMachineOperation, 0, min(len(l.operations), maxResultsLimit))

	// Break up work into chunks and check context periodically
	const chunkSize = 1000
	for i := 0; i < len(l.operations) && len(matches) < maxResultsLimit; i += chunkSize {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		end := i + chunkSize
		if end > len(l.operations) {
			end = len(l.operations)
		}

		for _, op := range l.operations[i:end] {
			if pathEquals(op.Path, path) {
				matches = append(matches, proto.Clone(op).(*persistencespb.StateMachineOperation))
				if len(matches) >= maxResultsLimit {
					l.logger.Warn("GetOperations reached result limit",
						tag.Name("oplog"),
						tag.Value(fmt.Sprintf("limit: %d", maxResultsLimit)))
					break
				}
			}
		}
	}

	return matches, nil
}

func (l *OpLog) PruneOperations(ctx context.Context, retention time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	l.Lock()
	defer l.Unlock()

	if l.closed {
		return fmt.Errorf("operation log is closed")
	}

	beforeCount := len(l.operations)

	cutoff := time.Now().Add(-retention)
	var retained []*persistencespb.StateMachineOperation

	// Break up work into chunks and check context periodically
	const chunkSize = 1000
	for i := 0; i < len(l.operations); i += chunkSize {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		end := i + chunkSize
		if end > len(l.operations) {
			end = len(l.operations)
		}

		for _, op := range l.operations[i:end] {
			if op.OperationTime.AsTime().After(cutoff) {
				retained = append(retained, op)
			}
		}
	}
	afterCount := len(retained)
	l.logger.Info("Pruned operations",
		tag.Name("oplog"),
		tag.Value(fmt.Sprintf("pruned %d operations", beforeCount-afterCount)))

	l.operations = retained
	return nil
}

func (l *OpLog) Close() error {
	l.Lock()
	defer l.Unlock()

	if l.closed {
		return nil
	}

	l.logger.Info("Closing operation log",
		tag.Name("oplog"),
		tag.Value("close"))

	close(l.stopCh)

	// Wait for any in-progress prune operation to complete
	l.wg.Wait()

	l.closed = true
	return nil
}

func (l *OpLog) pruneLoop() {
	ticker := time.NewTicker(l.pruneInterval)
	defer ticker.Stop()

	// Check if we're closed before starting loop
	select {
	case <-l.stopCh:
		return
	default:
	}

	for {
		select {
		case <-ticker.C:
			l.wg.Add(1)
			ctx, cancel := context.WithTimeout(context.Background(), defaultPruneTimeout)

			l.RLock()
			retention := l.retention
			l.RUnlock()

			if err := l.PruneOperations(ctx, retention); err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					l.logger.Error("Prune operation timed out",
						tag.Name("oplog"),
						tag.Error(err))
				} else {
					l.logger.Error("Failed to prune operations",
						tag.Name("oplog"),
						tag.Error(err))
				}
			}

			cancel()
			l.wg.Done()
		case <-l.stopCh:
			return
		}
	}
}

func (l *OpLog) GetRetention() time.Duration {
	l.RLock()
	defer l.RUnlock()
	return l.retention
}

func (l *OpLog) SetRetention(d time.Duration) error {
	if d <= 0 {
		return fmt.Errorf("retention must be positive")
	}

	l.Lock()
	defer l.Unlock()
	if l.closed {
		return fmt.Errorf("operation log is closed")
	}
	l.retention = d
	return nil
}

func pathEquals(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
