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

package tqid

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common/enums"
)

const (
	// nonRootPartitionPrefix is the prefix for all mangled task queue names.
	nonRootPartitionPrefix  = "/_sys/"
	partitionDelimiter      = "/"
	reservedTaskQueuePrefix = "/_sys/"
)

type (
	// TaskQueueFamily represents the high-level "task queue" that user creates by explicitly providing a task queue name
	// when starting a worker or a workflow. A task queue family consists of separate TaskQueues for different types of
	// task (e.g. Workflow, Activity).
	TaskQueueFamily struct {
		namespaceId string
		// this can be any string as long as it does not start with /_sys/.
		name string
	}

	// TaskQueue represents a logical task queue for a type of tasks (e.g. Activity or Workflow). Under the hood,
	// a TaskQueue can be broken down to multiple sticky or normal partitions.
	TaskQueue struct {
		family   TaskQueueFamily
		taskType enumspb.TaskQueueType
	}

	// Partition is a sticky or normal partition of a TaskQueue.
	// Each Partition has a distinct task queue partition manager in memory in Matching service.
	// Normal partition with `partitionId=0` is called the "root". Sticky queues are not considered root.
	Partition interface {
		NamespaceId() string
		TaskQueue() *TaskQueue
		TaskType() enumspb.TaskQueueType
		// IsRoot always returns false for Sticky partitions
		IsRoot() bool
		Kind() enumspb.TaskQueueKind

		// RpcName returns the mangled name of the task queue partition, to be used in RPCs.
		//
		// RPC names look like this:
		//
		//  sticky partition:			<sticky name>
		//	root normal partition: 		<task queue name>
		//	non-root normal partition: 	/_sys/<task queue name>/<partition id>
		//
		// This scheme lets users use anything they like for a base name, except for strings
		// starting with "/_sys/", without ambiguity.
		//
		// For backward compatibility, unversioned low-level task queues with partition 0 do not
		// use mangled names, they use the bare base name.
		RpcName() string
		Key() PartitionKey
		// RoutingKey returns the string that should be used to find the owner of a task queue partition.
		RoutingKey() string
	}

	// NormalPartition is used to distribute load of a TaskQueue in multiple Matching instances. A normal partition is
	// identified by `partitionId`. The partition with ID 0 is called a root partition.
	NormalPartition struct {
		taskQueue   *TaskQueue
		partitionId int
	}

	// StickyPartition is made by SDK for a single workflow worker to keep workflow tasks of the same execution
	// in the same worker for caching benefits. Each sticky partition is identified by a unique `stickyName`
	// generated by SDK. A StickyPartition can only have workflow task type.
	StickyPartition struct {
		stickyName string
		taskQueue  *TaskQueue
	}

	// PartitionKey uniquely identifies a task queue partition, to be used in maps.
	// Note that task queue kind (sticky vs normal) and normal name for sticky task queues are not
	// part of the task queue partition identity.
	PartitionKey struct {
		namespaceId string
		name        string
		partitionId int
		taskType    enumspb.TaskQueueType
	}
)

var _ Partition = (*NormalPartition)(nil)
var _ Partition = (*StickyPartition)(nil)

var (
	ErrNoParent      = errors.New("root task queue partition has no parent")
	ErrInvalidDegree = errors.New("invalid task queue partition branching degree")
	ErrNonZeroSticky = errors.New("only sticky partitions can not have non-zero partition ID")
)

// NewTaskQueueFamily takes a user-provided task queue name (aka family name) and returns a TaskQueueFamily. Returns an
// error if name looks like a mangled name.
func NewTaskQueueFamily(namespaceId string, name string) (*TaskQueueFamily, error) {
	if strings.HasPrefix(name, nonRootPartitionPrefix) {
		return nil, serviceerror.NewInvalidArgument("task queue family name cannot have prefix /_sys/ " + name)
	}
	return &TaskQueueFamily{
		namespaceId: namespaceId,
		name:        name,
	}, nil
}

// UnsafeTaskQueueFamily should be avoided as much as possible. Use NewTaskQueueFamily instead as it validates the tq name.
func UnsafeTaskQueueFamily(namespaceId string, name string) *TaskQueueFamily {
	return &TaskQueueFamily{namespaceId, name}
}

// UnsafePartitionFromProto tries parsing proto using PartitionFromProto but if it fails still returns a Partition
// object using the raw values in the proto.
// This method should only be used in logs/metrics, not in the server logic.
func UnsafePartitionFromProto(proto *taskqueuepb.TaskQueue, namespaceId string, taskType enumspb.TaskQueueType) Partition {
	p, err := PartitionFromProto(proto, namespaceId, taskType)
	if err == nil {
		return p
	}
	kind := proto.GetKind()
	switch kind { //nolint:exhaustive
	case enumspb.TASK_QUEUE_KIND_STICKY:
		tq := &TaskQueue{TaskQueueFamily{namespaceId, proto.GetNormalName()}, taskType}
		return tq.StickyPartition(proto.GetName())
	default:
		tq := &TaskQueue{TaskQueueFamily{namespaceId, proto.GetName()}, taskType}
		return tq.RootPartition()
	}
}

func PartitionFromProto(proto *taskqueuepb.TaskQueue, namespaceId string, taskType enumspb.TaskQueueType) (Partition, error) {
	baseName, partition, err := parseRpcName(proto.GetName())
	if err != nil {
		return nil, err
	}

	kind := proto.GetKind()
	normalName := proto.GetNormalName()
	if normalName != "" && kind != enumspb.TASK_QUEUE_KIND_STICKY {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("only sticky queues can have normal name. tq: %s, normal name: %s", baseName, normalName))
	}

	switch kind {
	case enumspb.TASK_QUEUE_KIND_STICKY:
		if partition != 0 {
			return nil, fmt.Errorf("%w. base name: %s, normal name: %s", ErrNonZeroSticky, baseName, normalName)
		}
		tq := &TaskQueue{TaskQueueFamily{namespaceId, normalName}, taskType}
		return tq.StickyPartition(baseName), nil
	default:
		tq := &TaskQueue{TaskQueueFamily{namespaceId, baseName}, taskType}
		return tq.NormalPartition(partition), nil
	}
}

func PartitionFromPartitionProto(proto *taskqueuespb.TaskQueuePartition, namespaceId string) Partition {
	tq := &TaskQueue{TaskQueueFamily{namespaceId, proto.GetTaskQueue()}, proto.GetTaskQueueType()}
	switch proto.GetPartitionId().(type) {
	case *taskqueuespb.TaskQueuePartition_StickyName:
		return tq.StickyPartition(proto.GetStickyName())
	default:
		return tq.NormalPartition(int(proto.GetNormalPartitionId()))
	}
}

func NormalPartitionFromRpcName(rpcName string, namespaceId string, taskType enumspb.TaskQueueType) (*NormalPartition, error) {
	baseName, partition, err := parseRpcName(rpcName)
	if err != nil {
		return nil, err
	}
	tq := &TaskQueue{TaskQueueFamily{namespaceId, baseName}, taskType}
	return tq.NormalPartition(partition), nil
}

func MustNormalPartitionFromRpcName(rpcName string, namespaceId string, taskType enumspb.TaskQueueType) *NormalPartition {
	p, err := NormalPartitionFromRpcName(rpcName, namespaceId, taskType)
	if err != nil {
		panic(err)
	}
	return p
}

func (n *TaskQueueFamily) Name() string {
	return n.name
}

func (n *TaskQueueFamily) NamespaceId() string {
	return n.namespaceId
}

func (n *TaskQueueFamily) TaskQueue(taskType enumspb.TaskQueueType) *TaskQueue {
	return &TaskQueue{
		family:   *n,
		taskType: taskType,
	}
}

func (n *TaskQueue) Name() string {
	return n.family.Name()
}

func (n *TaskQueue) Family() *TaskQueueFamily {
	return &n.family
}

func (n *TaskQueue) NamespaceId() string {
	return n.family.NamespaceId()
}

func (n *TaskQueue) TaskType() enumspb.TaskQueueType {
	return n.taskType
}

func (n *TaskQueue) NormalPartition(partitionId int) *NormalPartition {
	return &NormalPartition{
		taskQueue:   n,
		partitionId: partitionId,
	}
}

func (n *TaskQueue) StickyPartition(stickyName string) *StickyPartition {
	return &StickyPartition{stickyName, n}
}

func (n *TaskQueue) RootPartition() *NormalPartition {
	return n.NormalPartition(0)
}

func (s *StickyPartition) StickyName() string {
	return s.stickyName
}

func (s *StickyPartition) TaskType() enumspb.TaskQueueType {
	return s.taskQueue.TaskType()
}

func (s *StickyPartition) Kind() enumspb.TaskQueueKind {
	return enumspb.TASK_QUEUE_KIND_STICKY
}

func (s *StickyPartition) NamespaceId() string {
	return s.taskQueue.family.NamespaceId()
}

func (s *StickyPartition) RootPartition() Partition {
	return s
}

func (s *StickyPartition) TaskQueue() *TaskQueue {
	return s.taskQueue
}

func (s *StickyPartition) IsRoot() bool {
	return false
}

func (s *StickyPartition) RpcName() string {
	return s.stickyName
}

func (s *StickyPartition) Key() PartitionKey {
	return PartitionKey{
		namespaceId: s.NamespaceId(),
		name:        s.StickyName(),
		taskType:    s.TaskType(),
	}
}

func (s *StickyPartition) RoutingKey() string {
	return fmt.Sprintf("%s:%s:%d", s.NamespaceId(), s.RpcName(), s.TaskType())
}

func (p *NormalPartition) TaskQueue() *TaskQueue {
	return p.taskQueue
}

func (p *NormalPartition) IsRoot() bool {
	return p.partitionId == 0
}

func (p *NormalPartition) Kind() enumspb.TaskQueueKind {
	return enumspb.TASK_QUEUE_KIND_NORMAL
}

func (p *NormalPartition) PartitionId() int {
	return p.partitionId
}

func (p *NormalPartition) NamespaceId() string {
	return p.taskQueue.family.namespaceId
}

func (p *NormalPartition) TaskType() enumspb.TaskQueueType {
	return p.taskQueue.taskType
}

// ParentPartition returns a NormalPartition for the parent partition, using the given branching degree.
func (p *NormalPartition) ParentPartition(degree int) (*NormalPartition, error) {
	if p.IsRoot() {
		return nil, ErrNoParent
	} else if degree < 1 {
		return nil, ErrInvalidDegree
	}
	parent := (p.partitionId+degree-1)/degree - 1
	return p.taskQueue.NormalPartition(parent), nil
}

func (p *NormalPartition) RpcName() string {
	if p.IsRoot() {
		return p.TaskQueue().family.Name()
	}
	return nonRootPartitionPrefix + p.TaskQueue().Name() + partitionDelimiter + strconv.Itoa(p.partitionId)
}

func (p *NormalPartition) Key() PartitionKey {
	return PartitionKey{
		namespaceId: p.NamespaceId(),
		name:        p.TaskQueue().Name(),
		partitionId: p.partitionId,
		taskType:    p.TaskType(),
	}
}

func (p *NormalPartition) RoutingKey() string {
	return fmt.Sprintf("%s:%s:%d", p.NamespaceId(), p.RpcName(), p.TaskType())
}

// ValidateTaskQueue validates a TaskQueue object.
// It checks the TaskQueue's name for emptiness, length, UTF-8 validity, and whitespace.
// For sticky queues, it also validates the NormalName.
// If the name is empty and defaultVal is provided, it sets the name to defaultVal.
// If the Kind is unspecified, it sets it to NORMAL.
//
// Parameters:
//   - taskQueue: The TaskQueue to validate. If nil, returns an error.
//   - defaultVal: Default name to use if taskQueue name is empty.
//   - maxIDLengthLimit: Maximum allowed length for the TaskQueue name.
//
// Returns an error if validation fails, nil otherwise.
func ValidateTaskQueue(
	taskQueue *taskqueuepb.TaskQueue,
	defaultVal string,
	maxIDLengthLimit int,
) error {
	if taskQueue == nil {
		return serviceerror.NewInvalidArgument("taskQueue is not set.")
	}

	enums.SetDefaultTaskQueueKind(&taskQueue.Kind)

	if taskQueue.GetName() == "" {
		if defaultVal == "" {
			return serviceerror.NewInvalidArgument("missing task queue name.")
		}
		taskQueue.Name = defaultVal
	}

	if err := ValidateTaskQueueName(taskQueue.GetName(), maxIDLengthLimit); err != nil {
		return err
	}

	if taskQueue.GetKind() == enumspb.TASK_QUEUE_KIND_STICKY {
		if err := ValidateTaskQueueName(taskQueue.GetNormalName(), maxIDLengthLimit); err != nil {
			return err
		}
	}

	return nil
}

// ValidateTaskQueueName checks if a given task queue name is valid.
// It verifies the name is not empty, does not exceed the maximum length,
// contains no leading or trailing whitespace, and is a valid UTF-8 string.
//
// Parameters:
//   - name: The task queue name to validate.
//   - maxLength: The maximum allowed length for the name.
//
// Returns an error if the name is invalid, nil otherwise.
func ValidateTaskQueueName(name string, maxLength int) error {
	if name == "" {
		return serviceerror.NewInvalidArgument("taskQueue is not set.")
	}
	if len(name) > maxLength {
		return serviceerror.NewInvalidArgument("taskQueue length exceeds limit.")
	}

	if strings.TrimSpace(name) != name {
		return serviceerror.NewInvalidArgument("taskQueue name must not contain leading or trailing whitespace.")
	}

	if strings.HasPrefix(name, reservedTaskQueuePrefix) {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("task queue name cannot start with reserved prefix %v.", reservedTaskQueuePrefix))
	}

	if !utf8.ValidString(name) {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("taskQueue %v is not a valid UTF-8 string.", name))
	}

	return nil
}

// parseRpcName takes the rpc name of a task queue partition and returns a ParseTaskQueuePartition.
// Returns an error if the given name is not a valid rpc name.
func parseRpcName(rpcName string) (string, int, error) {
	baseName := rpcName
	partition := 0

	if strings.HasPrefix(rpcName, nonRootPartitionPrefix) {
		suffixOff := strings.LastIndex(rpcName, partitionDelimiter)
		if suffixOff <= len(nonRootPartitionPrefix) {
			return "", 0, serviceerror.NewInvalidArgument("invalid task queue partition name " + rpcName)
			// nolint:goerr113
		}
		baseName = rpcName[len(nonRootPartitionPrefix):suffixOff]
		suffix := rpcName[suffixOff+1:]

		var err error
		partition, err = strconv.Atoi(suffix)
		if err != nil || partition <= 0 {
			return "", 0, serviceerror.NewInvalidArgument("invalid task queue partition name " + rpcName)
		}
	}

	if strings.HasPrefix(baseName, nonRootPartitionPrefix) {
		return "", 0, serviceerror.NewInvalidArgument("task queue family name cannot have prefix /_sys/ " + baseName)
	}
	return baseName, partition, nil
}
