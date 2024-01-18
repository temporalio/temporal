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

package matching

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/tqid"
)

const (
	versionSetDelimiter = ":"
	buildIdDelimiter    = "#"
)

type (
	// DBTaskQueue Each task queue partition corresponds to one or more "DB-level task queues", each of
	// which has a distinct DB queue manager in memory in matching service, as well as a
	// distinct identity in persistence.
	//
	// DB task queues with a version set or build ID are called "versioned". The ones without a version set
	// or build ID are called "unversioned". A DB queue cannot have both version set and build ID.
	DBTaskQueue struct {
		partition  tqid.Partition
		versionSet string // version set id
		// BuildId and VersionSet are mutually exclusive
		buildId string
	}

	dbTaskQueueKey struct {
		partitionKey tqid.PartitionKey
		versionSet   string
		buildId      string
	}
)

var (
	ErrInvalidPersistenceName = errors.New("invalid persistence name")
)

func (dbq *DBTaskQueue) NamespaceID() namespace.ID {
	return dbq.partition.TaskQueue().NamespaceID()
}

func (dbq *DBTaskQueue) TaskQueue() *tqid.TaskQueue {
	return dbq.partition.TaskQueue()
}

func (dbq *DBTaskQueue) TaskType() enumspb.TaskQueueType {
	return dbq.partition.TaskType()
}

func (dbq *DBTaskQueue) Partition() tqid.Partition {
	return dbq.partition
}

func (dbq *DBTaskQueue) BuildId() string {
	return dbq.buildId
}

func (dbq *DBTaskQueue) VersionSet() string {
	return dbq.versionSet
}

func (dbq *DBTaskQueue) key() dbTaskQueueKey {
	return dbTaskQueueKey{dbq.partition.Key(), dbq.versionSet, dbq.buildId}
}

// UnversionedDBQueue returns the unversioned DBTaskQueue of a task queue partition
func UnversionedDBQueue(p tqid.Partition) *DBTaskQueue {
	return &DBTaskQueue{
		partition: p,
	}
}

// VersionSetDBQueue returns a DBTaskQueue of a task queue partition with the given version set id.
func VersionSetDBQueue(p tqid.Partition, versionSet string) *DBTaskQueue {
	return &DBTaskQueue{
		partition:  p,
		versionSet: versionSet,
	}
}

// BuildIDDBQueue returns a DBTaskQueue of a task queue partition with the given build ID.
func BuildIDDBQueue(p tqid.Partition, buildId string) *DBTaskQueue {
	return &DBTaskQueue{
		partition: p,
		buildId:   buildId,
	}
}

// PersistenceName returns the unique name for this DB queue to be used in persistence.
//
// Unversioned DB use the RPC name of the partition, i.e.:
//
//	unversioned and root: 	<base name>
//	unversioned: 			/_sys/<base name>/<partition id>
//
// All versioned DB queues use mangled names, using the following format:
//
//	with build ID: 		/_sys/<base name>/<build ID base64 URL encoded>;<partition id>
//	with version set: 		/_sys/<base name>/<version set id>:<partition id>
func (dbq *DBTaskQueue) PersistenceName() string {
	baseName := dbq.TaskQueue().Name()
	partitionId := 0

	if p, ok := dbq.Partition().(*tqid.NormalPartition); ok {
		partitionId = p.PartitionID()
	}

	if len(dbq.versionSet) > 0 {
		return fmt.Sprintf("%s%s%s%s%s%d", tqid.NonRootPartitionPrefix, baseName, tqid.PartitionDelimiter, dbq.versionSet, versionSetDelimiter, partitionId)
	}

	if len(dbq.buildId) > 0 {
		encodedBuildId := base64.URLEncoding.EncodeToString([]byte(dbq.buildId))
		return fmt.Sprintf("%s%s%s%s%s%d", tqid.NonRootPartitionPrefix, baseName, tqid.PartitionDelimiter, encodedBuildId, buildIdDelimiter, partitionId)
	}

	// unversioned DB queues use the RPC name of their partition
	return dbq.partition.RpcName()
}

// ParseDBQueue takes the persistence name of a DB task queue and returns a DBTaskQueue. Returns an error if the
// given name is not a valid persistence name.
func ParseDBQueue(persistenceName string, namespaceId string, taskType enumspb.TaskQueueType) (*DBTaskQueue, error) {
	baseName := persistenceName
	partitionId := 0
	versionSet := ""
	buildId := ""

	if strings.HasPrefix(persistenceName, tqid.NonRootPartitionPrefix) {
		suffixOff := strings.LastIndex(persistenceName, tqid.PartitionDelimiter)
		if suffixOff <= len(tqid.NonRootPartitionPrefix) {
			return nil, fmt.Errorf("%w: %s", ErrInvalidPersistenceName, persistenceName)
		}
		baseName = persistenceName[len(tqid.NonRootPartitionPrefix):suffixOff]
		suffix := persistenceName[suffixOff+1:]
		var err error
		partitionId, versionSet, buildId, err = parseSuffix(persistenceName, suffix)
		if err != nil {
			return nil, err
		}
	}

	taskQueue, err := tqid.FromBaseName(namespaceId, baseName)
	if err != nil {
		return nil, err
	}
	return &DBTaskQueue{
		partition:  taskQueue.NormalPartition(taskType, partitionId),
		versionSet: versionSet,
		buildId:    buildId,
	}, nil
}

func parseSuffix(persistenceName string, suffix string) (partition int, versionSet string, buildId string, err error) {
	if partitionOff := strings.LastIndex(suffix, buildIdDelimiter); partitionOff == 0 {
		return 0, "", "", fmt.Errorf("%w: %s", ErrInvalidPersistenceName, persistenceName)
	} else if partitionOff > 0 {
		buildIdBytes, err := base64.URLEncoding.DecodeString(suffix[:partitionOff])
		if err != nil {
			return 0, "", "", fmt.Errorf("%w: %s", ErrInvalidPersistenceName, persistenceName)
		}
		buildId = string(buildIdBytes)
		suffix = suffix[partitionOff+1:]
	} else if partitionOff := strings.LastIndex(suffix, versionSetDelimiter); partitionOff == 0 {
		return 0, "", "", fmt.Errorf("%w: %s", ErrInvalidPersistenceName, persistenceName)
	} else if partitionOff > 0 {
		// pull out version set
		versionSet, suffix = suffix[:partitionOff], suffix[partitionOff+1:]
	}

	partition, err = strconv.Atoi(suffix)
	if err != nil || partition < 0 || (partition == 0 && len(versionSet) == 0 && len(buildId) == 0) {
		return 0, "", "", fmt.Errorf("%w: %s", ErrInvalidPersistenceName, persistenceName)
	}
	return partition, versionSet, buildId, err
}

func (dbq *DBTaskQueue) String() string {
	return fmt.Sprintf("DBTaskQueue(tqrtn:%q vset:%s buildId:%s)",
		dbq.partition,
		dbq.versionSet,
		dbq.buildId,
	)
}

func (dbq *DBTaskQueue) IsVersioned() bool {
	return dbq.versionSet != "" || dbq.buildId != ""
}
