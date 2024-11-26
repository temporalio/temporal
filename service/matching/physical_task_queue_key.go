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

	"go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/tqid"
)

const (
	// nonRootPartitionPrefix is the prefix for all mangled task queue names.
	nonRootPartitionPrefix  = "/_sys/"
	partitionDelimiter      = "/"
	versionSetDelimiter     = ":"
	buildIdDelimiter        = "#"
	deploymentNameDelimiter = "|"
)

type (
	// PhysicalTaskQueueKey Each task queue partition corresponds to one or more "physical" (aka DB-level) task queues,
	// each of which has a distinct physicalTaskQueueManager in memory in matching service, as well as a
	// distinct identity in persistence.
	//
	// Physical task queues with a version set or build ID are called "versioned". The ones without a version set
	// or build ID are called "unversioned". A physical queue cannot have both version set and build ID.
	PhysicalTaskQueueKey struct {
		partition tqid.Partition
		version   PhysicalTaskQueueVersion
	}

	PhysicalTaskQueueVersion struct {
		versionSet string // version set id
		// buildId and versionSet are mutually exclusive. buildId must be set if deploymentSeriesName is.
		buildId string
		// When present, it means this is a V3 pinned queue.
		deploymentSeriesName string
	}
)

var (
	ErrInvalidPersistenceName = errors.New("invalid persistence name")
)

func (q *PhysicalTaskQueueKey) NamespaceId() string {
	return q.partition.NamespaceId()
}

func (q *PhysicalTaskQueueKey) TaskQueueFamily() *tqid.TaskQueueFamily {
	return q.partition.TaskQueue().Family()
}

func (q *PhysicalTaskQueueKey) TaskType() enumspb.TaskQueueType {
	return q.partition.TaskType()
}

func (q *PhysicalTaskQueueKey) Partition() tqid.Partition {
	return q.partition
}

// UnversionedQueueKey returns the unversioned PhysicalTaskQueueKey of a task queue partition
func UnversionedQueueKey(p tqid.Partition) *PhysicalTaskQueueKey {
	return &PhysicalTaskQueueKey{
		partition: p,
	}
}

// VersionSetQueueKey returns a PhysicalTaskQueueKey of a task queue partition with the given version set id.
func VersionSetQueueKey(p tqid.Partition, versionSet string) *PhysicalTaskQueueKey {
	return &PhysicalTaskQueueKey{
		partition: p,
		version: PhysicalTaskQueueVersion{
			versionSet: versionSet,
		},
	}
}

// BuildIdQueueKey returns a PhysicalTaskQueueKey of a task queue partition with the given build ID.
func BuildIdQueueKey(p tqid.Partition, buildId string) *PhysicalTaskQueueKey {
	return &PhysicalTaskQueueKey{
		partition: p,
		version: PhysicalTaskQueueVersion{
			buildId: buildId,
		},
	}
}

// DeploymentQueueKey returns a PhysicalTaskQueueKey of a task queue partition for a deployment.
func DeploymentQueueKey(p tqid.Partition, deployment *deployment.Deployment) *PhysicalTaskQueueKey {
	return &PhysicalTaskQueueKey{
		partition: p,
		version: PhysicalTaskQueueVersion{
			buildId:              deployment.GetBuildId(),
			deploymentSeriesName: deployment.GetSeriesName(),
		},
	}
}

// PersistenceName returns the unique name for this DB queue to be used in persistence.
//
// Unversioned DB use the RPC name of the partition, i.e.:
//
//	sticky: 				<sticky name>
//	unversioned and root: 	<base name>
//	unversioned: 			/_sys/<base name>/<partition id>
//
// All versioned DB queues use mangled names, using the following format:
//
//	with build ID: 		/_sys/<base name>/<deployment name base64 URL encoded>|<build ID base64 URL encoded>#<partition id>
//	with build ID: 		/_sys/<base name>/<build ID base64 URL encoded>#<partition id>
//	with version set: 		/_sys/<base name>/<version set id>:<partition id>
func (q *PhysicalTaskQueueKey) PersistenceName() string {
	switch p := q.Partition().(type) {
	case *tqid.StickyPartition:
		return p.StickyName()
	case *tqid.NormalPartition:
		baseName := q.TaskQueueFamily().Name()

		if len(q.version.versionSet) > 0 {
			return nonRootPartitionPrefix + baseName + partitionDelimiter + q.version.versionSet + versionSetDelimiter + strconv.Itoa(p.PartitionId())
		}

		if len(q.version.deploymentSeriesName) > 0 {
			encodedBuildId := base64.RawURLEncoding.EncodeToString([]byte(q.version.buildId))
			encodedDeploymentName := base64.RawURLEncoding.EncodeToString([]byte(q.version.deploymentSeriesName))
			return nonRootPartitionPrefix + baseName + partitionDelimiter + encodedDeploymentName + deploymentNameDelimiter + encodedBuildId + buildIdDelimiter + strconv.Itoa(p.PartitionId())
		} else if len(q.version.buildId) > 0 {
			encodedBuildId := base64.URLEncoding.EncodeToString([]byte(q.version.buildId))
			return nonRootPartitionPrefix + baseName + partitionDelimiter + encodedBuildId + buildIdDelimiter + strconv.Itoa(p.PartitionId())
		}

		// unversioned
		if p.IsRoot() {
			return baseName
		}
		return nonRootPartitionPrefix + baseName + partitionDelimiter + strconv.Itoa(p.PartitionId())
	default:
		panic("unsupported partition kind: " + p.Kind().String())
	}
}

// ParsePhysicalTaskQueueKey takes the persistence name of a DB task queue and returns a PhysicalTaskQueueKey. Returns an error if the
// given name is not a valid persistence name.
func ParsePhysicalTaskQueueKey(persistenceName string, namespaceId string, taskType enumspb.TaskQueueType) (*PhysicalTaskQueueKey, error) {
	baseName := persistenceName
	partitionId := 0
	versionSet := ""
	buildId := ""
	deploymentName := ""

	if strings.HasPrefix(persistenceName, nonRootPartitionPrefix) {
		suffixOff := strings.LastIndex(persistenceName, partitionDelimiter)
		if suffixOff <= len(nonRootPartitionPrefix) {
			return nil, fmt.Errorf("%w: %s", ErrInvalidPersistenceName, persistenceName)
		}
		baseName = persistenceName[len(nonRootPartitionPrefix):suffixOff]
		suffix := persistenceName[suffixOff+1:]
		var err error
		partitionId, versionSet, buildId, deploymentName, err = parseSuffix(persistenceName, suffix)
		if err != nil {
			return nil, err
		}
	}

	f, err := tqid.NewTaskQueueFamily(namespaceId, baseName)
	if err != nil {
		return nil, err
	}
	return &PhysicalTaskQueueKey{
		partition: f.TaskQueue(taskType).NormalPartition(partitionId),
		version: PhysicalTaskQueueVersion{
			versionSet:           versionSet,
			buildId:              buildId,
			deploymentSeriesName: deploymentName,
		},
	}, nil
}

//nolint:revive // cognitive complexity will be simpler once versioning 1-2 is cleaned up
func parseSuffix(persistenceName string, suffix string) (partition int, versionSet string, buildId string, deploymentName string, err error) {
	if buildIdOff := strings.LastIndex(suffix, deploymentNameDelimiter); buildIdOff == 0 {
		return 0, "", "", "", fmt.Errorf("%w: %s", ErrInvalidPersistenceName, persistenceName)
	} else if buildIdOff > 0 {
		deploymentNameBytes, err := base64.RawURLEncoding.DecodeString(suffix[:buildIdOff])
		if err != nil {
			return 0, "", "", "", fmt.Errorf("%w: %s", ErrInvalidPersistenceName, persistenceName)
		}
		deploymentName = string(deploymentNameBytes)

		suffix = suffix[buildIdOff+1:]
		if partitionOff := strings.LastIndex(suffix, buildIdDelimiter); partitionOff == 0 {
			return 0, "", "", "", fmt.Errorf("%w: %s", ErrInvalidPersistenceName, persistenceName)
		} else if partitionOff > 0 {
			buildIdBytes, err := base64.RawURLEncoding.DecodeString(suffix[:partitionOff])
			if err != nil {
				return 0, "", "", "", fmt.Errorf("%w: %s", ErrInvalidPersistenceName, persistenceName)
			}
			buildId = string(buildIdBytes)
			suffix = suffix[partitionOff+1:]
		}
	} else if partitionOff := strings.LastIndex(suffix, buildIdDelimiter); partitionOff == 0 {
		return 0, "", "", "", fmt.Errorf("%w: %s", ErrInvalidPersistenceName, persistenceName)
	} else if partitionOff > 0 {
		buildIdBytes, err := base64.URLEncoding.DecodeString(suffix[:partitionOff])
		if err != nil {
			return 0, "", "", "", fmt.Errorf("%w: %s", ErrInvalidPersistenceName, persistenceName)
		}
		buildId = string(buildIdBytes)
		suffix = suffix[partitionOff+1:]
	} else if partitionOff := strings.LastIndex(suffix, versionSetDelimiter); partitionOff == 0 {
		return 0, "", "", "", fmt.Errorf("%w: %s", ErrInvalidPersistenceName, persistenceName)
	} else if partitionOff > 0 {
		// pull out version set
		versionSet, suffix = suffix[:partitionOff], suffix[partitionOff+1:]
	}

	partition, err = strconv.Atoi(suffix)
	if err != nil || partition < 0 || (partition == 0 && len(versionSet) == 0 && len(buildId) == 0) {
		return 0, "", "", "", fmt.Errorf("%w: %s", ErrInvalidPersistenceName, persistenceName)
	}
	return partition, versionSet, buildId, deploymentName, err
}

func (q *PhysicalTaskQueueKey) IsVersioned() bool {
	return q.version.IsVersioned()
}

// Version returns a pointer to the physical queue version key. Caller must not manipulate the
// returned value.
func (q *PhysicalTaskQueueKey) Version() PhysicalTaskQueueVersion {
	return q.version
}

func (v PhysicalTaskQueueVersion) IsVersioned() bool {
	return v.versionSet != "" || v.buildId != ""
}

func (v PhysicalTaskQueueVersion) Deployment() *deployment.Deployment {
	if len(v.deploymentSeriesName) > 0 {
		return &deployment.Deployment{
			SeriesName: v.deploymentSeriesName,
			BuildId:    v.buildId,
		}
	}
	return nil
}

// BuildId returns empty if this is not a Versioning v2 queue.
func (v PhysicalTaskQueueVersion) BuildId() string {
	if len(v.deploymentSeriesName) > 0 {
		return ""
	}
	return v.buildId
}

func (v PhysicalTaskQueueVersion) VersionSet() string {
	return v.versionSet
}

// MetricsTagValue returns the build ID tag value for this version.
func (v PhysicalTaskQueueVersion) MetricsTagValue() string {
	if v.versionSet != "" {
		return v.versionSet
	} else if v.deploymentSeriesName == "" {
		return v.buildId
	}
	return v.deploymentSeriesName + "/" + v.buildId
}
