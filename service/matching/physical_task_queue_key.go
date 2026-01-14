package matching

import (
	"encoding/base64"
	"errors"
	"strconv"

	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/common/worker_versioning"
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
func DeploymentQueueKey(p tqid.Partition, deployment *deploymentpb.Deployment) *PhysicalTaskQueueKey {
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

func (v PhysicalTaskQueueVersion) Deployment() *deploymentpb.Deployment {
	if len(v.deploymentSeriesName) > 0 {
		return &deploymentpb.Deployment{
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
	return v.deploymentSeriesName + worker_versioning.WorkerDeploymentVersionDelimiter + v.buildId
}
