package metrics

import (
	"go.temporal.io/server/common/tqid"
)

const (
	omitted = "__omitted__"
)

// GetPerTaskQueueFamilyScope returns "namespace" and "taskqueue" tags. "taskqueue" will be "__omitted__" if
// taskQueueBreakdown is false.
func GetPerTaskQueueFamilyScope(
	handler Handler,
	namespaceName string,
	taskQueueFamily *tqid.TaskQueueFamily,
	taskQueueBreakdown bool,
	tags ...Tag,
) Handler {
	metricTaskQueueName := omitted
	if taskQueueBreakdown {
		metricTaskQueueName = taskQueueFamily.Name()
	}

	tags = append(tags, NamespaceTag(namespaceName), UnsafeTaskQueueTag(metricTaskQueueName))
	return handler.WithTags(tags...)
}

// GetPerTaskQueueScope returns GetPerTaskQueueFamilyScope plus the "task_type" tag.
func GetPerTaskQueueScope(
	handler Handler,
	namespaceName string,
	taskQueue *tqid.TaskQueue,
	taskQueueBreakdown bool,
	tags ...Tag,
) Handler {
	return GetPerTaskQueueFamilyScope(handler, namespaceName, taskQueue.Family(), taskQueueBreakdown,
		append(tags, TaskQueueTypeTag(taskQueue.TaskType()))...)
}

// GetPerTaskQueuePartitionIDScope is similar to GetPerTaskQueuePartitionTypeScope, except that the partition tag will
// hold the normal partition ID if partitionIDBreakdown is true.
func GetPerTaskQueuePartitionIDScope(
	handler Handler,
	namespaceName string,
	partition tqid.Partition,
	taskQueueBreakdown bool,
	partitionIDBreakdown bool,
	tags ...Tag,
) Handler {
	var value string
	if partition == nil {
		value = unknownValue
	} else {
		value = partition.MetricTag(partitionIDBreakdown)
	}

	return GetPerTaskQueueScope(handler, namespaceName, partition.TaskQueue(), taskQueueBreakdown,
		append(tags, PartitionTag(value))...)
}

// GetPerTaskQueuePartitionTypeScope returns GetPerTaskQueueScope scope plus a "partition" tag which
// can be "__normal__", "__sticky__", "__worker_commands__", or "_unknown_".
func GetPerTaskQueuePartitionTypeScope(
	handler Handler,
	namespaceName string,
	partition tqid.Partition,
	taskQueueBreakdown bool,
	tags ...Tag,
) Handler {
	var value string
	if partition == nil {
		value = unknownValue
	} else {
		value = partition.MetricTag(false)
	}

	return GetPerTaskQueueScope(handler, namespaceName, partition.TaskQueue(), taskQueueBreakdown,
		append(tags, PartitionTag(value))...)
}
