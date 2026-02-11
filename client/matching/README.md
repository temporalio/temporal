# Matching Client

This package provides the client for communicating with the Matching service.
It handles two key concerns: partition selection and routing.

## Partition Selection (Load Balancing)

The `LoadBalancer` distributes add/poll API calls across available task queue partitions.

**Write path**: Partitions are selected uniformly at random from the configured write partition count.

**Read path**: Partitions are selected to balance poller counts across partitions.
The load balancer tracks outstanding polls per partition
and sends new polls to the partition with the fewest active polls.

The number of partitions is controlled by dynamic config
(`matching.numTaskqueueWritePartitions`, `matching.numTaskqueueReadPartitions`).

There are test hooks to force specific partition selection for testing.

## Routing

Routing determines which matching node owns a given task queue partition.
All clients (frontend, history) independently perform this computation using
consistent hashing via ringpop.

### Basic Routing

Each partition has a routing key of the form:

```
namespace_id:queue_name:task_type
```

This key is hashed with the consistent hashing algorithm to find the owning node.

### Spread Routing

With basic routing, partitions of the same queue are placed independently,
 which can cause multiple partitions to land on the same node, creating hot spots.

Spread routing groups partitions into batches and uses `LookupN` to ensure
partitions within a batch are assigned to different nodes if possible.

The batch size is controlled by dynamic config `matching.spreadRoutingBatchSize`,
default zero (i.e. use basic routing).

**Algorithm**:
1. Compute batch number: `batch = partition_id / batch_size`
2. Compute index within batch: `index = partition_id % batch_size`
3. Generate routing key with batch number (batch 0 omits the batch number for backward compatibility):
   `namespace_id:queue_name:batch_number:task_type`
4. Call `LookupN(key, index+1)` and take the host at position `index`

For example, with batch size 8 and partition 25:
- Batch 3 (floor(25/8)), index 1 (25%8)
- Key: `namespace_id:queue_name:3:task_type`
- Call `LookupN(key, 2)`, take host at index 1

If fewer hosts are available than the batch size,
wrap around to spread among available hosts.

**Tradeoff**: Larger batch sizes provide better spread but cause more partition
movement when membership changes.

### Changes

Changing partition count dynamically is generally safe and doesn't cause partitions to move.
The caveat is that when reducing, write partitions has to be reduced first,
and then the extra partitions have to be empty before reducing read partitions.

Changing batch size will cause most partitions to move between nodes.
To avoid moving lots of partitions simultaneously on a live cluster,
spread routing can be rolled out gradually (partition by partition)
using wall-clock-synchronized changes. See the the `GradualChange` mechanism.

## Interface

The `Route(partition)` method on the client computes the owning node address for any partition.
This is used internally by the grpc client, and can be used by other code to
determine the owner for other purposes (e.g. matching engine knowing when to
unload non-owned partitions).
