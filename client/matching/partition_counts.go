package matching

import (
	"context"
	"errors"
	"slices"

	"github.com/gogo/protobuf/proto"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// The "-bin" suffix instructs grpc to base64-encode the value, so we can use binary.
const partitionCountsHeaderName = "pcnt-bin"
const partitionCountsTrailerName = "pcnt-bin"

// PartitionCounts is a smaller version of taskqueuespb.ClientPartitionCounts that we can more
// easily pass around and put in a map.
type PartitionCounts struct {
	Read, Write int32
}

func (pc PartitionCounts) Valid() bool {
	return pc.Read > 0 && pc.Write > 0
}

func (pc PartitionCounts) encode() (string, error) {
	b, err := proto.Marshal(&taskqueuespb.ClientPartitionCounts{
		Read:  pc.Read,
		Write: pc.Write,
	})
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (pc PartitionCounts) appendToOutgoingContext(ctx context.Context) context.Context {
	v, err := pc.encode()
	if err != nil {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, partitionCountsHeaderName, v)
}

func (pc PartitionCounts) SetTrailer(ctx context.Context) error {
	v, err := pc.encode()
	if err != nil {
		return err
	}
	return grpc.SetTrailer(ctx, metadata.Pairs(partitionCountsTrailerName, v))
}

func parsePartitionCounts(hdr string) (PartitionCounts, error) {
	var cpc taskqueuespb.ClientPartitionCounts
	err := proto.Unmarshal([]byte(hdr), &cpc)
	if err != nil {
		return PartitionCounts{}, err
	}
	return PartitionCounts{
		Read:  cpc.Read,
		Write: cpc.Write,
	}, nil
}

func ParsePartitionCountsFromIncomingContext(ctx context.Context) (PartitionCounts, error) {
	vals := metadata.ValueFromIncomingContext(ctx, partitionCountsHeaderName)
	if len(vals) == 0 {
		return PartitionCounts{}, nil
	}
	return parsePartitionCounts(vals[0])
}

func parsePartitionCountsFromTrailer(trailer metadata.MD) (PartitionCounts, error) {
	vals := trailer.Get(partitionCountsTrailerName)
	if len(vals) == 0 {
		return PartitionCounts{}, nil
	}
	return parsePartitionCounts(vals[0])
}

// invokeWithPartitionCounts wraps a partition-aware matchingservice RPC call:
// - attaches the client's cached counts to the outgoing request (as header)
// - updates the cache from the server's response (trailer)
// - retries once if it receives StalePartitionCounts error
func invokeWithPartitionCounts[Req, Res any](
	ctx context.Context,
	logger log.Logger,
	cache *partitionCache,
	pkey string,
	kind enumspb.TaskQueueKind,
	request Req,
	opts []grpc.CallOption,
	op func(
		ctx context.Context,
		pc PartitionCounts,
		request Req,
		opts []grpc.CallOption,
	) (Res, error),
) (Res, error) {
	if kind != enumspb.TASK_QUEUE_KIND_NORMAL {
		// only normal partitions participate in scaling
		return op(ctx, PartitionCounts{}, request, opts)
	}

	// capture trailer
	var trailer metadata.MD
	opts = append(slices.Clone(opts), grpc.Trailer(&trailer))

	// get current idea of partition counts. if missing from the cache, this will send zeros
	// for counts, which the server will always accept as not-stale.
	pc := cache.lookup(pkey)

	for attempt := 0; ; attempt++ {
		res, err := op(pc.appendToOutgoingContext(ctx), pc, request, opts)

		// update cache on trailer on both success and error. if the trailer has no data,
		// this removes the key from the cache.
		newPc, parseErr := parsePartitionCountsFromTrailer(trailer)
		trailer = nil
		if parseErr != nil {
			logger.Info("partition count trailer parse error", tag.Error(parseErr))
			// continue with zero value for newPc
		}
		if newPc != pc {
			cache.put(pkey, newPc)
			pc = newPc
		}

		if _, ok := errors.AsType[*serviceerrors.StalePartitionCounts](err); ok && attempt == 0 {
			// if we got a StalePartitionCounts on the first attempt, retry once
			continue
		}

		return res, err
	}
}
