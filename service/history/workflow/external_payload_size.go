package workflow

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/proxy"
)

// CalculateExternalPayloadSize calculates the total size and count of all external payloads in the given history events.
func CalculateExternalPayloadSize(events []*historypb.HistoryEvent) (size int64, count int64, err error) {
	var totalSize int64
	var totalCount int64

	visitor := func(vpc *proxy.VisitPayloadsContext, payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
		for _, p := range payloads {
			totalCount += int64(len(p.ExternalPayloads))
			for _, extPayload := range p.ExternalPayloads {
				totalSize += extPayload.SizeBytes
			}
		}
		return payloads, nil
	}

	for _, event := range events {
		err := proxy.VisitPayloads(context.Background(), event, proxy.VisitPayloadsOptions{
			Visitor:              visitor,
			SkipSearchAttributes: true,
		})
		if err != nil {
			return 0, 0, err
		}
	}

	return totalSize, totalCount, nil
}
