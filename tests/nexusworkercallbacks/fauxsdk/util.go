package fauxsdk

import (
	"go.temporal.io/server/common/payload"

	commonpb "go.temporal.io/api/common/v1"
)

func mustBuildPayloads(v any) *commonpb.Payloads {
	p, err := payload.Encode(v)
	if err != nil {
		panic(err.Error())
	}
	return &commonpb.Payloads{
		Payloads: []*commonpb.Payload{p},
	}
}
