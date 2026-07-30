package nexus

import (
	"net/url"
	"strconv"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/proto"
)

// FormatDuration converts a duration into a string representation in millisecond resolution.
// TODO: replace this with the version exported from the Nexus SDK
func FormatDuration(d time.Duration) string {
	return strconv.FormatInt(d.Milliseconds(), 10) + "ms"
}

// ConvertLinksToProto converts Nexus SDK links to protobuf links.
func ConvertLinksToProto(links []nexus.Link) []*nexuspb.Link {
	if links == nil {
		return nil
	}
	result := make([]*nexuspb.Link, len(links))
	for i, link := range links {
		result[i] = &nexuspb.Link{
			Url:  link.URL.String(),
			Type: link.Type,
		}
	}
	return result
}

// ConvertLinksFromProto converts protobuf links to Nexus SDK links.
func ConvertLinksFromProto(links []*nexuspb.Link) []nexus.Link {
	if links == nil {
		return nil
	}
	result := make([]nexus.Link, len(links))
	for i, link := range links {
		u, _ := url.Parse(link.Url)
		result[i] = nexus.Link{
			URL:  u,
			Type: link.Type,
		}
	}
	return result
}

func ValidatePropagatedSerializationContext(
	existing *commonpb.PropagatedNexusSerializationContext,
	requested *commonpb.PropagatedNexusSerializationContext,
	resource string,
) error {
	if proto.Equal(existing, requested) {
		return nil
	}
	return serviceerror.NewFailedPreconditionf(
		"propagated nexus serialization context must match the existing %s",
		resource,
	)
}
