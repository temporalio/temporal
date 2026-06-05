// Package callerprop defines the seams for carrying verified caller identity
// across the cross-namespace Nexus HTTP hop. The OSS server provides only no-op
// implementations: in OSS, identity is attributed and stored locally (Principal
// Attribution + CHASM callers) but is NOT propagated across the Nexus HTTP
// boundary — that propagation is a Cloud concern and lives in saas-temporal,
// which injects real implementations via fx.Decorate.
package callerprop

import (
	"context"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	"go.uber.org/fx"
)

// OutboundDecorator adds the verified caller to an outbound Nexus request (the
// caller-side HTTP dispatch). Implementations encode the caller in a way the
// peer can verify (e.g. a signed token). The OSS default is a no-op.
type OutboundDecorator interface {
	Decorate(ctx context.Context, header nexus.Header, caller *commonpb.Caller) error
}

// InboundExtractor returns the verified caller carried on an inbound Nexus
// request (the handler-side HTTP ingress), or nil when none is trusted.
// Implementations establish trust (signature or peer) before returning the
// caller; they must never return a caller from an unverified source. The OSS
// default returns nil.
type InboundExtractor interface {
	Extract(ctx context.Context, header nexus.Header) *commonpb.Caller
}

// NoopOutbound is the OSS default: it propagates nothing across the hop.
type NoopOutbound struct{}

func (NoopOutbound) Decorate(context.Context, nexus.Header, *commonpb.Caller) error { return nil }

// NoopInbound is the OSS default: no caller is extracted from the inbound hop.
type NoopInbound struct{}

func (NoopInbound) Extract(context.Context, nexus.Header) *commonpb.Caller { return nil }

// Module provides the OSS no-op seams. saas-temporal overrides both with
// fx.Decorate to enable cross-namespace propagation.
var Module = fx.Module(
	"common.nexus.callerprop",
	fx.Provide(func() OutboundDecorator { return NoopOutbound{} }),
	fx.Provide(func() InboundExtractor { return NoopInbound{} }),
)
