package ringpop

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/primitives"
)

// TestEmitMembershipGauges verifies that emitMembershipGaugesFromHosts records the
// reachable/available/draining member counts, tagged with the service name.
func TestEmitMembershipGauges(t *testing.T) {
	r := require.New(t)

	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	defer handler.StopCapture(capture)

	resolver := &serviceResolver{
		service:        primitives.HistoryService,
		metricsHandler: handler,
	}

	// 3 reachable hosts: two accepting traffic, one draining.
	hosts := []*hostInfo{
		newHostInfo("10.0.0.1:7234", map[string]string{}),
		newHostInfo("10.0.0.2:7234", map[string]string{}),
		newHostInfo("10.0.0.3:7234", map[string]string{drainingKey: "true"}),
	}

	resolver.emitMembershipGaugesFromHosts(hosts)

	recordings := capture.Snapshot()

	assertGauge := func(name string, want float64) {
		recs, ok := recordings[name]
		r.True(ok, "expected a recording for %s", name)
		r.Len(recs, 1, "expected exactly one recording for %s", name)
		r.Equal(want, recs[0].Value, "unexpected value for %s", name)
		r.Equal(string(primitives.HistoryService), recs[0].Tags[metrics.ServiceNameTag(primitives.HistoryService).Key],
			"expected service name tag on %s", name)
	}

	assertGauge("membership_reachable_members", float64(3))
	assertGauge("membership_available_members", float64(2))
	assertGauge("membership_draining_members", float64(1))
}

// TestEmitMembershipGaugesNilHandlerNoPanic verifies the emit is a safe no-op
// when no metrics handler is configured.
func TestEmitMembershipGaugesNilHandlerNoPanic(t *testing.T) {
	resolver := &serviceResolver{
		service:        primitives.HistoryService,
		metricsHandler: nil,
	}
	resolver.emitMembershipGaugesFromHosts([]*hostInfo{
		newHostInfo("10.0.0.1:7234", map[string]string{}),
	})
}
