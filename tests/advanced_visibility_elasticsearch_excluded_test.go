//go:build temporal_minimal && !temporal_with_elasticsearch

package tests

import (
	"go.temporal.io/server/tests/testcore"
)

func (s *AdvancedVisibilitySuite) updateMaxResultWindow(*testcore.TestEnv) {
	s.FailNow("Elasticsearch visibility is not compiled into this build; rebuild with the temporal_with_elasticsearch build tag")
}
