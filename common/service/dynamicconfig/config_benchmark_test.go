package dynamicconfig

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/temporalio/temporal/common/log"
)

func BenchmarkGetIntProperty(b *testing.B) {
	client := newInMemoryClient()
	cln := NewCollection(client, log.NewNoop())
	key := MatchingMaxTaskBatchSize
	for i := 0; i < b.N; i++ {
		size := cln.GetIntProperty(key, 10)
		assert.Equal(b, 10, size())
	}
}
