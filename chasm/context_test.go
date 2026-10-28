package chasm

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
)

func TestMutableContextSetTimeSkippingConfigNil(t *testing.T) {
	t.Parallel()

	config := &commonpb.TimeSkippingConfig{Enabled: true}
	backend := &MockNodeBackend{
		HandleSetTimeSkippingConfig: func(next *commonpb.TimeSkippingConfig) {
			config = next
		},
	}
	ctx := NewMutableContext(context.Background(), &Node{nodeBase: &nodeBase{backend: backend}})

	ctx.SetTimeSkippingConfig(nil)

	require.Nil(t, config)
}
