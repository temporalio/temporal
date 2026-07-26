//go:build !test_dep

package interceptor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
)

func TestNewNonNilResponseInterceptorNoop(t *testing.T) {
	require.Nil(t, NewNonNilResponseInterceptor(log.NewNoopLogger()))
}
