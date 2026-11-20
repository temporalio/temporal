package await

import (
	"context"
	"testing"
	"time"

	"go.temporal.io/testx/await"
)

func Requiref(context.Context, testing.TB, func(*await.T), time.Duration, time.Duration, string, ...any) {
}
