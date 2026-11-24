package otherawait

import (
	"testing"
	"time"

	"go.temporal.io/testx/await"
)

type T await.T

func RequireTrue(tb testing.TB, ready func() bool, timeout, interval time.Duration) {
	await.RequireTrue(tb, ready, timeout, interval)
}
