package counter

import "time"

type Counter interface {
	// GetPass tracks a value per key, which may be lossy or approximate. It increments the
	// value by inc and returns the new value. The value returned must be >= base.
	GetPass(key string, base, inc int64) int64
	// EstimateDistinctKeys returns an estimate of the number of distinct keys in the counter.
	EstimateDistinctKeys() int
	// Reseed should be called periodically with the current time to reseed the counter.
	Reseed(now time.Time)
}
