package mixedbrain

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"
)

const (
	defaultProcessChaosInterval = time.Minute
	processChaosIntervalEnv     = "MIXED_BRAIN_PROCESS_CHAOS_INTERVAL"
	clusterReformationTimeout   = 90 * time.Second
)

type restartTarget interface {
	Restart(context.Context) error
}

type processChaosTarget struct {
	name   string
	target restartTarget
}

type processChaosEvent struct {
	Target      string
	StartedAt   time.Time
	RestartedAt time.Time
	ReformedAt  time.Time
	Err         string
}

type chaosEvents struct {
	mu     sync.Mutex
	events []processChaosEvent
}

func (e *chaosEvents) append(event processChaosEvent) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.events = append(e.events, event)
	return len(e.events) - 1
}

func (e *chaosEvents) update(index int, update func(*processChaosEvent)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	update(&e.events[index])
}

func (e *chaosEvents) snapshot() []processChaosEvent {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]processChaosEvent(nil), e.events...)
}

func configuredProcessChaosInterval() (time.Duration, error) {
	raw := os.Getenv(processChaosIntervalEnv)
	if raw == "" {
		return defaultProcessChaosInterval, nil
	}
	interval, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid %s %q: %w", processChaosIntervalEnv, raw, err)
	}
	if interval <= 0 {
		return 0, fmt.Errorf("%s must be positive, got %q", processChaosIntervalEnv, raw)
	}
	return interval, nil
}

func runProcessChaos(
	ctx context.Context,
	interval time.Duration,
	waitForReformation func(context.Context) error,
	events *chaosEvents,
	targets ...processChaosTarget,
) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	return runProcessChaosTicks(ctx, ticker.C, rand.Intn, waitForReformation, events, targets...)
}

func runProcessChaosTicks(
	ctx context.Context,
	ticks <-chan time.Time,
	chooseTarget func(int) int,
	waitForReformation func(context.Context) error,
	events *chaosEvents,
	targets ...processChaosTarget,
) error {
	if len(targets) == 0 {
		return fmt.Errorf("process chaos needs at least one target")
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case startedAt, ok := <-ticks:
			if !ok {
				return nil
			}
			target := targets[chooseTarget(len(targets))]
			index := events.append(processChaosEvent{Target: target.name, StartedAt: startedAt})
			if err := target.target.Restart(ctx); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				events.update(index, func(event *processChaosEvent) { event.Err = err.Error() })
				return fmt.Errorf("restart %s: %w", target.name, err)
			}
			events.update(index, func(event *processChaosEvent) { event.RestartedAt = time.Now() })
			if err := waitForReformation(ctx); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				events.update(index, func(event *processChaosEvent) { event.Err = err.Error() })
				return fmt.Errorf("wait after restarting %s: %w", target.name, err)
			}
			events.update(index, func(event *processChaosEvent) { event.ReformedAt = time.Now() })
		}
	}
}
