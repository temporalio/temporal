package health

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
)

type (
	HostHealthAggregator struct {
		lifetime          context.Context
		checkables        map[string]Checkable
		start             sync.Once
		latestObservation atomic.Pointer[AggregateHealth]
		// requiredChecks will generate an error entry if there isn't a health
		// check reported in the final result with the same CheckName
		requiredChecks []string
	}
	AggregateHealth struct {
		state  enumsspb.HealthState
		checks []HealthCheck
	}
	// The singleHealthCheckWrapper enables the convenience function AddCheck
	// so that single health checks can be added without creating a struct/component for them.
	singleHealthCheckWrapper struct {
		name string
		fn   func() HealthCheck
	}
)

func (h *HostHealthAggregator) GetHealth() AggregateHealth {
	return *h.latestObservation.Load()
}

func (h *HostHealthAggregator) Start() {
	h.start.Do(func() {
		go h.continuouslyMonitor()
	})
}

// continuouslyMonitor pulls health check results from all registered checkables
func (h *HostHealthAggregator) continuouslyMonitor() {
	ticker := time.NewTicker(2 * time.Second)
	for {
		// Make a guess at initial capacity for the health check slice
		observations := make([]HealthCheck, len(h.checkables)+2*len(h.checkables))
		aggregateState := enumsspb.HEALTH_STATE_UNSPECIFIED
		for name, observer := range h.checkables {
			observations = append(observations, safeObserve(name, observer)...)
		}
		checkValidation := make(map[string]bool, len(h.checkables))
		for _, reqd := range h.requiredChecks {
			checkValidation[reqd] = false
		}
		for _, check := range observations {
			checkValidation[check.CheckName] = true
		}
		for check, present := range checkValidation {
			if !present {
				aggregateState = enumsspb.HEALTH_STATE_INTERNAL_ERROR
				observations = append(observations, HealthCheck{
					CheckName: fmt.Sprintf("%s-unregistered", check),
					State:     enumsspb.HEALTH_STATE_INTERNAL_ERROR,
					Message:   fmt.Sprintf("required checkable %s not registered", check),
				})
			}
		}
		h.latestObservation.Store(&AggregateHealth{
			state:  aggregateState,
			checks: observations,
		})
		select {
		case <-ticker.C:
		case <-h.lifetime.Done():
			ticker.Stop()
			return
		}
	}
}

// safeObserve converts panics to normal health check errors
func safeObserve(name string, observer Checkable) (checkResult []HealthCheck) {
	defer func() {
		if r := recover(); r != nil {
			checkResult = []HealthCheck{{
				CheckName: fmt.Sprintf("%s-failure", name),
				State:     enumsspb.HEALTH_STATE_INTERNAL_ERROR,
				Message:   fmt.Sprintf("panic'ed when trying to observe: %s", name),
			}}
		}
	}()
	checkResult = observer.CheckHealth()
	return
}

// AddCheck adds a single health check to the aggregator.
// This is a convenience function for adding a single health check, and it generates
// a struct per-function registered. If you're calling this multiple times from the same
// struct, implement Checkable and pass the struct to AddObserver.
func (h *HostHealthAggregator) AddCheck(name string, check func() HealthCheck) {
	h.checkables[name] = singleHealthCheckWrapper{fn: check}
}

func (h *HostHealthAggregator) AddObserver(observer Checkable) {
	h.checkables[observer.NameForHealthReporting()] = observer
}

func (w singleHealthCheckWrapper) CheckHealth() []HealthCheck {
	return []HealthCheck{w.fn()}
}

func (w singleHealthCheckWrapper) NameForHealthReporting() string {
	return w.name
}
