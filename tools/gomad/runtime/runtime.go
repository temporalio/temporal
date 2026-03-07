// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package sim_runtime

import (
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/petermattis/goid"
	"go.temporal.io/server/tools/gomad/util/verify"
)

// modelled after https://www.risingwave.com/blog/deterministic-simulation-a-new-era-of-distributed-system-testing/

const (
	DebugModeEnvKey         = "GOMAD_DEBUG"
	RemoteControlAddrEnvKey = "GOMAD_REMOTE_CONTROL_ADDR"
	RemoteControlIdEnvKey   = "GOMAD_REMOTE_CONTROL_ID"
	SeedEnvKey              = "GOMAD_SEED"

	RemoteControlPauseCmd    = "<<< pause >>>"
	RemoteControlContinueCmd = "<<< cont >>>"
)

// goroutineRegistration records the simulator (and optionally the cooperative
// goroutine struct) for one OS goroutine in the registry.
type goroutineRegistration struct {
	sim *simulator
	// g is non-nil only for goroutines owned by the cooperative scheduler.
	// It is nil for the "main" goroutine that called Start() and for the
	// loop goroutine — both are registered for trySim() access but are not
	// cooperative scheduler participants.
	g *goroutine
}

var (
	// registry maps OS goroutine ID → *goroutineRegistration for all registered goroutines.
	registry sync.Map

	// globalSim is a fallback for goroutines not explicitly registered (e.g. goroutines
	// spawned by the Go testing framework for parallel subtests). Set by Start().
	globalSim atomic.Pointer[simulator]

	DebugMode = initOptionFunc(func(s *simulator) {
		s.debug = true
	})
	Seed = func(seed int64) initOptionFunc {
		return func(s *simulator) {
			s.seed = seed
		}
	}
	// Hook provides a function that is invoked after every step.
	// It can decide to stop the execution.
	Hook = func(h HookFunc) initOptionFunc {
		return func(s *simulator) {
			s.hook = h
		}
	}
)

func currentSim() *simulator {
	s := trySim()
	if s != nil {
		return s
	}
	// dump registry contents for diagnosis
	id := goid.Get()
	var keys []int64
	registry.Range(func(k, _ any) bool {
		keys = append(keys, k.(int64))
		return true
	})
	panic("currentSim: no simulator registered for goroutine " +
		strconv.FormatInt(id, 10) +
		"; registry keys: " + fmt.Sprintf("%v", keys))
}

// trySim returns the simulator for the current goroutine, or nil if not in a simulation.
// Used by debug helpers that must be safe to call from any goroutine.
func trySim() *simulator {
	v, ok := registry.Load(goid.Get())
	if !ok {
		return nil
	}
	return v.(*goroutineRegistration).sim
}

// IsCooperativeGoroutine reports whether the calling OS goroutine is currently
// running as a cooperative goroutine managed by the simulator's scheduler.
func IsCooperativeGoroutine() bool {
	return tryCurrentGoroutine() != nil
}

// tryCurrentGoroutine returns the cooperative-scheduler goroutine for the
// current OS goroutine, or nil if the caller is not a scheduler-managed
// goroutine (e.g. the main goroutine that called Start() or a tRunner goroutine).
func tryCurrentGoroutine() *goroutine {
	v, ok := registry.Load(goid.Get())
	if !ok {
		return nil
	}
	return v.(*goroutineRegistration).g
}

// tryAnySimulator returns a simulator if any simulation is active, regardless of whether
// the calling goroutine is registered. Used by functions that access shared simulator
// state (e.g. virtual network listeners) from goroutines not managed by the scheduler
// (e.g. testing framework goroutines). Callers must NOT yield or suspend.
func tryAnySimulator() *simulator {
	if s := trySim(); s != nil {
		return s
	}
	return globalSim.Load()
}

// registerSim records s for the calling goroutine without associating a
// cooperative-scheduler goroutine struct.  Used by Start() and the loop goroutine.
func registerSim(s *simulator) {
	registry.Store(goid.Get(), &goroutineRegistration{sim: s})
}

// registerSimGoroutine records both s and g for the calling goroutine.
// Called from goroutine.spawn() to mark the OS goroutine as a cooperative
// scheduler participant.
func registerSimGoroutine(s *simulator, g *goroutine) {
	registry.Store(goid.Get(), &goroutineRegistration{sim: s, g: g})
}

func deregisterSim() {
	registry.Delete(goid.Get())
}

type (
	simulator struct {
		// config options
		seed  int64
		debug bool

		info      Info
		hook      HookFunc
		hookCh    chan struct{}
		scheduler *scheduler
		Drng      *rand.Rand

		panicErr string
		idSeq    map[string]uint64 // used to generate goroutine/channel ids
		State    map[string]any    // used to store arbitrary state
		// StateMu protects State and idSeq from concurrent access by non-cooperative
		// goroutines (e.g. testing.tRunner goroutines for parallel subtests).
		// Must NOT be held across suspend() calls.
		StateMu sync.Mutex

		// nativeTimes maps OS goroutine ID (goid.Get()) to the scheduler's
		// clock.now value at the moment the most recent delegation goroutine
		// completed for that native goroutine. Native goroutines read Time()
		// from this snapshot to avoid racing with concurrent clock advances.
		nativeTimes sync.Map

		done        chan struct{}
		abortCh     chan struct{}
		keepAliveCh chan struct{} // closed by Join() to signal that no more work is expected
		stopped     atomic.Bool
	}
	InitOption interface {
		apply(*simulator)
	}
	initOptionFunc func(*simulator)
	HookFunc       func(info *Info)
	// Info tracks simulator execution details; it can be modified by the client to influence execution.
	Info struct {
		Steps          int64         // the number of iterations the simulator ran through
		Stop           chan struct{} // can be set by the client to indicate they want the simulator to stop
		stopAtTimeSkip bool          // run until there are no more events for the current time
	}
)

// Simulator is the exported alias for the simulator type, allowing packages
// in api/lib and api/lang to reference it by name.
type Simulator = simulator

// Start launches a new simulator for the calling goroutine.
// Multiple simulations can run concurrently in the same process.
func Start(opts ...InitOption) {
	// collect option functions from environment variables (no side effects)
	if debugMode, _ := os.LookupEnv(DebugModeEnvKey); debugMode == "true" {
		opts = append(opts, DebugMode)
	}
	if seed, _ := os.LookupEnv(SeedEnvKey); seed != "" {
		parsedSeed, err := strconv.ParseInt(seed, 10, 64)
		verify.T(err == nil, "failed to parse seed from environment variable: %v", err)
		opts = append(opts, Seed(parsedSeed))
	}
	var remoteCtrlAddr, remoteClientId string
	if addr, _ := os.LookupEnv(RemoteControlAddrEnvKey); addr != "" {
		clientId, _ := os.LookupEnv(RemoteControlIdEnvKey)
		verify.T(strings.TrimSpace(clientId) != "", "missing/empty remote ctrl client id")
		remoteCtrlAddr = addr
		remoteClientId = clientId
	}

	keepAliveCh := make(chan struct{})

	// create new simulator and apply init options
	s := &simulator{
		done:        make(chan struct{}),
		abortCh:     make(chan struct{}),
		keepAliveCh: keepAliveCh,
		State:       make(map[string]any),
		idSeq:       make(map[string]uint64),
		info:        Info{Stop: make(chan struct{})},
		hook:        func(info *Info) {}, // noop
		hookCh:      make(chan struct{}, 1),
		seed:        rand.Int63(), // always generate a fresh seed (user might override this later)
	}
	for _, opt := range opts {
		opt.apply(s)
	}

	// This pseudo-random number generator produces all random factors in the system,
	// including random numbers, delays, and errors introduced by the simulator.
	// With the same random seed, the same execution sequence can be produced.
	s.Drng = rand.New(rand.NewSource(s.seed))

	// create new scheduler
	s.scheduler = initScheduler(s.Drng, s.debug, keepAliveCh)

	// Register the calling goroutine so that trySim() / CurrentSimulator() work for it.
	// It is NOT added to the cooperative scheduler; blocking simulation operations
	// called from this goroutine go through the delegation path in channel.go / select.go.
	registerSim(s)

	// expose as global fallback for goroutines not explicitly registered (e.g. testing framework)
	globalSim.Store(s)

	// setup remote ctrl after registration (newRemoteCtrl may call Dbg())
	if remoteCtrlAddr != "" {
		hook := newRemoteCtrl(remoteCtrlAddr, remoteClientId)
		s.hook = hook.hook
	}

	Print("⭐", "init", AnyTag("seed", s.seed))

	// start simulator's main loop in its own registered goroutine
	errChan := make(chan interface{}, 1)
	go func() {
		registerSim(s)
		loop(s, errChan)
	}()
}

func loop(s *simulator, errChan chan interface{}) {
	defer func() {
		// Deregister the loop goroutine BEFORE closing s.done.
		// If deregisterSim ran after close(s.done), Join() could unblock, the next
		// simulation could start on a recycled goroutine ID, and this deferred delete
		// would then remove the new simulation's registry entry.
		deregisterSim()

		// mark simulator as stopped (which prevents a deadlock if Stop is invoked more than once)
		s.stopped.Store(true)

		// signal that simulator loop is done
		close(s.done)
	}()

	defer func() {
		if err := recover(); err != nil {
			errChan <- err
		}
	}()

	// listen for process shutdown signal
	interruptCh := make(chan os.Signal, 2)
	signal.Notify(interruptCh, os.Interrupt, os.Kill)

	for {
		s.info.Steps += 1
		s.hook(&s.info)

		stepsTag := AnyTag("step", s.info.Steps)
		Dbg("♻️", "   ", stepsTag)

		// Run tick in a goroutine so the select can race it against other
		// signals (abort, interrupt, done).  The stuckTimeout inside tick()
		// itself will panic if a cooperative goroutine blocks natively.
		tickDone := make(chan chan struct{}, 1)
		go func() { tickDone <- s.scheduler.tick() }()
		var tickCh chan struct{}
		select {
		case tickCh = <-tickDone:
			// tick completed; fall through to receive on tickCh below
		case <-s.abortCh:
			Dbg("🛑️", "   ",
				stepsTag,
				AnyTag("reason", "abort"))
			return
		case <-interruptCh:
			Dbg("🛑️", "   ",
				stepsTag,
				AnyTag("reason", "sigkill"))
			return
		case <-s.info.Stop:
			Dbg("🛑️", "   ",
				stepsTag,
				AnyTag("reason", "hook"))
			return
		case <-s.scheduler.doneCh:
			Dbg("🛑️", "   ",
				stepsTag,
				AnyTag("reason", "done"))
			return
		}
		// tickCh is either tickCh (already buffered) or wakeUpCh (idle, waiting
		// for new work).  Race it against the same shutdown signals.
		// When all goroutines are blocked (tickCh == wakeUpCh with blocked goroutines),
		// apply a timeout: non-coop goroutines may still be running and about to add
		// work; if nothing arrives within stuckTimeout, it's a true deadlock.
		deadlineTimer := time.NewTimer(s.scheduler.stuckTimeout)
		select {
		case <-tickCh:
			// regular scheduler tick or wakeup
		case <-s.abortCh:
			deadlineTimer.Stop()
			Dbg("🛑️", "   ", stepsTag, AnyTag("reason", "abort"))
			return
		case <-interruptCh:
			deadlineTimer.Stop()
			Dbg("🛑️", "   ", stepsTag, AnyTag("reason", "sigkill"))
			return
		case <-s.info.Stop:
			deadlineTimer.Stop()
			Dbg("🛑️", "   ", stepsTag, AnyTag("reason", "hook"))
			return
		case <-s.scheduler.doneCh:
			deadlineTimer.Stop()
			Dbg("🛑️", "   ", stepsTag, AnyTag("reason", "done"))
			return
		case <-deadlineTimer.C:
			if blocked := s.scheduler.blockedGoroutines; len(blocked) > 0 {
				panic(fmt.Sprintf("deadlock: all goroutines are blocked\n\t%v",
					strings.Join(blocked, "\n\t")))
			}
			// Timer fired while idle (no blocked goroutines); continue normally.
		}
		deadlineTimer.Stop()
	}
}

func Join() {
	s := trySim()
	if s == nil {
		// already joined (deregistered by a previous Join() call on this goroutine)
		return
	}

	// Signal the scheduler that no more work is expected.  The scheduler will
	// exit cleanly once all in-flight goroutines finish.
	close(s.keepAliveCh)

	// Wake up the scheduler loop in case it is currently idle (waiting on wakeUpCh).
	select {
	case s.scheduler.wakeUpCh <- struct{}{}:
	default:
	}

	// wait for simulator loop to finish, then deregister
	<-s.done
	deregisterSim()
	globalSim.Store(nil)
}

func (f initOptionFunc) apply(s *simulator) {
	f(s)
}

func nextId(s *simulator, ns string) uint64 {
	s.StateMu.Lock()
	defer s.StateMu.Unlock()
	if _, ok := s.idSeq[ns]; !ok {
		s.idSeq[ns] = 0
	}
	s.idSeq[ns] += 1
	return s.idSeq[ns]
}

func Time() time.Time {
	s := trySim()
	if s == nil {
		return time.Now() // pre-simulator fallback (e.g., package init)
	}
	// For native goroutines (not managed by the cooperative scheduler), return
	// the clock snapshot captured when the last delegation goroutine completed.
	// This prevents a race where the scheduler advances clock.now after a DG
	// completes but before the native goroutine reads the time.
	if tryCurrentGoroutine() == nil {
		if t, ok := s.nativeTimes.Load(goid.Get()); ok {
			return time.UnixMilli(t.(int64)).UTC()
		}
	}
	return s.scheduler.time()
}

func Sleep(d time.Duration) {
	CurrentSimulator().scheduler.sleep(d)
}

func CurrentSimulator() *simulator {
	return currentSim()
}

// TryCurrentSimulator returns the current simulator, or nil if no simulator is
// registered for this goroutine. Safe to call before the simulation starts.
func TryCurrentSimulator() *simulator {
	return trySim()
}

// TryAnySimulator returns the active simulator regardless of whether the calling
// goroutine is registered in the scheduler. Safe for unregistered goroutines
// (e.g. testing framework goroutines) to access shared simulator state.
// Callers MUST NOT yield or suspend the scheduler.
func TryAnySimulator() *simulator {
	return tryAnySimulator()
}
