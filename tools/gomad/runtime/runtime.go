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

	// activeSimulators tracks all running simulators. Used as a fallback for
	// goroutines not explicitly registered (e.g. testing framework goroutines).
	// When exactly one simulator is active, tryAnySimulator() returns it.
	// When multiple are active, the goroutine must be registered explicitly.
	activeSimulators sync.Map // *simulator → struct{}

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
	// Fallback: return the active simulator if exactly one is running.
	// This covers testing framework goroutines not explicitly registered.
	var found *simulator
	var count int
	activeSimulators.Range(func(k, _ any) bool {
		found = k.(*simulator)
		count++
		return count < 2 // stop early if more than one
	})
	if count == 1 {
		return found
	}
	if count > 1 {
		panic("multiple simulators active; goroutine must be registered via Start()")
	}
	return nil
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
		drngSrc   *countingSource // backing source for Drng; enables state snapshots

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

		// checkpoint/replay support
		recording    bool
		opLog        map[goroutineId][]logEntry
		replayLog    map[goroutineId][]logEntry
		replayIdx    map[goroutineId]int // current replay position per goroutine
		goroutineFns map[goroutineId]func()

		// pendingChanRestores holds channel buffer snapshots from a checkpoint,
		// keyed by native channel pointer. Applied lazily in GetOrCreateChan
		// when the channel is first accessed after a restore.
		pendingChanRestores map[uintptr]channelSnapshot
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
	s.Drng, s.drngSrc = newDrng(s.seed)

	// create new scheduler
	s.scheduler = initScheduler(s.Drng, s.debug, keepAliveCh)

	// Register the calling goroutine so that trySim() / CurrentSimulator() work for it.
	// It is NOT added to the cooperative scheduler; blocking simulation operations
	// called from this goroutine go through the delegation path in channel.go / select.go.
	registerSim(s)

	// track as active for tryAnySimulator() fallback (e.g. testing framework goroutines)
	activeSimulators.Store(s, struct{}{})

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
	activeSimulators.Delete(s)
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
	s := CurrentSimulator()
	g := tryCurrentGoroutine()

	// replay: skip the actual sleep
	if g != nil && s.isReplaying(g.id) {
		if entry, ok := s.nextReplayEntry(g.id); ok && entry.op == logSleep {
			return
		}
	}

	s.scheduler.sleep(d)

	if s.recording && g != nil {
		s.appendLog(g.id, logEntry{op: logSleep, result: struct{}{}})
	}
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

// StartRecording enables operation logging for checkpoint support.
// Must be called from outside the cooperative scheduler (e.g. from the test goroutine).
func StartRecording() {
	s := currentSim()
	s.recording = true
	s.opLog = make(map[goroutineId][]logEntry)
	s.goroutineFns = make(map[goroutineId]func())
}

// appendLog records an operation result for the given goroutine.
func (s *simulator) appendLog(id goroutineId, e logEntry) {
	s.StateMu.Lock()
	defer s.StateMu.Unlock()
	s.opLog[id] = append(s.opLog[id], e)
}

// nextReplayEntry returns the next log entry for replay, or false if the
// goroutine's log is exhausted (transition to normal evaluation).
func (s *simulator) nextReplayEntry(id goroutineId) (logEntry, bool) {
	s.StateMu.Lock()
	defer s.StateMu.Unlock()
	log := s.replayLog[id]
	idx := s.replayIdx[id]
	if idx >= len(log) {
		return logEntry{}, false
	}
	e := log[idx]
	s.replayIdx[id] = idx + 1
	// also append to opLog so that future checkpoints include replayed entries
	if s.recording {
		s.opLog[id] = append(s.opLog[id], e)
	}
	return e, true
}

// isReplaying reports whether the given goroutine still has log entries to replay.
func (s *simulator) isReplaying(id goroutineId) bool {
	if s.replayLog == nil {
		return false
	}
	s.StateMu.Lock()
	defer s.StateMu.Unlock()
	log := s.replayLog[id]
	idx := s.replayIdx[id]
	return idx < len(log)
}

// TakeCheckpoint captures the current simulation state. Must be called from a
// cooperative goroutine. The simulation continues normally after this call.
func TakeCheckpoint() *Checkpoint {
	s := currentSim()
	g := tryCurrentGoroutine()
	if g == nil {
		panic("TakeCheckpoint must be called from a cooperative goroutine")
	}
	if !s.recording {
		panic("TakeCheckpoint requires recording to be enabled (call StartRecording first)")
	}

	s.StateMu.Lock()

	// deep-copy the operation log
	logCopy := make(map[goroutineId][]logEntry, len(s.opLog))
	for id, entries := range s.opLog {
		cp := make([]logEntry, len(entries))
		copy(cp, entries)
		logCopy[id] = cp
	}

	// deep-copy goroutine fns
	fnsCopy := make(map[goroutineId]func(), len(s.goroutineFns))
	for id, fn := range s.goroutineFns {
		fnsCopy[id] = fn
	}

	// deep-copy idSeq
	idSeqCopy := make(map[string]uint64, len(s.idSeq))
	for k, v := range s.idSeq {
		idSeqCopy[k] = v
	}

	// shallow-copy state (callers needing deep semantics must handle it)
	stateCopy := make(map[string]any, len(s.State))
	for k, v := range s.State {
		stateCopy[k] = v
	}

	s.StateMu.Unlock()

	// snapshot DRNG state
	drngSnap := s.drngSrc.snapshot()
	drngSnap.seed = s.seed

	// snapshot channel buffers
	s.scheduler.channelsMu.Lock()
	chanBufs := make(map[uintptr]channelSnapshot, len(s.scheduler.channels))
	for key, ch := range s.scheduler.channels {
		chanBufs[key] = snapshotChannel(ch)
	}
	s.scheduler.channelsMu.Unlock()

	// snapshot goroutines
	var snaps []goroutineSnap
	for id, sg := range s.scheduler.goroutines {
		snaps = append(snaps, goroutineSnap{
			id:       id,
			internal: sg.internal,
			logLen:   len(logCopy[id]),
		})
	}

	return &Checkpoint{
		log:         logCopy,
		clock:       s.scheduler.clock.now,
		drngState:   drngSnap,
		channelBufs: chanBufs,
		goroutines:  snaps,
		fns:         fnsCopy,
		idSeq:       idSeqCopy,
		state:       stateCopy,
	}
}

// snapshotChannel extracts the buffered messages from a Channel interface.
func snapshotChannel(ch Channel) channelSnapshot {
	type bufferAccessor interface {
		SnapshotBuf() (buf []any, capacity int, closed bool)
	}
	if ba, ok := ch.(bufferAccessor); ok {
		buf, cap, closed := ba.SnapshotBuf()
		return channelSnapshot{buf: buf, cap: cap, closed: closed}
	}
	return channelSnapshot{}
}

// RestoreFrom stops the current simulation (if any) and creates a fresh one
// from the checkpoint. Goroutines are re-spawned and replay their operation
// logs at full speed (no suspend/scheduling). Once a goroutine exhausts its
// log it transitions to normal cooperative evaluation.
//
// Must be called from the goroutine that called Start() (the "main" goroutine).
func RestoreFrom(cp *Checkpoint) {
	// stop current simulation if one is active
	if s := trySim(); s != nil {
		if !s.stopped.Load() {
			select {
			case s.abortCh <- struct{}{}:
			default:
			}
			<-s.done
		}
		deregisterSim()
		activeSimulators.Delete(s)
	}

	keepAliveCh := make(chan struct{})

	// restore DRNG
	drng, drngSrc := restoreDrng(cp.drngState.seed, cp.drngState)

	// create a fresh simulator with checkpoint state
	s := &simulator{
		seed:         cp.drngState.seed,
		done:         make(chan struct{}),
		abortCh:      make(chan struct{}),
		keepAliveCh:  keepAliveCh,
		info:         Info{Stop: make(chan struct{})},
		hook:         func(info *Info) {},
		hookCh:       make(chan struct{}, 1),
		Drng:         drng,
		drngSrc:      drngSrc,
		recording:    true,
		goroutineFns: make(map[goroutineId]func(), len(cp.fns)),
	}

	// restore idSeq
	s.idSeq = make(map[string]uint64, len(cp.idSeq))
	for k, v := range cp.idSeq {
		s.idSeq[k] = v
	}

	// restore state
	s.State = make(map[string]any, len(cp.state))
	for k, v := range cp.state {
		s.State[k] = v
	}

	// restore goroutine fns
	for id, fn := range cp.fns {
		s.goroutineFns[id] = fn
	}

	// set up replay log
	s.replayLog = make(map[goroutineId][]logEntry, len(cp.log))
	s.replayIdx = make(map[goroutineId]int, len(cp.log))
	s.opLog = make(map[goroutineId][]logEntry, len(cp.log))
	for id, entries := range cp.log {
		logCopy := make([]logEntry, len(entries))
		copy(logCopy, entries)
		s.replayLog[id] = logCopy
		s.replayIdx[id] = 0
	}

	// store channel snapshots for lazy restoration in GetOrCreateChan
	s.pendingChanRestores = make(map[uintptr]channelSnapshot, len(cp.channelBufs))
	for key, snap := range cp.channelBufs {
		s.pendingChanRestores[key] = snap
	}

	// create scheduler with restored clock
	s.scheduler = initScheduler(s.Drng, s.debug, keepAliveCh)
	s.scheduler.clock.now = cp.clock

	// register simulator
	registerSim(s)
	activeSimulators.Store(s, struct{}{})

	Print("🔄", "restore", AnyTag("seed", s.seed), AnyTag("clock", cp.clock))

	// re-spawn goroutines from checkpoint
	for _, snap := range cp.goroutines {
		fn, ok := cp.fns[snap.id]
		if !ok {
			continue
		}
		g := &goroutine{
			id:          snap.id,
			sim:         s,
			fn:          fn,
			internal:    snap.internal,
			syncCh:      make(chan struct{}),
			suspendedCh: make(chan struct{}),
		}
		s.scheduler.goroutines[g.id] = g
		s.scheduler.enqueue(g)
	}

	// start the main loop
	errChan := make(chan interface{}, 1)
	go func() {
		registerSim(s)
		loop(s, errChan)
	}()
}
