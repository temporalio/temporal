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
	"sync/atomic"
	"time"

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

var (
	// Simulator is the global Simulator (ie a singleton). Use Start to create it.
	sim *simulator

	DebugMode = initOptionFunc(func(s *simulator) {
		s.debug = true
	})
	Seed = func(seed int64) initOptionFunc {
		return func(s *simulator) {
			s.seed = seed
		}
	}
	StopFirstIfRunning = initOptionFunc(func(s *simulator) {
		s.stopFirstIfRunning = true
	})
	// Hook provides a function that is invoked after every step.
	// It can decide to stop the execution.
	Hook = func(h HookFunc) initOptionFunc {
		return func(s *simulator) {
			s.hook = h
		}
	}
)

type (
	simulator struct {
		// config options
		seed               int64
		stopFirstIfRunning bool
		debug              bool

		info      Info
		hook      HookFunc
		hookCh    chan struct{}
		scheduler *scheduler
		Drng      *rand.Rand
		main      *goroutine

		panicErr string
		idSeq    map[string]uint64 // used to generate goroutine/channel ids
		State    map[string]any    // used to store arbitrary state

		done    chan struct{}
		abortCh chan struct{}
		stopped atomic.Bool
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

// Start launches a global simulator
//
// It is *not* safe to call this concurrently.
// For concurrent execution, start a separate Go process instead.
func Start(opts ...InitOption) {
	// load init options from environment variables
	if debugMode, _ := os.LookupEnv(DebugModeEnvKey); debugMode == "true" {
		opts = append(opts, DebugMode)
	}
	if seed, _ := os.LookupEnv(SeedEnvKey); seed != "" {
		parsedSeed, err := strconv.ParseInt(seed, 10, 64)
		verify.T(err == nil, fmt.Errorf("failed to parse seed from environment variable: %w", err).Error())
		opts = append(opts, Seed(parsedSeed))
	}
	if remoteCtrlAddr, _ := os.LookupEnv(RemoteControlAddrEnvKey); remoteCtrlAddr != "" {
		remoteClientId, _ := os.LookupEnv(RemoteControlIdEnvKey)
		verify.T(strings.TrimSpace(remoteClientId) != "", "missing/empty remote ctrl client id")
		hook := newRemoteCtrl(remoteCtrlAddr, remoteClientId)
		opts = append(opts, Hook(hook.hook))
	}

	// a "main" goroutine to represent the currently running goroutine
	main := &goroutine{
		id:          mainGoroutineID,
		fn:          func() {},
		state:       running,
		syncCh:      make(chan struct{}),
		suspendedCh: make(chan struct{}),
	}

	// create new simulator and apply init options
	newSimulator := &simulator{
		done:    make(chan struct{}),
		abortCh: make(chan struct{}),
		State:   make(map[string]any),
		idSeq:   make(map[string]uint64),
		info:    Info{Stop: make(chan struct{})},
		hook:    func(info *Info) {}, // noop
		hookCh:  make(chan struct{}, 1),
		main:    main,
		seed:    rand.Int63(), // always generate a fresh seed (user might override this later)
	}
	for _, opt := range opts {
		opt.apply(newSimulator)
	}

	// This pseudo-random number generator produces all random factors in the system,
	// including random numbers, delays, and errors introduced by the simulator.
	// With the same random seed, the same execution sequence can be produced.
	newSimulator.Drng = rand.New(rand.NewSource(newSimulator.seed))

	// create new scheduler
	newSimulator.scheduler = initScheduler(newSimulator.Drng, newSimulator.debug)

	// stop last simulator first, if running and requested
	if sim != nil {
		if newSimulator.stopFirstIfRunning {
			// TODO: not working
			//abort()
		} else {
			panic("previous simulator still running")
		}
	}
	sim = newSimulator
	Print("⭐", "init", AnyTag("seed", sim.seed))

	// start simulator's main loop
	errChan := make(chan interface{}, 1)
	go loop(errChan)

	// TODO
	sim.scheduler.active = main
	sim.scheduler.add(main)
}

func loop(errChan chan interface{}) {
	defer func() {
		// mark simulator as stopped (which prevents a deadlock if Stop is invoked more than once)
		sim.stopped.Store(true)

		// signal that simulator loop is done
		close(sim.done)
	}()

	// TODO
	//defer func() {
	//	if err := recover(); err != nil {
	//		errChan <- err
	//	}
	//}()

	// listen for process shutdown signal
	interruptCh := make(chan os.Signal, 2)
	signal.Notify(interruptCh, os.Interrupt, os.Kill)

	// TODO: explain
	sim.main.waitUntilStopped()

	for {
		sim.info.Steps += 1
		sim.hook(&sim.info)

		stepsTag := AnyTag("step", sim.info.Steps)
		Dbg("♻️", "   ", stepsTag)

		select {
		case <-sim.scheduler.tick():
			// regular scheduler tick
		case <-time.NewTimer(sim.scheduler.stuckTimeout).C:
			Dbg("🛑️", "   ",
				stepsTag,
				AnyTag("reason", "stuck"))
			panic("💥simulator is stuck")
		case <-sim.abortCh:
			Dbg("🛑️", "   ",
				stepsTag,
				AnyTag("reason", "abort"))
			return
		case <-interruptCh:
			Dbg("🛑️", "   ",
				stepsTag,
				AnyTag("reason", "sigkill"))
			return
		case <-sim.info.Stop:
			Dbg("🛑️", "   ",
				stepsTag,
				AnyTag("reason", "hook"))
			return
		case <-sim.scheduler.doneCh:
			Dbg("🛑️", "   ",
				stepsTag,
				AnyTag("reason", "done"))
			return
		}
	}
}

func abort() {
	if sim.stopped.Load() {
		// already stopped
		return
	}

	// signal simulator to stop loop
	close(sim.abortCh)

	// wait for simulator loop to finish
	<-sim.done
}

func Join() {
	verify.T(sim.main.state != suspended,
		"only the main goroutine can be joined with the simulator")

	switch sim.main.state {
	case running:
		// do not reschedule main goroutine again
		sim.main.done()
	case done:
		// already done
	}

	// wait for simulator loop to finish
	<-sim.done
}

func (f initOptionFunc) apply(s *simulator) {
	f(s)
}

func nextId(ns string) uint64 {
	if _, ok := sim.idSeq[ns]; !ok {
		sim.idSeq[ns] = 0
	}
	sim.idSeq[ns] += 1
	return sim.idSeq[ns]
}

func Time() time.Time {
	return CurrentSimulator().scheduler.time()
}

func Sleep(d time.Duration) {
	CurrentSimulator().scheduler.sleep(d)
}

func CurrentSimulator() *simulator {
	if sim == nil {
		Start()
	}
	return sim
}
