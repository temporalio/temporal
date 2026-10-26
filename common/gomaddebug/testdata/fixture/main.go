package main

import (
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"go.temporal.io/server/common/gomaddebug"
)

var (
	progress                                                = gomaddebug.DeclareProbe("fixture.progress")
	allocating                                              = gomaddebug.DeclareProbe("fixture.allocating")
	oversized                                               = gomaddebug.DeclareProbe("fixture.oversized")
	applicationLock                                         sync.Mutex
	perturbation                                            []byte
	largeString                                             string
	completed                                               uint64
	applicationTurns                                        uint64
	inspectionTimer, inspectionFinalizer, inspectionCleanup bool
)

type garbage struct{ bytes [1024]byte }

//go:noinline
func inspectionGarbage() {
	finalized := new(garbage)
	runtime.SetFinalizer(finalized, func(*garbage) { inspectionFinalizer = true })
	cleaned := new(garbage)
	runtime.AddCleanup(cleaned, func(int) { inspectionCleanup = true }, 0)
}

func init() {
	largeString = string(make([]byte, 32<<10))
	gomaddebug.RegisterDump("fixture.snapshot", func(w *gomaddebug.DumpWriter) {
		if applicationLock.TryLock() {
			applicationLock.Unlock()
			w.String("application lock was not held\n")
		} else {
			w.String("application lock is held; callback reads only frozen scalar state\n")
		}
		if completed > 0 {
			w.String("progress observed\n")
		}
	})
	gomaddebug.RegisterDump("fixture.panic", func(*gomaddebug.DumpWriter) { panic("dump fixture") }) //nolint:forbidigo // Exercises dump panic isolation.
	gomaddebug.RegisterDump("fixture.truncated", func(w *gomaddebug.DumpWriter) {
		for range 4 {
			w.String(largeString)
		}
	})
	gomaddebug.RegisterDump("fixture.gc", func(w *gomaddebug.DumpWriter) {
		before := applicationTurns
		inspectionGarbage()
		time.AfterFunc(0, func() { inspectionTimer = true })
		memory := make([]byte, 32<<20)
		for i := range memory {
			memory[i] = byte(i)
		}
		runtime.GC()
		runtime.GC()
		runtime.KeepAlive(memory)
		if before != applicationTurns || inspectionTimer || inspectionFinalizer || inspectionCleanup {
			panic("application or timer/finalizer/cleanup callback ran during dump collection") //nolint:forbidigo // Fails the freeze acceptance fixture if application execution resumes.
		}
		w.String("32MiB allocation and collection completed with application frozen\n")
	})
	gomaddebug.RegisterInvariant("fixture.before_three", true, func() bool { return completed < 3 })
	gomaddebug.RegisterInvariant("fixture.recovering", false, func() bool { return completed != 2 && completed != 4 })
	gomaddebug.RegisterInvariant("fixture.panic_check", false, func() bool { panic("invariant fixture") }) //nolint:forbidigo // Distinguishes invariant panic from predicate failure.
	gomaddebug.RegisterInvariant("fixture.timeout_check", false, func() bool {
		for { //nolint:staticcheck // SA5002: A nonyielding predicate exercises host watchdog cancellation.
		}
	})
	gomaddebug.RegisterDump("fixture.timeout", func(*gomaddebug.DumpWriter) {
		for { //nolint:staticcheck // SA5002: A nonyielding dump exercises host watchdog cancellation.
		}
	})
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "early":
			panic("early initialization fixture") //nolint:forbidigo // Exercises incomplete registry refusal after initialization failure.
		case "duplicate":
			gomaddebug.DeclareProbe("fixture.progress")
		case "capacity":
			for i := range 256 {
				gomaddebug.DeclareProbe("fixture.extra." + strconv.Itoa(i))
			}
		default:
		}
	}
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "late" {
		gomaddebug.DeclareProbe("fixture.late")
	}
	applicationLock.Lock()
	for range 3 {
		go func() {
			for range 12 {
				applicationTurns++
				runtime.Gosched()
			}
		}()
	}
	done := make(chan struct{})
	go func() {
		left, right := make(chan struct{}, 1), make(chan struct{}, 1)
		left <- struct{}{}
		right <- struct{}{}
		for range 8 {
			select {
			case <-left:
				left <- struct{}{}
			case <-right:
				right <- struct{}{}
			}
			completed++
			if progress.Enabled() {
				progress.Emit(gomaddebug.Uint64(completed), gomaddebug.String("existing"), gomaddebug.Bool(true))
			}
			if oversized.Enabled() {
				oversized.Emit(gomaddebug.String(largeString))
			}
			if allocating.Enabled() {
				perturbation = make([]byte, 37)
			}
			runtime.Gosched()
		}
		close(done)
	}()
	for range 8 {
		runtime.Gosched()
	}
	<-done
	applicationLock.Unlock()
	os.Exit(1)
}
