package sim_runtime

// logOp identifies the type of SIMAPI operation recorded in the operation log.
type logOp uint8

const (
	logChanRcv      logOp = iota // result: rcvResult{msg any, ok bool}
	logChanSnd                   // result: struct{}
	logSelect                    // result: selectResult{caseIdx int, msg any, ok bool}
	logSleep                     // result: struct{}
	logNewGoroutine              // result: goroutineId
)

type logEntry struct {
	op     logOp
	result any
}

type rcvResult struct {
	Msg any
	Ok  bool
}

type selectResult struct {
	CaseIdx int
	Msg     any
	Ok      bool
}

// goroutineSnap records the state of a goroutine at checkpoint time.
type goroutineSnap struct {
	id       goroutineId
	internal bool
	logLen   int
}

// Checkpoint captures the simulation state at a point in time, enabling
// replay-based branching without re-executing from step 0.
//
// Timer queue and synchronizer state are NOT captured: replay reconstructs
// them naturally as goroutines re-execute their SIMAPI calls after log
// exhaustion.
type Checkpoint struct {
	// per-goroutine operation log up to this point
	log map[goroutineId][]logEntry

	// scheduler state
	clock       int64                       // scheduler.clock.now
	drngState   drngSnapshot                // DRNG seed + call count
	channelBufs map[uintptr]channelSnapshot // buffered messages per channel
	goroutines  []goroutineSnap

	// closure registry: goroutineId → original fn (populated at spawn)
	fns map[goroutineId]func()

	// idSeq snapshot for deterministic ID generation on restore
	idSeq map[string]uint64

	// state snapshot
	state map[string]any
}

// channelSnapshot captures the buffered state of a channel at checkpoint time.
type channelSnapshot struct {
	buf    []any
	cap    int
	closed bool
}
