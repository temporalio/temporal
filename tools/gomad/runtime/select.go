package sim_runtime

import (
	"reflect"

	"github.com/petermattis/goid"
	"go.temporal.io/server/tools/gomad/util/verify"
)

const (
	noDefaultCase = -1
)

type (
	Selector struct {
		Case           int  // the case that was selected
		Value          any  // the value that was received (for channel rcv)
		Ok             bool // whether a value was received (for channel rcv)
		hasDefaultCase bool
	}
)

func Select(values ...any) *Selector {
	verify.T(len(values) > 0, "select with infinite loop")

	// If called from outside the cooperative scheduler, delegate to a simulated goroutine
	// so that the blocking select can be properly cooperative-scheduled.
	if tryCurrentGoroutine() == nil {
		s := tryAnySimulator()
		verify.T(s != nil, "Select called outside of simulation")
		nativeGid := goid.Get()
		nativeDone := make(chan *Selector, 1)
		g := &goroutine{
			id:  goroutineId(nextId(s, "go")),
			sim: s,
			fn: func() {
				result := Select(values...)
				s.nativeTimes.Store(nativeGid, s.scheduler.clock.now)
				nativeDone <- result
			},
			syncCh:      make(chan struct{}),
			suspendedCh: make(chan struct{}),
		}
		s.scheduler.addFromNative(g)
		return <-nativeDone
	}

	slct := &Selector{}

	// collect channels
	chanCount := len(values) / 3
	var blocks []*syncBlock
	for i := 0; i < chanCount; i++ {
		op := SyncOp(values[i*3].(int))
		ch := values[i*3+1].(Channel)
		if reflect.ValueOf(ch).IsNil() { // cannot use `== nil` as it's an interface type
			continue
		}

		writeFn := values[i*3+2]
		selectCase := i

		blocks = append(blocks, &syncBlock{
			g:                CurrentSimulator().scheduler.active,
			pt:               ch,
			op:               op,
			requireSyncMatch: ch.requiresSyncMatch(op),
			onSync: func() {
				slct.Case = selectCase
				if writeFn != nil {
					ch.write(writeFn.(func() any)())
				} else {
					slct.Value, slct.Ok = ch.read()
				}
			},
		})
	}

	// configure default case, if present
	defaultCaseNo := chanCount
	if len(values)%3 == 1 {
		slct.hasDefaultCase = true
		slct.Case = defaultCaseNo
	} else {
		slct.Case = noDefaultCase
	}

	verify.T(len(blocks) > 0 || slct.hasDefaultCase, "select has no valid cases")

	// *suspend* goroutine until any of the cases is unblocked
	suspend(&syncBlock{pt: slct, op: slc, children: blocks})

	verify.T(slct.Case != noDefaultCase, "did not select a case: %v", slct.Case)
	verify.T(slct.Case <= defaultCaseNo, "selected invalid case: %v", slct.Case)
	if slct.Case == defaultCaseNo {
		Dbg("🌀🔀️", "select", AnyTag("case", "default"), CurLocTag())
	} else {
		Dbg("🌀🔀️", "select", AnyTag("case", slct.Case), CurLocTag())
	}

	return slct
}
