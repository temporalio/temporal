package stamp

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
)

type (
	actionLog[T any] struct {
		env              modelEnv
		result           atomic.Pointer[T]
		removeCallbackFn func()

		matchLock  sync.Mutex
		matchItems map[string][]modelWrapper
		matchSet   map[string]map[modelKey]struct{}
		matchTypes map[modelType]struct{}
	}
	target[T any] interface {
		getTarget() T
	}
)

func newActionLog[T any](env modelEnv) *actionLog[T] {
	return &actionLog[T]{
		env:        env,
		matchItems: map[string][]modelWrapper{},
		matchSet:   map[string]map[modelKey]struct{}{},
		matchTypes: map[modelType]struct{}{},
	}
}

func (am *actionLog[T]) start(triggerID ActID) {
	am.removeCallbackFn = am.env.onMatched(triggerID, func(act routableAction, mdl modelWrapper) {
		if am.result.Load() != nil {
			// already triggered, ignore
			return
		}

		am.matchLock.Lock()
		defer am.matchLock.Unlock()

		actStr := act.String()
		if _, ok := am.matchSet[actStr]; !ok {
			am.matchSet[actStr] = map[modelKey]struct{}{}
			am.matchItems[actStr] = []modelWrapper{}
		}
		if _, ok := am.matchSet[actStr][mdl.getKey()]; !ok {
			am.matchSet[actStr][mdl.getKey()] = struct{}{}
			am.matchItems[actStr] = append(am.matchItems[actStr], mdl)
		}
		am.matchTypes[mdl.getType()] = struct{}{}

		// Check if the model matches the expected type and store it if it does
		if matchedMdl, ok := mdl.(T); ok {
			am.result.Store(&matchedMdl)
		}
	})
}

func (am *actionLog[T]) matched() bool {
	return am.result.Load() != nil
}

func (am *actionLog[T]) stop() *T {
	am.removeCallbackFn()
	return am.result.Load()
}

func (am *actionLog[T]) errMessage(
	actor any,
	trg target[T],
) string {
	triggerTargetType := reflect.TypeFor[T]().Elem()
	am.matchLock.Lock()
	defer am.matchLock.Unlock()

	var matchedDetails strings.Builder
	for act, models := range am.matchItems {
		matchedDetails.WriteString("- ")
		matchedDetails.WriteString(act)
		matchedDetails.WriteString(":\n")
		for _, mdl := range models {
			matchedDetails.WriteString("\t- ")
			matchedDetails.WriteString(mdl.str())
			matchedDetails.WriteString("\n")
		}
	}

	var expectedNextMatch string
	var notMatched strings.Builder
	for _, mdlType := range am.env.getPathTo(triggerTargetType) {
		if _, ok := am.matchTypes[mdlType]; ok {
			continue
		}
		if expectedNextMatch == "" {
			expectedNextMatch = mdlType.name
		}
		notMatched.WriteString("- ")
		notMatched.WriteString(mdlType.String())
		notMatched.WriteString("\n")
	}

	return fmt.Sprintf(`
Trigger '%s' by actor '%s' did not arrive at target '%s'.

Router should have matched '%s' next.

%s:
%s
%s:
%s
%s:
%s`,
		boldStr(reflect.TypeOf(trg).String()),
		boldStr(reflect.TypeOf(actor).String()),
		boldStr(reflect.TypeOf(trg.getTarget()).String()),
		boldStr(expectedNextMatch),
		underlineStr("Not Matched"),
		notMatched.String(),
		underlineStr("Matched"),
		matchedDetails.String(),
		underlineStr("Trigger"),
		simpleSpew.Sdump(trg))
}
