// The MIT License
//
// Copyright (c) 2025 Temporal Technologies Inc.  All rights reserved.
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
// THE SOFTMARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package stamp

import (
	"context"
	"fmt"
	"hash/fnv"
	"os"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

var (
	DebugActID = ActID("<debugActID>")
)

type (
	action interface {
		String() string
	}
	ActionParams struct {
		Payload any
		ActID   ActID
		Async   bool
	}
	routableAction interface {
		action
		Route() string
		ID() ActID
		GetValidationErrors() []string
	}
	ActID                         ID
	ActionActor[MA modelAccessor] struct {
		actor MA
	}
	ActorModel[MA modelAccessor] struct {
		actor MA
	}
	actorModel[MA modelAccessor] interface {
		modelAccessor
		getModelAccessor() MA
		OnAction(ActionParams) error // implemented by user
	}
	ActionTarget[M modelAccessor] struct {
		mdl M
	}
	ActionTargetAsync[M modelAccessor] struct {
		mdl M
	}
	syncActionGen[A actorModel[AMA], AMA modelAccessor, T modelAccessor, P payload] interface {
		Generator[P]
		GetActor() AMA
		getTarget() T
	}
	asyncActionGen[A actorModel[AMA], AMA modelAccessor, T modelAccessor, P payload] interface {
		Generator[P]
		GetActor() AMA
		getTarget() T
	}
	payload       interface{}
	actOption     interface{} // TODO: add marker
	actIdDebugOpt struct {
		id ActID
	}
	actAsyncOpt struct{}
)

func NewActorModel[AM modelAccessor](mdl AM) ActorModel[AM] {
	return ActorModel[AM]{actor: mdl}
}

func newSeed(values ...any) int {
	h := fnv.New32a()
	h.Write([]byte(fmt.Sprintf("%v", values)))
	return int(h.Sum32())
}

func WithDebugActID() actOption {
	if _, ok := os.LookupEnv("CI"); ok {
		panic("WithDebugActID is not available in CI, only for local debugging")
	}
	return actIdDebugOpt{id: DebugActID}
}

// Act sends an action, as created by the action generator, from the actor to the System-under-Test (SUT).
// It blocks until the SUT's response is returned; or times out. It fails if the action was not routed
// to the expected target, as defined by the action generator.
func Act[
	A actorModel[AMA], // the actor that is triggering the action
	AMA modelAccessor, // the underlying model of the actor
	T modelAccessor, // the target model to receive the action
	P payload, // the payload
	AG syncActionGen[A, AMA, T, P], // the trigger type
](actor A, actionGen AG, opts ...actOption) T {
	env := actor.getModel().getEnv()
	tenv := env.getTestEnv()

	actParams := ActionParams{ActID: ActID(uuid.NewString())}
	for _, opt := range opts {
		switch o := opt.(type) {
		case actIdDebugOpt:
			actParams.ActID = o.id
		case actAsyncOpt:
			actParams.Async = true
		default:
			env.Fatal(fmt.Sprintf("unknown option: %T", opt))
		}
	}

	// validate action generator
	if err := validator.Struct(actionGen); err != nil {
		env.Fatal(fmt.Sprintf("action generator '%v' is invalid: %v", actionGen, err))
	}

	// copy action generator and assign actor
	// TODO: check that it wasn't set by user
	copyVal := reflect.New(reflect.TypeOf(actionGen))
	copyVal.Elem().Set(reflect.ValueOf(actionGen))
	copyVal.Elem().FieldByName("ActionActor").
		Set(reflect.ValueOf(ActionActor[AMA]{actor: actor.getModelAccessor()}))
	newActGen := copyVal.Elem().Interface().(syncActionGen[A, AMA, T, P])

	// generate the action
	// TODO: use type of payload to change the seed; that gives actions more unique IDs
	actParams.Payload = newActGen.Next(tenv.genContext())

	// setup trigger callback and action monitor
	actMonitor := newActionLog[T](env)
	actMonitor.start(actParams.ActID)

	env.Info(fmt.Sprintf("Sending '%v'",
		simpleSpew.Sdump(actParams.Payload)), triggerTag(actParams.ActID))

	var completed atomic.Bool
	send := func() {
		err := actor.OnAction(actParams)
		if err != nil {
			env.Fatal(fmt.Sprintf("failed to send '%v': %v", actParams.Payload, err))
		}
		completed.Store(true)
	}
	if actParams.Async {
		go send()
	} else {
		send()
	}

	timeout, _ := tenv.Context().Deadline()
	for {
		if time.Now().After(timeout) {
			break
		}
		if actParams.Async && actMonitor.matched() {
			break // async exits asap
		}
		if !actParams.Async && completed.Load() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// verify that the action arrived at the target
	res := actMonitor.stop()
	if res == nil {
		env.Fatal(actMonitor.errMessage(actor, newActGen))
	}
	return *res
}

// ActAsync works like Act but returns immediately once the targeted model has received the
// generated action, instead of waiting for the System-under-Test's response.
// TODO: enforce compile-type check
func ActAsync[
	A actorModel[AMA], // the actor that is triggering the action
	AMA modelAccessor, // the underlying model of the actor
	T modelAccessor, // the target model to receive the action
	P payload, // the payload
	AG asyncActionGen[A, AMA, T, P],
](actor A, actionGen AG, opts ...actOption) T {
	return Act(actor, actionGen, append(opts, actAsyncOpt{})...)
}

func (tt ActionTarget[M]) getTarget() M {
	return tt.mdl
}

func (tt ActionTargetAsync[M]) getTarget() M {
	return tt.mdl
}

func (ta ActionActor[A]) GetActor() A {
	return ta.actor
}

func (a *ActorModel[MA]) getModel() *internalModel {
	return a.actor.getModel()
}

func (a *ActorModel[MA]) GetID() ID {
	return a.actor.GetID()
}

func (a *ActorModel[MA]) getModelAccessor() MA {
	return a.actor
}

func (a *ActorModel[MA]) Context() context.Context {
	return a.getModel().getEnv().getTestEnv().Context()
}
