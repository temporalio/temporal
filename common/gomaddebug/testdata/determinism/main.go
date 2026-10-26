//go:build gomad

package main

import (
	"fmt"
	"os"
	"runtime"
	"time"
	_ "unsafe"

	"go.temporal.io/server/common/gomaddebug"
)

//go:linkname fixtureDraw runtime.gomadDeterminismFixtureDraw
func fixtureDraw()

//go:linkname fixtureOutputFault runtime.gomadDeterminismFixtureOutput
func fixtureOutputFault() bool

func init() {
	gomaddebug.RegisterDump("fixture.determinism", func(w *gomaddebug.DumpWriter) { w.String("executed seeded fixture\n") })
}

func readyChoice() {
	left, right := make(chan struct{}, 1), make(chan struct{}, 1)
	left <- struct{}{}
	right <- struct{}{}
	select {
	case <-left:
	case <-right:
	}
}

func main() {
	done := make(chan struct{})
	ready := make(chan struct{})
	go func() {
		for range 16 {
			runtime.Gosched()
		}
		<-ready
		left, right := make(chan struct{}, 1), make(chan struct{}, 1)
		left <- struct{}{}
		right <- struct{}{}
		for range 16 {
			select {
			case <-left:
				left <- struct{}{}
			case <-right:
				right <- struct{}{}
			}
			runtime.Gosched()
		}
		close(done)
	}()
	for range 8 {
		readyChoice()
		runtime.Gosched()
	}
	if fixtureOutputFault() {
		fmt.Println("perturbed fixture output")
	} else {
		fmt.Println("reference fixture output")
	}
	fixtureDraw()
	close(ready)
	for range 3 {
		before := time.Now()
		time.Sleep(time.Millisecond)
		if !time.Now().After(before) {
			panic("virtual time did not advance")
		}
		readyChoice()
	}
	<-done
	fmt.Println("executed seeded fixture")
	os.Exit(1)
}
