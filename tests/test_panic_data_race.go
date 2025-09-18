package tests

import (
	"fmt"
	"testing"
)

func showPanic() {
	panic("test panic")
}

func TestSamplePanic(t *testing.T) {
	showPanic()
}

func TestSampleDataRace(t *testing.T) {
	c := make(chan bool)
	m := make(map[string]string)
	go func() {
		m["1"] = "1"
		c <- true
	}()
	m["2"] = "b"
	<-c
	for k, v := range m {
		fmt.Println(k, v)
	}
}
