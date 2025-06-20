package goro_test

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/server/common/goro"
)

type ExampleService struct {
	gorogrp goro.Group // goroutines managed in here
}

func (svc *ExampleService) Start() {
	// launch two background goroutines
	svc.gorogrp.Go(svc.backgroundLoop1)
	svc.gorogrp.Go(svc.backgroundLoop2)
}

func (svc *ExampleService) Stop() {
	// stop all goroutines in the goro.Group
	svc.gorogrp.Cancel() // interrupt the background goroutines
	svc.gorogrp.Wait()   // wait for the background goroutines to finish
}

func (svc *ExampleService) backgroundLoop1(ctx context.Context) error {
	fmt.Println("starting backgroundLoop1")
	defer fmt.Println("stopping backgroundLoop1")
	for {
		timer := time.NewTimer(1 * time.Minute)
		select {
		case <-timer.C:
			// do something every minute
		case <-ctx.Done():
			timer.Stop()
			return nil
		}
	}
}

func (svc *ExampleService) backgroundLoop2(ctx context.Context) error {
	fmt.Println("starting backgroundLoop2")
	defer fmt.Println("stopping backgroundLoop2")
	for {
		timer := time.NewTimer(10 * time.Second)
		select {
		case <-timer.C:
			// do something every 10 seconds
		case <-ctx.Done():
			timer.Stop()
			return nil
		}
	}
}

func ExampleGroup() {
	var svc ExampleService
	svc.Start()
	svc.Stop()

	// it is safe to call svc.Stop() multiple times
	svc.Stop()
	svc.Stop()
	svc.Stop()

	// Unordered output:
	// starting backgroundLoop1
	// starting backgroundLoop2
	// stopping backgroundLoop1
	// stopping backgroundLoop2
}
