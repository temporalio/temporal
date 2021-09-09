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

package goro_test

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/server/internal/goro"
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
		select {
		case <-time.After(1 * time.Minute):
			// do something every minute
		case <-ctx.Done():
			return nil
		}
	}
}

func (svc *ExampleService) backgroundLoop2(ctx context.Context) error {
	fmt.Println("starting backgroundLoop2")
	defer fmt.Println("stopping backgroundLoop2")
	for {
		select {
		case <-time.After(10 * time.Second):
			// do something every 10 seconds
		case <-ctx.Done():
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
