// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

// Package mocksync contains a wrapper for the `mockgen` tool, which bypasses running `mockgen` if the destination file
// is newer than the source file. This is useful because `mockgen` is slow and can be a bottleneck in the build process.
package mocksync

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
)

// Run a cached version of `mockgen`, which checks the modification times of the source and destination files,
// running mockgen only if necessary. This is similar to the behavior of `Make`, but it wasn't easy to express this
// in a Makefile because the generate commands are in our source code. The args here should not include the command
// name, e.g. they should be [os.Args][1:].
func Run(mockgenExecFn ExecFn, args []string) error {
	upToDate, err := isDestinationFileUpToDate(args)
	if err != nil {
		return err
	}
	if upToDate {
		return nil
	}

	// We couldn't confirm that the target file was up-to-date; run mockgen
	return mockgenExecFn(args)
}

// ExecFn is a function that should run a command with the given arguments.
type ExecFn func(args []string) error

// RealExecFn is an [ExecFn] which runs `mockgen` with the given arguments using [syscall.Exec]. This replaces the
// current process with the `mockgen` process, so that we don't need to maintain a parent and child process.
func RealExecFn(args []string) error {
	fullPath, lookErr := exec.LookPath("mockgen")
	if lookErr != nil {
		return fmt.Errorf("failed to find mockgen binary: %w", lookErr)
	}
	err := syscall.Exec(fullPath, append([]string{""}, args...), os.Environ())
	if err != nil {
		return fmt.Errorf("failed to exec mockgen with args %v: %w", args, err)
	}
	return nil
}

func isDestinationFileUpToDate(args []string) (bool, error) {
	var sourcePath, destPath string

	// Extract source and destination paths from the args
	for i := 0; i < len(args)-1; i++ {
		if args[i] == "-source" {
			sourcePath = args[i+1]
			i++
		} else if args[i] == "-destination" {
			destPath = args[i+1]
			i++
		}
	}

	// Either source or destination path aren't specified, so we can't check if the destination file is up-to-date.
	if sourcePath == "" || destPath == "" {
		return false, nil
	}

	// Get modification times of source and destination files
	fileInfo, err := os.Stat(sourcePath)
	if err != nil {
		return false, fmt.Errorf("failed to stat source file %q: %w", sourcePath, err)
	}
	sourceTime := fileInfo.ModTime()
	fileInfo, err = os.Stat(destPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		} else {
			return false, fmt.Errorf("failed to stat dest file %q: %w", destPath, err)
		}
	}
	destTime := fileInfo.ModTime()

	// Check if the destination file is up-to-date by comparing modification times
	sourceIsNewer := sourceTime.After(destTime)
	upToDate := !sourceIsNewer
	return upToDate, nil
}
