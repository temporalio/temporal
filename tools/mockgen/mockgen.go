// Package mockgen contains a wrapper for the `mockgen` tool, which bypasses running `mockgen` if the destination file
// is newer than the source file. This is useful because `mockgen` is slow and can be a bottleneck in the build process.
package mockgen

import (
	"os"
	"os/exec"
	"time"
)

// Run a cached version of `mockgen`, which checks the modification times of the source and destination files,
// running mockgen only if necessary. This is similar to the behavior of `Make`, but it wasn't easy to express this
// in a Makefile because the generate commands are in our source code.
func Run(args []string, opts ...Option) error {
	params := &Params{
		Exec: runRealCommand,
	}
	for _, opt := range opts {
		opt(params)
	}

	upToDate, err := isDestinationFileUpToDateWithSourceInArgs(args)
	if err != nil {
		return err
	}
	if !upToDate {
		return nil
	}

	// We couldn't confirm that the target file was up-to-date; run mockgen
	return params.Exec(args)
}

// Params for running the mockgen wrapper.
type Params struct {
	// Exec is a function that should execute a shell command with the given arguments.
	Exec func(args []string) error
}

// Option to override the [Params] of the mockgen wrapper.
type Option func(*Params)

// WithExecFn sets a custom [Params.Exec] function.
func WithExecFn(f func(args []string) error) Option {
	return func(params *Params) {
		params.Exec = f
	}
}

func runRealCommand(args []string) error {
	cmd := exec.Command("mockgen", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func isDestinationFileUpToDateWithSourceInArgs(args []string) (bool, error) {
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

	// If either source or destination path aren't specified, run mockgen. There's no way for us to know if the source
	// is newer than the destination in this case.
	if sourcePath == "" || destPath == "" {
		return true, nil
	}

	// Get modification times of source and destination files
	var sourceTime, destTime time.Time
	for _, f := range []struct {
		filePath string
		modTime  *time.Time
	}{
		{sourcePath, &sourceTime},
		{destPath, &destTime},
	} {
		fileInfo, err := os.Stat(f.filePath)
		if err != nil {
			return false, err
		}
		*f.modTime = fileInfo.ModTime()
	}

	// Compare modification times
	upToDate := sourceTime.After(destTime)
	return upToDate, nil
}
