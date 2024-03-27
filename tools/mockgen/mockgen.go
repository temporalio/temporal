package mockgen

import (
	"os"
	"os/exec"
	"time"
)

// Run checks the modification times of the source and destination files,
// running mockgen only if necessary.
func Run(args []string, opts ...Option) error {
	params := &Params{
		RunCommand: runRealCommand,
	}
	for _, opt := range opts {
		opt(params)
	}

	shouldRun, err := shouldRunMockgenBasedOnArgs(args)
	if err != nil {
		return err
	}
	if !shouldRun {
		return nil
	}

	// Either the source or destination file doesn't exist, or the source is newer; run mockgen
	return params.RunCommand(args)
}

func runRealCommand(args []string) error {
	cmd := exec.Command("mockgen", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

type Params struct {
	RunCommand func(args []string) error
}

type Option func(*Params)

func WithRunCommand(f func(args []string) error) Option {
	return func(params *Params) {
		params.RunCommand = f
	}
}

func shouldRunMockgenBasedOnArgs(args []string) (bool, error) {
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

	// If either source or destination path is empty, run mockgen
	if sourcePath == "" || destPath == "" {
		return true, nil
	}

	// Get modification times of source and destination files
	var sourceTime, destTime time.Time
	for _, arg := range []struct {
		path  string
		field *time.Time
	}{
		{sourcePath, &sourceTime},
		{destPath, &destTime},
	} {
		fileInfo, err := os.Stat(arg.path)
		if err != nil {
			return false, err
		}
		*arg.field = fileInfo.ModTime()
	}

	// Compare modification times
	shouldRun := sourceTime.After(destTime)
	return shouldRun, nil
}
