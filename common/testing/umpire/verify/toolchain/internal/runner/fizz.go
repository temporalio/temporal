package runner

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"go.temporal.io/server/common/testing/umpire/verify/toolchain/internal/fizz"
)

func executeFizz(ctx context.Context, executor executor, request Request) (_ execution, _ [][]string, retErr error) {
	workDirectory, err := os.MkdirTemp("", "umpire-fizz-run-")
	if err != nil {
		return execution{}, nil, err
	}
	defer func() {
		retErr = errors.Join(retErr, os.RemoveAll(workDirectory))
	}()

	modelSource := filepath.Join(request.ModelDir, "Umpire.fizz")
	workingModel := filepath.Join(workDirectory, "Umpire.fizz")
	if err := copyFile(modelSource, workingModel); err != nil {
		return execution{}, nil, err
	}
	config, err := fizz.RenderConfig(request.Bounds)
	if err != nil {
		return execution{}, nil, err
	}
	workingConfig := filepath.Join(workDirectory, "fizz.yaml")
	if err := os.WriteFile(workingConfig, config, 0o600); err != nil {
		return execution{}, nil, err
	}

	nativeOutput := filepath.Join(workDirectory, "native")
	var replay [][]string
	if request.ArtifactDir != "" {
		nativeOutput = filepath.Join(request.ArtifactDir, "fizz-native")
		if err := os.RemoveAll(nativeOutput); err != nil {
			return execution{}, nil, err
		}
		inputs := filepath.Join(nativeOutput, "inputs")
		if err := os.MkdirAll(inputs, 0o755); err != nil {
			return execution{}, nil, err
		}
		persistentModel := filepath.Join(inputs, "Umpire.fizz")
		persistentConfig := filepath.Join(inputs, "fizz.yaml")
		if err := copyFile(workingModel, persistentModel); err != nil {
			return execution{}, nil, err
		}
		if err := copyFile(workingConfig, persistentConfig); err != nil {
			return execution{}, nil, err
		}
		replayOutput := filepath.Join(nativeOutput, "replay")
		replay = [][]string{{request.ToolPath, "--test", "--copy-ast", "--output-dir", replayOutput, persistentModel}}
	} else if err := os.MkdirAll(nativeOutput, 0o755); err != nil {
		return execution{}, nil, err
	}

	args := []string{"--test", "--copy-ast", "--output-dir", nativeOutput, "Umpire.fizz"}
	actual := executor.run(ctx, command{path: request.ToolPath, args: args, dir: workDirectory})
	evidence, evidenceErr := collectFizzTraceEvidence(nativeOutput)
	if errors.Is(evidenceErr, errNativeTraceTooLarge) {
		actual.nativeTraceErr = fmt.Errorf("native-trace-too-large: %w", evidenceErr)
	} else if evidenceErr != nil {
		return execution{}, nil, evidenceErr
	} else {
		actual.nativeTrace = evidence
	}
	return actual, replay, nil
}

func collectFizzTraceEvidence(directory string) (string, error) {
	path := filepath.Join(directory, "error-graph.json")
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if info.Size() > maxNativeTraceBytes {
		return "", fmt.Errorf("%w: FizzBee error graph exceeds 4 MiB", errNativeTraceTooLarge)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(contents), nil
}
