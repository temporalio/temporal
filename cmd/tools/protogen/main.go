package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/fatih/color"
)

// runCommand executes a shell command and returns an error if it fails
func runCommand(ctx context.Context, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// exists checks if a path exists
func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// replaceInFile replaces text in a file using sed-like patterns
func replaceInFile(filePath, oldPattern, newPattern string) error {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}

	// Convert sed pattern to Go regex
	re, err := regexp.Compile(oldPattern)
	if err != nil {
		return err
	}

	newContent := re.ReplaceAllString(string(content), newPattern)
	return os.WriteFile(filePath, []byte(newContent), 0644)
}

func main() {
	ctx := context.Background()

	// Get environment variables
	protoOut := os.Getenv("PROTO_OUT")
	if protoOut == "" {
		log.Fatalf("Error: PROTO_OUT environment variable not set")
	}

	protogen := os.Getenv("PROTOGEN")
	if protogen == "" {
		log.Fatalf("Error: PROTOGEN environment variable not set")
	}

	apiBinpb := os.Getenv("API_BINPB")
	if apiBinpb == "" {
		log.Fatalf("Error: API_BINPB environment variable not set")
	}

	protoRoot := os.Getenv("PROTO_ROOT")
	if protoRoot == "" {
		log.Fatalf("Error: PROTO_ROOT environment variable not set")
	}

	goimports := os.Getenv("GOIMPORTS")
	if goimports == "" {
		log.Fatalf("Error: GOIMPORTS environment variable not set")
	}

	mockgen := os.Getenv("MOCKGEN")
	if mockgen == "" {
		log.Fatalf("Error: MOCKGEN environment variable not set")
	}

	api := protoOut
	new := api + ".new"

	color.HiCyan("Generating protos...")

	// Remove and create new directory
	if err := os.RemoveAll(new); err != nil && !os.IsNotExist(err) {
		log.Fatalf("Error removing directory %s: %v", new, err)
	}

	if err := os.MkdirAll(new, 0755); err != nil {
		log.Fatalf("Error creating directory %s: %v", new, err)
	}

	// Run protogen
	protoArgs := []string{
		"--descriptor_set_in=" + apiBinpb,
		"--root=" + filepath.Join(protoRoot, "internal"),
		"--rewrite-enum=BuildId_State:BuildId",
		"--output=" + new,
		"-p", "go-grpc_out=paths=source_relative:" + new,
		"-p", "go-helpers_out=paths=source_relative:" + new,
	}

	if err := runCommand(ctx, protogen, protoArgs...); err != nil {
		log.Fatalf("Error running protogen: %v", err)
	}

	color.HiCyan("Run goimports for proto files...")
	if err := runCommand(ctx, goimports, "-w", new); err != nil {
		log.Fatalf("Error running goimports: %v", err)
	}

	color.HiCyan("Generate proto mocks...")

	// Find service.pb.go and service_grpc.pb.go files
	err := filepath.Walk(new, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if strings.HasSuffix(path, "service.pb.go") || strings.HasSuffix(path, "service_grpc.pb.go") {
			// Convert service/ to servicemock/ and .go to .mock.go
			dst := strings.ReplaceAll(path, "service/", "servicemock/")
			dst = strings.ReplaceAll(dst, ".go", ".mock.go")

			// Get package name
			dstDir := filepath.Dir(dst)
			pkg := filepath.Base(filepath.Dir(dstDir))

			// Create destination directory
			if err := os.MkdirAll(dstDir, 0755); err != nil {
				return fmt.Errorf("error creating directory %s: %v", dstDir, err)
			}

			// Run mockgen
			mockgenArgs := []string{
				"-package", pkg,
				"-source", path,
				"-destination", dst,
			}

			if err := runCommand(ctx, mockgen, mockgenArgs...); err != nil {
				return fmt.Errorf("error running mockgen: %v", err)
			}

			// Fix imports in the generated mock file
			content, err := os.ReadFile(dst)
			if err != nil {
				return fmt.Errorf("error reading file %s: %v", dst, err)
			}

			// Replace the incorrect import path
			newContent := strings.ReplaceAll(string(content), new+"/temporal/server/", "")

			if err := os.WriteFile(dst, []byte(newContent), 0644); err != nil {
				return fmt.Errorf("error writing file %s: %v", dst, err)
			}
		}
		return nil
	})

	if err != nil {
		log.Fatalf("Error processing mock files: %v", err)
	}

	color.HiCyan("Modify history service server interface...")

	historyServiceFile := filepath.Join(new, "temporal", "server", "api", "historyservice", "v1", "service_grpc.pb.go")

	// Replace GetWorkflowExecutionHistory method signature
	err = replaceInFile(historyServiceFile,
		`GetWorkflowExecutionHistory\(context\.Context, \*GetWorkflowExecutionHistoryRequest\) \(\*GetWorkflowExecutionHistoryResponse, error\)`,
		`GetWorkflowExecutionHistory(context.Context, *GetWorkflowExecutionHistoryRequest) (*GetWorkflowExecutionHistoryResponseWithRaw, error)`)
	if err != nil {
		log.Fatalf("Error modifying GetWorkflowExecutionHistory: %v", err)
	}

	// Replace RecordWorkflowTaskStarted method signature
	err = replaceInFile(historyServiceFile,
		`RecordWorkflowTaskStarted\(context\.Context, \*RecordWorkflowTaskStartedRequest\) \(\*RecordWorkflowTaskStartedResponse, error\)`,
		`RecordWorkflowTaskStarted(context.Context, *RecordWorkflowTaskStartedRequest) (*RecordWorkflowTaskStartedResponseWithRawHistory, error)`)
	if err != nil {
		log.Fatalf("Error modifying RecordWorkflowTaskStarted: %v", err)
	}

	color.HiCyan("Moving proto files into place...")

	old := api + ".old"

	// Move existing api directory to old if it exists
	if exists(api) {
		if err := os.Rename(api, old); err != nil {
			log.Fatalf("Error moving %s to %s: %v", api, old, err)
		}
	}

	// Create api directory
	if err := os.MkdirAll(api, 0755); err != nil {
		log.Fatalf("Error creating directory %s: %v", api, err)
	}

	// Move files from new/temporal/server/api/* to api/
	sourceApiDir := filepath.Join(new, "temporal", "server", "api")
	err = filepath.Walk(sourceApiDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip the root directory
		if path == sourceApiDir {
			return nil
		}

		// Calculate relative path from sourceApiDir
		relPath, err := filepath.Rel(sourceApiDir, path)
		if err != nil {
			return err
		}

		dstPath := filepath.Join(api, relPath)

		if info.IsDir() {
			return os.MkdirAll(dstPath, info.Mode())
		} else {
			// Create parent directory if it doesn't exist
			if err := os.MkdirAll(filepath.Dir(dstPath), 0755); err != nil {
				return err
			}
			return os.Rename(path, dstPath)
		}
	})

	if err != nil {
		log.Fatalf("Error moving files: %v", err)
	}

	// Clean up temporary directories
	if err := os.RemoveAll(new); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: Error removing temporary directory %s: %v\n", new, err)
	}

	if err := os.RemoveAll(old); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: Error removing old directory %s: %v\n", old, err)
	}

	color.HiCyan("Proto generation completed successfully!")
}
