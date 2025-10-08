package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/fatih/color"
)

var cyan = color.New(color.FgHiCyan, color.Bold)

func info(format string, args ...interface{}) {
	log.Println(cyan.Sprintf(format, args...))
}

func runCommand(ctx context.Context, command string, args ...string) error {
	cmd := exec.CommandContext(ctx, command, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func copyRecursive(src, dst string) error {
	srcInfo, err := os.Stat(src)
	if err != nil {
		return fmt.Errorf("error stating source %s: %w", src, err)
	}

	if !srcInfo.IsDir() {
		return fmt.Errorf("source %s is not a directory", src)
	}

	err = os.MkdirAll(dst, srcInfo.Mode())
	if err != nil {
		return fmt.Errorf("error creating destination directory %s: %w", dst, err)
	}

	entries, err := os.ReadDir(src)
	if err != nil {
		return fmt.Errorf("error reading source directory %s: %w", src, err)
	}

	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())
		srcInfo, err := entry.Info()
		if err != nil {
			return fmt.Errorf("error getting info for %s: %w", srcPath, err)
		}

		if entry.IsDir() {
			if err := copyRecursive(srcPath, dstPath); err != nil {
				return err
			}
		} else {
			// Fix imports in the generated mock file
			content, err := os.ReadFile(srcPath)
			if err != nil {
				return fmt.Errorf("error reading file %s: %w", srcPath, err)
			}

			if err := os.WriteFile(dstPath, []byte(content), srcInfo.Mode()); err != nil {
				return fmt.Errorf("error writing file %s: %w", dstPath, err)
			}
		}
	}

	return nil
}

// exists checks if a path exists
func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func existsDir(path string) bool {
	stat, err := os.Stat(path)
	return err == nil && stat.IsDir()
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

type generator struct {
	rootDir               string
	tempOut               string
	tempProtoRoot         string
	protoOut              string
	protoBackup           string
	apiBinpb              string
	protoRoot             string
	protogenBin           string
	goimportsBin          string
	mockgenBin            string
	protocGenGoBin        string
	protocGenGoGrpcBin    string
	protocGenGoHelpersBin string
	protocGenGoChasmBin   string
	chasmLibDirs          []string
}

func newGenerator() (*generator, error) {
	var gen generator
	flag.StringVar(&gen.protoOut, "proto-out", "", "Proto output directory (required)")
	flag.StringVar(&gen.protoRoot, "proto-root", "", "Proto root directory (required)")
	flag.StringVar(&gen.rootDir, "root", "", "Root directory (required)")
	flag.StringVar(&gen.apiBinpb, "api-binpb", "", "Path to API binpb file (required)")
	flag.StringVar(&gen.protogenBin, "protogen-bin", "", "Path to protogen binary (required)")
	flag.StringVar(&gen.goimportsBin, "goimports-bin", "", "Path to goimports binary (required)")
	flag.StringVar(&gen.mockgenBin, "mockgen-bin", "", "Path to mockgen binary (required)")
	flag.StringVar(&gen.protocGenGoBin, "protoc-gen-go-bin", "", "Path to protoc-gen-go binary (required)")
	flag.StringVar(&gen.protocGenGoGrpcBin, "protoc-gen-go-grpc-bin", "", "Path to protoc-gen-go-grpc binary (required)")
	flag.StringVar(&gen.protocGenGoChasmBin, "protoc-gen-go-chasm-bin", "", "Path to protoc-gen-go-chasm binary (required)")
	flag.StringVar(&gen.protocGenGoHelpersBin, "protoc-gen-go-helpers-bin", "", "Path to protoc-gen-go-helpers binary (required)")
	flag.Parse()

	// Validate required flags
	if gen.protoOut == "" || gen.protoRoot == "" || gen.rootDir == "" || gen.apiBinpb == "" ||
		gen.protogenBin == "" || gen.goimportsBin == "" || gen.mockgenBin == "" ||
		gen.protocGenGoBin == "" || gen.protocGenGoGrpcBin == "" || gen.protocGenGoHelpersBin == "" ||
		gen.protocGenGoChasmBin == "" {
		flag.Usage()
		return nil, errors.New("all flags are required")
	}

	chasmDir := filepath.Join(gen.rootDir, "chasm", "lib")

	ls, err := os.ReadDir(chasmDir)
	if err != nil {
		return nil, fmt.Errorf("error reading chasm/lib directory %s: %w", chasmDir, err)
	}
	gen.chasmLibDirs = make([]string, 0, len(ls))
	for _, entry := range ls {
		if !entry.IsDir() {
			continue
		}
		gen.chasmLibDirs = append(gen.chasmLibDirs, filepath.Join(chasmDir, entry.Name()))
	}
	gen.tempOut = gen.protoOut + ".new"
	gen.tempProtoRoot = gen.protoRoot + ".tmp"
	gen.protoBackup = gen.protoOut + ".old"
	return &gen, nil
}

func (g *generator) removeExistingGenDirs() error {
	for _, dir := range g.chasmLibDirs {
		genDir := filepath.Join(dir, "gen")
		if err := os.RemoveAll(genDir); err != nil {
			return fmt.Errorf("error removing directory %s: %w", genDir, err)
		}
	}
	return nil
}

func (g *generator) backupProtos() error {
	if exists(g.protoOut) {
		if err := os.Rename(g.protoOut, g.protoBackup); err != nil {
			return fmt.Errorf("error backing up proto output directory %s to %s: %w", g.protoOut, g.protoBackup, err)
		}
	}
	return nil
}

func (g *generator) prepareTempDirs() error {
	// Remove and create new directory
	if err := os.RemoveAll(g.tempOut); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("error removing directory %s: %w", g.tempOut, err)
	}
	if err := os.MkdirAll(g.tempOut, 0755); err != nil {
		return fmt.Errorf("error creating directory %s: %w", g.tempOut, err)
	}
	if err := os.RemoveAll(g.tempProtoRoot); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("error removing directory %s: %w", g.tempOut, err)
	}
	if err := copyRecursive(g.protoRoot, g.tempProtoRoot); err != nil {
		return fmt.Errorf("error copying proto root %s to %s: %w", g.protoRoot, g.tempProtoRoot, err)
	}
	return nil
}

func (g *generator) copyChasmLibProtos() error {
	for _, dir := range g.chasmLibDirs {
		protoDir := filepath.Join(dir, "proto")
		if !existsDir(protoDir) {
			continue
		}
		destDir := filepath.Join(g.tempProtoRoot, "internal", "temporal", "server", "chasm", "lib", filepath.Base(dir))
		if err := os.MkdirAll(destDir, 0755); err != nil {
			return fmt.Errorf("error creating directory %s: %w", destDir, err)
		}
		if err := copyRecursive(protoDir, filepath.Join(destDir, "proto")); err != nil {
			return fmt.Errorf("error copying proto files from %s to %s: %w", protoDir, destDir, err)
		}
		protos, err := filepath.Glob(filepath.Join(destDir, "proto", "**", "*.proto"))
		if err != nil {
			return fmt.Errorf("error finding proto files in %s: %w", destDir, err)
		}
		for _, proto := range protos {
			if err := replaceInFile(proto, `import "chasm`, `import "temporal/server/chasm`); err != nil {
				return fmt.Errorf("error updating import path in %s: %w", proto, err)
			}
		}
	}
	return nil
}

func (g *generator) runProtogen(ctx context.Context) error {
	// Run protogen
	protoArgs := []string{
		"--descriptor_set_in=" + g.apiBinpb,
		"--root=" + filepath.Join(g.tempProtoRoot, "internal"),
		"--rewrite-enum=BuildId_State:BuildId",
		"--output=" + g.tempOut,
		"-p", "plugin=protoc-gen-go=" + g.protocGenGoBin,
		"-p", "plugin=protoc-gen-go-grpc=" + g.protocGenGoGrpcBin,
		"-p", "plugin=protoc-gen-go-helpers=" + g.protocGenGoHelpersBin,
		"-p", "plugin=protoc-gen-go-chasm=" + g.protocGenGoChasmBin,
		"-p", "go-grpc_out=paths=source_relative:" + g.tempOut,
		"-p", "go-helpers_out=paths=source_relative:" + g.tempOut,
		"-p", "go-chasm_out=paths=source_relative:" + g.tempOut,
	}
	if err := runCommand(ctx, g.protogenBin, protoArgs...); err != nil {
		return fmt.Errorf("error running protogen: %w", err)
	}
	return nil
}

func (g *generator) runGoImports(ctx context.Context) error {
	if err := runCommand(ctx, g.goimportsBin, "-w", g.tempOut); err != nil {
		return fmt.Errorf("error running goimports: %w", err)
	}
	return nil
}

func (g *generator) generateProtoMocks(ctx context.Context) error {
	// Find service.pb.go and service_grpc.pb.go files
	return filepath.Walk(g.tempOut, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// Don't generate mocks for chasm lib files, it's not needed and broken at the moment.
		if strings.HasPrefix(path, "api.new/temporal/server/chasm/lib") {
			return nil
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

			if err := runCommand(ctx, g.mockgenBin, mockgenArgs...); err != nil {
				return fmt.Errorf("error running mockgen: %v", err)
			}

			// Fix imports in the generated mock file
			content, err := os.ReadFile(dst)
			if err != nil {
				return fmt.Errorf("error reading file %s: %v", dst, err)
			}

			// Replace the incorrect import path
			newContent := strings.ReplaceAll(string(content), g.tempOut+"/temporal/server/", "")

			if err := os.WriteFile(dst, []byte(newContent), 0644); err != nil {
				return fmt.Errorf("error writing file %s: %v", dst, err)
			}
		}
		return nil
	})
}

func (g *generator) modifyHistoryServiceFile() error {
	historyServiceFile := filepath.Join(g.tempOut, "temporal", "server", "api", "historyservice", "v1", "service_grpc.pb.go")

	// Replace GetWorkflowExecutionHistory method signature
	err := replaceInFile(historyServiceFile,
		`GetWorkflowExecutionHistory\(context\.Context, \*GetWorkflowExecutionHistoryRequest\) \(\*GetWorkflowExecutionHistoryResponse, error\)`,
		`GetWorkflowExecutionHistory(context.Context, *GetWorkflowExecutionHistoryRequest) (*GetWorkflowExecutionHistoryResponseWithRaw, error)`)
	if err != nil {
		return fmt.Errorf("error modifying GetWorkflowExecutionHistory: %w", err)
	}

	// Replace RecordWorkflowTaskStarted method signature
	err = replaceInFile(historyServiceFile,
		`RecordWorkflowTaskStarted\(context\.Context, \*RecordWorkflowTaskStartedRequest\) \(\*RecordWorkflowTaskStartedResponse, error\)`,
		`RecordWorkflowTaskStarted(context.Context, *RecordWorkflowTaskStartedRequest) (*RecordWorkflowTaskStartedResponseWithRawHistory, error)`)
	if err != nil {
		return fmt.Errorf("error modifying RecordWorkflowTaskStarted: %w", err)
	}

	return nil
}

func (g *generator) moveProtoFiles() error {
	sourceApiDir := filepath.Join(g.tempOut, "temporal", "server", "api")
	return os.Rename(sourceApiDir, g.protoOut)
}

func (g *generator) moveGeneratedChasmFiles() error {
	sourceChasmDir := filepath.Join(g.tempOut, "temporal", "server", "chasm")

	return filepath.Walk(sourceChasmDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("error walking source chasm directory %s: %w", sourceChasmDir, err)
		}

		// Calculate relative path from sourceChasmDir
		relPath, err := filepath.Rel(sourceChasmDir, path)
		if err != nil {
			return err
		}
		// Transform relPath to match the destination structure
		// e.g., "lib/<name>/proto/v1/*" -> "lib/<name>/gen/<name>pb/*"
		re := regexp.MustCompile(`^lib/([^/]+)/proto/(v\d+)/(.*)$`)
		parts := re.FindStringSubmatch(relPath)
		if len(parts) < 4 {
			return nil
		}
		dstPath := filepath.Join("chasm", "lib", parts[1], "gen", parts[1]+"pb", parts[2], parts[3])
		dirName := filepath.Dir(dstPath) // Ensure the destination path is a base path

		if err := os.MkdirAll(dirName, 0755); err != nil {
			return fmt.Errorf("error creating directory %s: %w", dirName, err)
		}

		if err := os.Rename(path, dstPath); err != nil {
			return fmt.Errorf("error moving file %s to %s: %w", path, dstPath, err)
		}
		return nil
	})
}

func (g *generator) cleanup(restoreOld bool) error {
	// Remove temporary directories
	if err := os.RemoveAll(g.tempProtoRoot); err != nil {
		return fmt.Errorf("error removing temporary proto root %s: %w", g.tempProtoRoot, err)
	}
	if err := os.RemoveAll(g.tempOut); err != nil {
		return fmt.Errorf("error removing temporary output directory %s: %w", g.tempOut, err)
	}
	if restoreOld {
		// Restore the old proto output directory
		if exists(g.protoBackup) {
			if err := os.Rename(g.protoBackup, g.protoOut); err != nil {
				return fmt.Errorf("error restoring proto output directory from %s to %s: %w", g.protoBackup, g.protoOut, err)
			}
		}
	} else {
		// Remove the old proto output directory if it exists
		if err := os.RemoveAll(g.protoBackup); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("error removing backup proto output directory %s: %w", g.protoBackup, err)
		}
	}

	return nil
}

func main() {
	ctx := context.Background()
	gen, err := newGenerator()
	if err != nil {
		log.Fatalln("Error creating generator:", err)
	}

	info("Removing existing CHASM gen directories...")
	if err := gen.removeExistingGenDirs(); err != nil {
		log.Fatalln("Error removing existing CHASM gen directories:", err)
	}

	info("Backing up existing proto output directory...")
	if err := gen.backupProtos(); err != nil {
		log.Fatalln("Error backing up proto output directory:", err)
	}

	genErr := generate(ctx, gen)
	cleanupErr := gen.cleanup(genErr != nil)
	if cleanupErr != nil {
		cleanupErr = fmt.Errorf("error cleaning up after generation: %w", cleanupErr)
	}
	err = errors.Join(genErr, cleanupErr)
	if err != nil {
		log.Fatalln("Generation failed:", err)
	}
}

func generate(ctx context.Context, gen *generator) error {
	info("Preparing temp directories...")
	if err := gen.prepareTempDirs(); err != nil {
		return fmt.Errorf("error preparing new gen directory: %w", err)
	}
	info("Copying CHASM lib protos...")
	if err := gen.copyChasmLibProtos(); err != nil {
		return fmt.Errorf("error copying CHASM lib protos: %w", err)
	}
	info("Running protoc for proto files...")
	if err := gen.runProtogen(ctx); err != nil {
		return fmt.Errorf("error running protogen: %w", err)
	}
	info("Running goimports for proto files...")
	if err := gen.runGoImports(ctx); err != nil {
		return fmt.Errorf("error running goimports: %w", err)
	}
	info("Generating proto mocks...")
	if err := gen.generateProtoMocks(ctx); err != nil {
		return fmt.Errorf("error generating mock files: %w", err)
	}
	info("Modifying history service server interface...")
	if err := gen.modifyHistoryServiceFile(); err != nil {
		return fmt.Errorf("error modifying history service file: %w", err)
	}
	info("Moving proto files into place...")
	if err := gen.moveProtoFiles(); err != nil {
		return fmt.Errorf("error moving proto files: %w", err)
	}
	info("Moving generated CHASM files into place...")
	if err := gen.moveGeneratedChasmFiles(); err != nil {
		return fmt.Errorf("error moving CHASM files: %w", err)
	}
	return nil
}
