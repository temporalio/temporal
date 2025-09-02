package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"

	"go.temporal.io/server/tools/tdbg/commandsgen"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	// Get commands dir
	_, file, _, _ := runtime.Caller(0)
	commandsDir := filepath.Join(file, "../../../../../tools/tdbg")

	// Parse YAML
	cmds, err := commandsgen.ParseCommands()
	if err != nil {
		return fmt.Errorf("failed parsing yml: %w", err)
	}

	// Generate code
	b, err := commandsgen.GenerateCommandsCode("tdbg", cmds)
	if err != nil {
		return fmt.Errorf("failed generating code: %w", err)
	}

	// Write
	if err := os.WriteFile(filepath.Join(commandsDir, "commands_gen.go"), b, 0644); err != nil {
		return fmt.Errorf("failed writing file: %w", err)
	}
	return nil
}
