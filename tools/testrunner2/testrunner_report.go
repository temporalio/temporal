package testrunner2

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func reportLogs(logDir string, logf func(string, ...any)) error {
	// Read all package directories
	entries, err := os.ReadDir(logDir)
	if err != nil {
		return fmt.Errorf("failed to read log directory: %w", err)
	}

	if len(entries) == 0 {
		logf("no log files found in %s", logDir)
		return nil
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		pkgName := entry.Name()
		pkgDir := filepath.Join(logDir, pkgName)

		// Read log files in package directory
		logFiles, err := os.ReadDir(pkgDir)
		if err != nil {
			logf("warning: failed to read package directory %s: %v", pkgDir, err)
			continue
		}

		for _, logFile := range logFiles {
			if logFile.IsDir() || !strings.HasSuffix(logFile.Name(), ".log") {
				continue
			}

			logPath := filepath.Join(pkgDir, logFile.Name())
			if err := printLogFile(logPath, pkgName, logFile.Name()); err != nil {
				logf("warning: failed to print log file %s: %v", logPath, err)
			}
		}
	}

	return nil
}

func printLogFile(path, pkgName, fileName string) error {
	content, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	// Skip empty files
	if len(content) == 0 {
		return nil
	}

	fmt.Printf("\n=== %s/%s ===\n", pkgName, fileName)
	fmt.Print(string(content))

	// Ensure content ends with newline
	if len(content) > 0 && content[len(content)-1] != '\n' {
		fmt.Println()
	}

	return nil
}
