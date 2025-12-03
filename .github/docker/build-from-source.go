package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// buildFromSource builds binaries from source by cloning temporal repo at specific SHA using goreleaser
func main() {
	// Parse command line arguments
	cliVersion := flag.String("cli-version", "", "CLI version (e.g., 1.5.0)")
	arch := flag.String("arch", "amd64", "Architecture (amd64 or arm64)")
	temporalSHA := flag.String("temporal-sha", "", "Temporal SHA (defaults to latest main)")
	flag.Parse()

	if *cliVersion == "" {
		fmt.Fprintln(os.Stderr, "Error: --cli-version is required")
		flag.Usage()
		os.Exit(1)
	}

	// Get script directory
	scriptDir, err := os.Getwd()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting working directory: %v\n", err)
		os.Exit(1)
	}

	buildDir := filepath.Join(scriptDir, "build", *arch)
	tempDir := filepath.Join(scriptDir, "build", "temp")
	temporalCloneDir := filepath.Join(scriptDir, "build", "temporal")

	// Get latest SHA from main if not specified
	sha := *temporalSHA
	if sha == "" {
		cmd := exec.Command("git", "ls-remote", "https://github.com/temporalio/temporal.git", "HEAD")
		output, err := cmd.Output()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error getting latest SHA: %v\n", err)
			os.Exit(1)
		}
		sha = strings.Fields(string(output))[0]
	}

	fmt.Printf("Building binaries from source for %s...\n", *arch)
	fmt.Printf("  Temporal SHA: %s\n", sha)
	fmt.Printf("  CLI version: %s\n", *cliVersion)

	// Create build directories
	if err := os.MkdirAll(buildDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating build directory: %v\n", err)
		os.Exit(1)
	}
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating temp directory: %v\n", err)
		os.Exit(1)
	}

	// Clone or update temporal repository
	gitDir := filepath.Join(temporalCloneDir, ".git")
	if _, err := os.Stat(gitDir); os.IsNotExist(err) {
		fmt.Printf("Cloning temporal repository at SHA %s...\n", sha)
		os.RemoveAll(temporalCloneDir)

		if err := runCommand("git", "clone", "https://github.com/temporalio/temporal.git", temporalCloneDir); err != nil {
			fmt.Fprintf(os.Stderr, "Error cloning repository: %v\n", err)
			os.Exit(1)
		}

		if err := os.Chdir(temporalCloneDir); err != nil {
			fmt.Fprintf(os.Stderr, "Error changing to temporal directory: %v\n", err)
			os.Exit(1)
		}

		if err := runCommand("git", "checkout", sha); err != nil {
			fmt.Fprintf(os.Stderr, "Error checking out SHA: %v\n", err)
			os.Exit(1)
		}
	} else {
		fmt.Printf("Using existing temporal clone at %s\n", temporalCloneDir)

		if err := os.Chdir(temporalCloneDir); err != nil {
			fmt.Fprintf(os.Stderr, "Error changing to temporal directory: %v\n", err)
			os.Exit(1)
		}

		// Check current SHA
		cmd := exec.Command("git", "rev-parse", "HEAD")
		output, err := cmd.Output()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error getting current SHA: %v\n", err)
			os.Exit(1)
		}
		currentSHA := strings.TrimSpace(string(output))

		if currentSHA != sha {
			fmt.Printf("Updating temporal clone to SHA %s...\n", sha)
			if err := runCommand("git", "fetch", "origin"); err != nil {
				fmt.Fprintf(os.Stderr, "Error fetching updates: %v\n", err)
				os.Exit(1)
			}
			if err := runCommand("git", "checkout", sha); err != nil {
				fmt.Fprintf(os.Stderr, "Error checking out SHA: %v\n", err)
				os.Exit(1)
			}
		}
	}

	// Verify elasticsearch tool exists
	elasticsearchToolPath := filepath.Join(temporalCloneDir, "cmd", "tools", "elasticsearch")
	if _, err := os.Stat(elasticsearchToolPath); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Error: temporal-elasticsearch-tool source not found at %s\n", elasticsearchToolPath)
		os.Exit(1)
	}

	// Use goreleaser from PATH
	goreleaserBin := "goreleaser"

	// Build binaries using goreleaser
	fmt.Printf("Building temporal binaries with goreleaser for linux/%s...\n", *arch)

	binaries := []struct {
		id     string
		output string
	}{
		{"temporal-server", "temporal-server"},
		{"temporal-cassandra-tool", "temporal-cassandra-tool"},
		{"temporal-sql-tool", "temporal-sql-tool"},
		{"temporal-elasticsearch-tool", "temporal-elasticsearch-tool"},
		{"tdbg", "tdbg"},
	}

	for _, bin := range binaries {
		fmt.Printf("Building %s...\n", bin.id)
		outputPath := filepath.Join(buildDir, bin.output)

		cmd := exec.Command(goreleaserBin, "build", "--single-target", "--id", bin.id, "--output", outputPath, "--snapshot", "--clean")
		cmd.Env = append(os.Environ(),
			"GOOS=linux",
			fmt.Sprintf("GOARCH=%s", *arch),
		)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Dir = temporalCloneDir

		if err := cmd.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "Error building %s: %v\n", bin.id, err)
			os.Exit(1)
		}
	}

	// Download temporal CLI
	fmt.Println("Downloading temporal CLI...")
	cliURL := fmt.Sprintf("https://github.com/temporalio/cli/releases/download/v%s/temporal_cli_%s_linux_%s.tar.gz", *cliVersion, *cliVersion, *arch)

	cmd := exec.Command("sh", "-c", fmt.Sprintf("curl -fsSL %s | tar -xz -C %s temporal", cliURL, buildDir))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error downloading temporal CLI: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Schema directory available at %s\n", filepath.Join(temporalCloneDir, "schema"))
	fmt.Printf("Done building binaries for %s\n", *arch)

	// List built binaries
	if err := runCommand("ls", "-lh", buildDir); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: could not list build directory: %v\n", err)
	}
}

func runCommand(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
