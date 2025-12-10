package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
)

const cliVersion = "1.5.1"

var validArchs = []string{"amd64", "arm64"}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <command>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Commands:\n")
		fmt.Fprintf(os.Stderr, "  sanitize-tag      - Sanitize branch name for Docker tag\n")
		fmt.Fprintf(os.Stderr, "  organize-binaries - Organize binaries for Docker\n")
		fmt.Fprintf(os.Stderr, "  download-cli      - Download Temporal CLI\n")
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "sanitize-tag":
		if err := sanitizeTag(); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "organize-binaries":
		if err := organizeBinaries(); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "download-cli":
		if err := downloadCLI(); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", command)
		os.Exit(1)
	}
}

// sanitizeTag sanitizes branch names for Docker tags
func sanitizeTag() error {
	// Get GITHUB_REF from environment
	ref := os.Getenv("GITHUB_REF")
	if ref == "" {
		return fmt.Errorf("GITHUB_REF environment variable not set")
	}

	// Remove refs/heads/ or refs/tags/ prefix
	ref = strings.TrimPrefix(ref, "refs/heads/")
	ref = strings.TrimPrefix(ref, "refs/tags/")

	// Sanitize ref name first
	// Replace any non-alphanumeric (except .-_) with dash
	reg := regexp.MustCompile(`[^a-zA-Z0-9._-]`)
	sanitizedRef := reg.ReplaceAllString(ref, "-")

	// Collapse multiple consecutive dashes
	multiDashReg := regexp.MustCompile(`-+`)
	sanitizedRef = multiDashReg.ReplaceAllString(sanitizedRef, "-")

	// Remove leading and trailing dashes
	sanitizedRef = strings.Trim(sanitizedRef, "-")

	// Prefix with "branch-" for branch builds
	safeTag := fmt.Sprintf("branch-%s", sanitizedRef)

	// Docker tags must be lowercase
	safeTag = strings.ToLower(safeTag)

	// Truncate to 128 characters (Docker tag limit)
	if len(safeTag) > 128 {
		safeTag = safeTag[:128]
	}

	if safeTag == "" {
		return fmt.Errorf("failed to generate valid Docker tag from branch name")
	}

	fmt.Printf("Original: %s\n", ref)
	fmt.Printf("Sanitized: %s\n", safeTag)

	// Set output for GitHub Actions
	if err := setOutput("tag", safeTag); err != nil {
		return fmt.Errorf("failed to set output: %w", err)
	}

	return nil
}

// organizeBinaries organizes binaries for Docker builds
func organizeBinaries() error {
	// Create build directories in docker/build
	archs := []string{"amd64", "arm64"}
	binaries := []string{
		"temporal-server",
		"temporal-cassandra-tool",
		"temporal-sql-tool",
		"temporal-elasticsearch-tool",
		"tdbg",
	}

	// Validate architecture and binary names
	archReg := regexp.MustCompile(`^[a-z0-9]+$`)
	for _, arch := range archs {
		if !archReg.MatchString(arch) {
			return fmt.Errorf("invalid architecture name: %s", arch)
		}
	}

	binReg := regexp.MustCompile(`^[a-z0-9-]+$`)
	for _, binary := range binaries {
		if !binReg.MatchString(binary) {
			return fmt.Errorf("invalid binary name: %s", binary)
		}
	}

	// Create architecture directories
	for _, arch := range archs {
		dir := filepath.Join("docker", "build", arch)
		if err := validatePath(dir, "docker/build"); err != nil {
			return err
		}
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	// Map GoReleaser dist structure to build structure
	archMap := map[string]string{
		"amd64": "amd64_v1",
		"arm64": "arm64",
	}

	// Copy binaries
	for _, binary := range binaries {
		for _, arch := range archs {
			distArch := archMap[arch]
			distPath := filepath.Join("dist", fmt.Sprintf("%s_linux_%s", binary, distArch), binary)
			buildPath := filepath.Join("docker", "build", arch, binary)

			// Validate paths before file operations
			if err := validatePath(distPath, "dist"); err != nil {
				return fmt.Errorf("invalid dist path: %w", err)
			}
			if err := validatePath(buildPath, "docker/build"); err != nil {
				return fmt.Errorf("invalid build path: %w", err)
			}

			if _, err := os.Stat(distPath); err == nil {
				if err := copyFile(distPath, buildPath); err != nil {
					return fmt.Errorf("failed to copy %s to %s: %w", distPath, buildPath, err)
				}
				if err := os.Chmod(buildPath, 0755); err != nil {
					return fmt.Errorf("failed to chmod %s: %w", buildPath, err)
				}
				fmt.Printf("Copied %s -> %s\n", distPath, buildPath)
			} else {
				fmt.Printf("Warning: Binary not found: %s for %s\n", binary, arch)
			}
		}
	}

	// Copy schema directory for admin-tools
	schemaDir := filepath.Join("docker", "build", "temporal", "schema")
	if err := validatePath(schemaDir, "docker/build"); err != nil {
		return err
	}
	if err := os.MkdirAll(schemaDir, 0755); err != nil {
		return fmt.Errorf("failed to create schema directory: %w", err)
	}

	// Copy all schema files recursively with path validation
	if _, err := os.Stat("schema"); err == nil {
		if err := copyRecursive("schema", schemaDir); err != nil {
			return fmt.Errorf("failed to copy schema directory: %w", err)
		}
		fmt.Println("Copied schema directory")
	}

	// Validate required binaries for Docker images
	fmt.Println("\nValidating required binaries for Docker images...")

	// Check which architectures have binaries
	var availableArchs []string
	for _, arch := range archs {
		testBinary := filepath.Join("docker", "build", arch, "temporal-server")
		if _, err := os.Stat(testBinary); err == nil {
			availableArchs = append(availableArchs, arch)
		}
	}

	if len(availableArchs) == 0 {
		return fmt.Errorf("❌ No binaries found for any architecture")
	}

	fmt.Printf("Found binaries for architectures: %s\n", strings.Join(availableArchs, ", "))

	// Validate that each available architecture has all required binaries
	missingFiles := false
	for _, arch := range availableArchs {
		for _, binary := range binaries {
			binaryPath := filepath.Join("docker", "build", arch, binary)
			if _, err := os.Stat(binaryPath); err != nil {
				fmt.Fprintf(os.Stderr, "Error: Missing %s\n", binaryPath)
				missingFiles = true
			}
		}
	}

	// Validate schema directory exists
	if _, err := os.Stat(filepath.Join("docker", "build", "temporal", "schema")); err != nil {
		fmt.Fprintln(os.Stderr, "Error: Missing docker/build/temporal/schema directory")
		missingFiles = true
	}

	if missingFiles {
		return fmt.Errorf("❌ Binary validation failed")
	}

	fmt.Println("✓ All required binaries present for available architectures")

	// Export available architectures for Docker build
	if err := setOutput("available-archs", strings.Join(availableArchs, ",")); err != nil {
		return fmt.Errorf("failed to set output: %w", err)
	}

	return nil
}

// downloadCLI downloads the Temporal CLI for available architectures
func downloadCLI() error {
	// Get available architectures from environment or input
	availableArchsStr := os.Getenv("AVAILABLE_ARCHS")
	if availableArchsStr == "" {
		return fmt.Errorf("AVAILABLE_ARCHS environment variable not set")
	}

	availableArchs := strings.Split(availableArchsStr, ",")

	// Filter to only valid architectures
	var validAvailableArchs []string
	for _, arch := range availableArchs {
		arch = strings.TrimSpace(arch)
		for _, validArch := range validArchs {
			if arch == validArch {
				validAvailableArchs = append(validAvailableArchs, arch)
				break
			}
		}
	}

	if len(validAvailableArchs) == 0 {
		return fmt.Errorf("no valid architectures found in: %s", availableArchsStr)
	}

	for _, arch := range validAvailableArchs {
		if err := downloadCLIForArch(arch); err != nil {
			return fmt.Errorf("failed to download CLI for %s: %w", arch, err)
		}
	}

	return nil
}

func downloadCLIForArch(arch string) error {
	tarballName := fmt.Sprintf("temporal_cli_%s_linux_%s.tar.gz", cliVersion, arch)
	downloadURL := fmt.Sprintf("https://github.com/temporalio/cli/releases/download/v%s/%s", cliVersion, tarballName)

	fmt.Printf("Downloading Temporal CLI for %s from %s\n", arch, downloadURL)

	tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("temporal-cli-%s", arch))
	tarballPath := filepath.Join(os.TempDir(), tarballName)

	// Download tarball
	if err := downloadFile(downloadURL, tarballPath); err != nil {
		return fmt.Errorf("failed to download: %w", err)
	}
	defer os.Remove(tarballPath)

	// Create temp directory
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(tempDir)

	// Extract tarball
	cmd := exec.Command("tar", "-xzf", tarballPath, "-C", tempDir)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to extract: %w\nOutput: %s", err, string(output))
	}

	// Move to build directory
	destDir := filepath.Join("build", arch)
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return fmt.Errorf("failed to create build directory: %w", err)
	}

	sourcePath := filepath.Join(tempDir, "temporal")
	destPath := filepath.Join(destDir, "temporal")

	if err := os.Rename(sourcePath, destPath); err != nil {
		// If rename fails (e.g., cross-device), try copy and delete
		if err := copyFile(sourcePath, destPath); err != nil {
			return fmt.Errorf("failed to copy binary: %w", err)
		}
		os.Remove(sourcePath)
	}

	if err := os.Chmod(destPath, 0755); err != nil {
		return fmt.Errorf("failed to chmod binary: %w", err)
	}

	fmt.Printf("Installed Temporal CLI to %s\n", destPath)

	return nil
}

// Helper functions

func setOutput(name, value string) error {
	outputFile := os.Getenv("GITHUB_OUTPUT")
	if outputFile == "" {
		return fmt.Errorf("GITHUB_OUTPUT environment variable not set")
	}

	f, err := os.OpenFile(outputFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = fmt.Fprintf(f, "%s=%s\n", name, value)
	return err
}

func validatePath(path, allowedPrefix string) error {
	// Clean and resolve paths
	normalized := filepath.Clean(path)
	resolved, err := filepath.Abs(normalized)
	if err != nil {
		return fmt.Errorf("failed to resolve path: %w", err)
	}

	allowedResolved, err := filepath.Abs(allowedPrefix)
	if err != nil {
		return fmt.Errorf("failed to resolve allowed prefix: %w", err)
	}

	// Check for path traversal
	if strings.Contains(normalized, "..") {
		return fmt.Errorf("path traversal detected in: %s", path)
	}

	// Ensure path is within allowed directory
	if !strings.HasPrefix(resolved, allowedResolved) {
		return fmt.Errorf("path outside allowed directory: %s", path)
	}

	return nil
}

func copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	return err
}

func copyRecursive(src, dst string) error {
	// Validate paths
	if err := validatePath(src, "schema"); err != nil {
		return err
	}
	if err := validatePath(dst, "docker/build"); err != nil {
		return err
	}

	srcInfo, err := os.Stat(src)
	if err != nil {
		return err
	}

	if srcInfo.IsDir() {
		// Create destination directory
		if err := os.MkdirAll(dst, 0755); err != nil {
			return err
		}

		// Read directory entries
		entries, err := os.ReadDir(src)
		if err != nil {
			return err
		}

		// Copy each entry
		for _, entry := range entries {
			// Validate item name to prevent directory traversal
			if strings.Contains(entry.Name(), "..") || strings.Contains(entry.Name(), "/") || strings.Contains(entry.Name(), "\\") {
				return fmt.Errorf("invalid file name: %s", entry.Name())
			}

			srcPath := filepath.Join(src, entry.Name())
			dstPath := filepath.Join(dst, entry.Name())

			if err := copyRecursive(srcPath, dstPath); err != nil {
				return err
			}
		}
	} else {
		// Copy file
		if err := copyFile(src, dst); err != nil {
			return err
		}
	}

	return nil
}

func downloadFile(url, filepath string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status: %s", resp.Status)
	}

	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	return err
}
