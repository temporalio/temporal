package main

import (
	"archive/tar"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func main() {
	cliVersion := flag.String("cli-version", "", "CLI version (e.g., 1.5.0)")
	temporalSHA := flag.String("temporal-sha", "", "Temporal SHA (defaults to latest main)")
	imageRepo := flag.String("image-repo", "temporaliotest", "Docker image repository")
	tagLatest := flag.Bool("tag-latest", false, "Tag images as latest")
	buildTarget := flag.String("target", "", "Build target: server, admin-tools, or empty for both")
	flag.Parse()

	if *cliVersion == "" {
		fmt.Fprintln(os.Stderr, "Error: --cli-version is required")
		flag.Usage()
		os.Exit(1)
	}

	scriptDir, err := os.Getwd()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting working directory: %v\n", err)
		os.Exit(1)
	}

	temporalCloneDir := filepath.Join(scriptDir, "build", "temporal")

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

	fmt.Printf("Building binaries from source...\n")
	fmt.Printf("  Temporal SHA: %s\n", sha)
	fmt.Printf("  CLI version: %s\n", *cliVersion)

	gitDir := filepath.Join(temporalCloneDir, ".git")
	if _, err := os.Stat(gitDir); os.IsNotExist(err) {
		fmt.Printf("Cloning temporal repository at SHA %s...\n", sha)
		os.RemoveAll(temporalCloneDir)

		if err := runCommand("git", "clone", "--depth=1", "--no-tags", "--filter=blob:none", "https://github.com/temporalio/temporal.git", temporalCloneDir); err != nil {
			fmt.Fprintf(os.Stderr, "Error cloning repository: %v\n", err)
			os.Exit(1)
		}

		if err := os.Chdir(temporalCloneDir); err != nil {
			fmt.Fprintf(os.Stderr, "Error changing to temporal directory: %v\n", err)
			os.Exit(1)
		}

		if err := runCommand("git", "fetch", "--depth=1", "origin", sha); err != nil {
			fmt.Fprintf(os.Stderr, "Error fetching commit: %v\n", err)
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

		cmd := exec.Command("git", "rev-parse", "HEAD")
		output, err := cmd.Output()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error getting current SHA: %v\n", err)
			os.Exit(1)
		}
		currentSHA := strings.TrimSpace(string(output))

		if currentSHA != sha {
			fmt.Printf("Updating temporal clone to SHA %s...\n", sha)
			if err := runCommand("git", "fetch", "--depth=1", "origin", sha); err != nil {
				fmt.Fprintf(os.Stderr, "Error fetching commit: %v\n", err)
				os.Exit(1)
			}
			if err := runCommand("git", "checkout", sha); err != nil {
				fmt.Fprintf(os.Stderr, "Error checking out SHA: %v\n", err)
				os.Exit(1)
			}
		}
	}

	architectures := []string{"amd64", "arm64"}
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

	for _, arch := range architectures {
		buildDir := filepath.Join(scriptDir, "build", arch)

		fmt.Printf("\nBuilding temporal binaries with goreleaser for linux/%s...\n", arch)

		if err := os.MkdirAll(buildDir, 0755); err != nil {
			fmt.Fprintf(os.Stderr, "Error creating build directory: %v\n", err)
			os.Exit(1)
		}

		args := []string{"build", "--single-target", "--snapshot", "--clean"}
		for _, bin := range binaries {
			args = append(args, "--id", bin.id)
		}

		cmd := exec.Command("goreleaser", args...)
		cmd.Env = append(os.Environ(),
			"GOOS=linux",
			fmt.Sprintf("GOARCH=%s", arch),
		)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Dir = temporalCloneDir

		if err := cmd.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "Error building binaries: %v\n", err)
			os.Exit(1)
		}

		for _, bin := range binaries {
			srcPath := filepath.Join(temporalCloneDir, "dist", bin.output+"_linux_"+arch+"_v1", bin.output)
			if _, err := os.Stat(srcPath); os.IsNotExist(err) {
				srcPath = filepath.Join(temporalCloneDir, "dist", bin.output+"_linux_"+arch, bin.output)
			}
			destPath := filepath.Join(buildDir, bin.output)

			input, err := os.ReadFile(srcPath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error reading built binary %s: %v\n", bin.output, err)
				os.Exit(1)
			}
			if err := os.WriteFile(destPath, input, 0755); err != nil {
				fmt.Fprintf(os.Stderr, "Error copying binary %s: %v\n", bin.output, err)
				os.Exit(1)
			}
		}

		fmt.Println("Downloading temporal CLI...")
		cliURL := fmt.Sprintf("https://github.com/temporalio/cli/releases/download/v%s/temporal_cli_%s_linux_%s.tar.gz", *cliVersion, *cliVersion, arch)

		if err := downloadAndExtractTarGz(cliURL, buildDir, "temporal"); err != nil {
			fmt.Fprintf(os.Stderr, "Error downloading temporal CLI: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("Validating binaries for %s...\n", arch)
		expectedBinaries := append([]string{"temporal"}, func() []string {
			names := make([]string, len(binaries))
			for i, b := range binaries {
				names[i] = b.output
			}
			return names
		}()...)

		for _, binary := range expectedBinaries {
			binaryPath := filepath.Join(buildDir, binary)
			info, err := os.Stat(binaryPath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: binary %s not found at %s\n", binary, binaryPath)
				os.Exit(1)
			}
			if info.Size() == 0 {
				fmt.Fprintf(os.Stderr, "Error: binary %s is empty\n", binary)
				os.Exit(1)
			}
			if info.Mode().Perm()&0111 == 0 {
				fmt.Fprintf(os.Stderr, "Error: binary %s is not executable\n", binary)
				os.Exit(1)
			}
		}

		fmt.Printf("All binaries validated for %s\n", arch)

		if err := runCommand("ls", "-lh", buildDir); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: could not list build directory: %v\n", err)
		}
	}

	fmt.Printf("\nSchema directory available at %s\n", filepath.Join(temporalCloneDir, "schema"))
	fmt.Println("All architectures built successfully!")

	imageSHATag, err := exec.Command("git", "rev-parse", "--short", "HEAD").Output()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting git SHA: %v\n", err)
		os.Exit(1)
	}

	imageBranchTag, err := exec.Command("git", "rev-parse", "--abbrev-ref", "HEAD").Output()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting git branch: %v\n", err)
		os.Exit(1)
	}

	targets := []string{}
	if *buildTarget == "" {
		targets = []string{"server", "admin-tools"}
	} else {
		targets = []string{*buildTarget}
	}

	for _, target := range targets {
		fmt.Printf("\nBuilding Docker image for %s...\n", target)

		cmd := exec.Command("docker", "buildx", "bake", target)
		cmd.Env = append(os.Environ(),
			fmt.Sprintf("CLI_VERSION=%s", *cliVersion),
			fmt.Sprintf("IMAGE_REPO=%s", *imageRepo),
			fmt.Sprintf("IMAGE_SHA_TAG=%s", strings.TrimSpace(string(imageSHATag))),
			fmt.Sprintf("IMAGE_BRANCH_TAG=%s", strings.TrimSpace(string(imageBranchTag))),
			fmt.Sprintf("TEMPORAL_SHA=%s", sha),
			fmt.Sprintf("TAG_LATEST=%t", *tagLatest),
		)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Dir = scriptDir

		if err := cmd.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "Error building Docker image for %s: %v\n", target, err)
			os.Exit(1)
		}
	}

	fmt.Println("\nDocker images built successfully!")
}

func runCommand(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func downloadAndExtractTarGz(url, destDir, filename string) error {
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("failed to download: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to download: HTTP %d", resp.StatusCode)
	}

	gzipReader, err := gzip.NewReader(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer gzipReader.Close()

	tarReader := tar.NewReader(gzipReader)

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read tar: %w", err)
		}

		if header.Name == filename {
			destPath := filepath.Join(destDir, filename)
			outFile, err := os.OpenFile(destPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
			if err != nil {
				return fmt.Errorf("failed to create file: %w", err)
			}
			defer outFile.Close()

			if _, err := io.Copy(outFile, tarReader); err != nil {
				return fmt.Errorf("failed to write file: %w", err)
			}

			return nil
		}
	}

	return fmt.Errorf("file %s not found in archive", filename)
}
