const fs = require("fs");
const path = require("path");
const { spawnSync } = require("child_process");

module.exports = ({ availableArchs }) => {
  const CLI_VERSION = "1.5.1";
  const VALID_ARCHS = ["amd64", "arm64"];

  // Filter to only valid architectures
  const validArchs = availableArchs.filter((arch) => VALID_ARCHS.includes(arch));

  for (const arch of validArchs) {
    const tarballName = `temporal_cli_${CLI_VERSION}_linux_${arch}.tar.gz`;
    const downloadUrl = `https://github.com/temporalio/cli/releases/download/v${CLI_VERSION}/${tarballName}`;

    console.log(`Downloading Temporal CLI for ${arch} from ${downloadUrl}`);

    const tempDir = `/tmp/temporal-cli-${arch}`;
    const tarballPath = `/tmp/${tarballName}`;

    // Download using curl (spawnSync with separate args prevents command injection)
    const downloadResult = spawnSync("curl", ["-sSL", downloadUrl, "-o", tarballPath], { stdio: "inherit" });
    if (downloadResult.error || downloadResult.status !== 0) {
      throw new Error(`Failed to download CLI: ${downloadResult.error || `exit code ${downloadResult.status}`}`);
    }

    // Create temp directory
    if (!fs.existsSync(tempDir)) {
      fs.mkdirSync(tempDir, { recursive: true });
    }

    // Extract tarball using tar (spawnSync with separate args)
    const extractResult = spawnSync("tar", ["-xzf", tarballPath, "-C", tempDir], { stdio: "inherit" });
    if (extractResult.error || extractResult.status !== 0) {
      throw new Error(`Failed to extract CLI: ${extractResult.error || `exit code ${extractResult.status}`}`);
    }

    // Move to build directory
    const destDir = `build/${arch}`;
    if (!fs.existsSync(destDir)) {
      fs.mkdirSync(destDir, { recursive: true });
    }

    const sourcePath = path.join(tempDir, "temporal");
    const destPath = path.join(destDir, "temporal");

    fs.renameSync(sourcePath, destPath);
    fs.chmodSync(destPath, 0o755);

    console.log(`Installed Temporal CLI to ${destPath}`);

    // Cleanup using fs instead of shell commands
    if (fs.existsSync(tarballPath)) {
      fs.unlinkSync(tarballPath);
    }
    if (fs.existsSync(tempDir)) {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  }
};
