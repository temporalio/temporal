const fs = require("fs");
const path = require("path");

module.exports = ({ core }) => {
  // Helper function to validate paths and prevent traversal
  const validatePath = (filePath, allowedPrefix) => {
    const normalized = path.normalize(filePath);
    const resolved = path.resolve(normalized);
    const allowedResolved = path.resolve(allowedPrefix);

    // Check for path traversal
    if (normalized.includes("..")) {
      throw new Error(`Path traversal detected in: ${filePath}`);
    }

    // Ensure path is within allowed directory
    if (!resolved.startsWith(allowedResolved)) {
      throw new Error(`Path outside allowed directory: ${filePath}`);
    }

    return normalized;
  };

  // Create build directories in docker/build
  const archs = ["amd64", "arm64"];
  const binaries = [
    "temporal-server",
    "temporal-cassandra-tool",
    "temporal-sql-tool",
    "temporal-elasticsearch-tool",
    "tdbg",
  ];

  // Validate architecture and binary names
  archs.forEach((arch) => {
    if (!/^[a-z0-9]+$/.test(arch)) {
      throw new Error(`Invalid architecture name: ${arch}`);
    }
  });

  binaries.forEach((binary) => {
    if (!/^[a-z0-9-]+$/.test(binary)) {
      throw new Error(`Invalid binary name: ${binary}`);
    }
  });

  archs.forEach((arch) => {
    const dir = `docker/build/${arch}`;
    validatePath(dir, "docker/build");
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
  });

  // Map GoReleaser dist structure to build structure
  const archMap = {
    amd64: "amd64_v1",
    arm64: "arm64",
  };

  binaries.forEach((binary) => {
    archs.forEach((arch) => {
      const distArch = archMap[arch] || arch;
      const distPath = `dist/${binary}_linux_${distArch}/${binary}`;
      const buildPath = `docker/build/${arch}/${binary}`;

      // Validate paths before file operations
      try {
        validatePath(distPath, "dist");
        validatePath(buildPath, "docker/build");
      } catch (error) {
        console.error(`Path validation failed: ${error.message}`);
        throw error;
      }

      if (fs.existsSync(distPath)) {
        fs.copyFileSync(distPath, buildPath);
        fs.chmodSync(buildPath, 0o755);
        console.log(`Copied ${distPath} -> ${buildPath}`);
      } else {
        console.warn(`Binary not found: ${binary} for ${arch}`);
      }
    });
  });

  // Copy schema directory for admin-tools
  const schemaDir = "docker/build/temporal/schema";
  validatePath(schemaDir, "docker/build");
  if (!fs.existsSync(schemaDir)) {
    fs.mkdirSync(schemaDir, { recursive: true });
  }

  // Copy all schema files recursively with path validation
  const copyRecursive = (src, dest) => {
    // Validate both source and destination
    validatePath(src, "schema");
    validatePath(dest, "docker/build");

    if (fs.statSync(src).isDirectory()) {
      if (!fs.existsSync(dest)) {
        fs.mkdirSync(dest, { recursive: true });
      }
      fs.readdirSync(src).forEach((item) => {
        // Validate item name to prevent directory traversal
        if (item.includes("..") || item.includes("/") || item.includes("\\")) {
          throw new Error(`Invalid file name: ${item}`);
        }
        copyRecursive(path.join(src, item), path.join(dest, item));
      });
    } else {
      fs.copyFileSync(src, dest);
    }
  };

  if (fs.existsSync("schema")) {
    copyRecursive("schema", schemaDir);
    console.log("Copied schema directory");
  }

  // Validate required binaries for Docker images
  console.log("\nValidating required binaries for Docker images...");

  // Required for admin-tools image
  const requiredBinaries = [
    "temporal-server",
    "temporal-cassandra-tool",
    "temporal-sql-tool",
    "temporal-elasticsearch-tool",
    "tdbg",
  ];

  // Check which architectures have binaries
  const availableArchs = ["amd64", "arm64"].filter((arch) => {
    const testBinary = `docker/build/${arch}/temporal-server`;
    return fs.existsSync(testBinary);
  });

  if (availableArchs.length === 0) {
    throw new Error("❌ No binaries found for any architecture");
  }

  console.log(`Found binaries for architectures: ${availableArchs.join(", ")}`);

  // Validate that each available architecture has all required binaries
  let missingFiles = false;
  availableArchs.forEach((arch) => {
    requiredBinaries.forEach((binary) => {
      const binaryPath = `docker/build/${arch}/${binary}`;
      if (!fs.existsSync(binaryPath)) {
        console.error(`Error: Missing ${binaryPath}`);
        missingFiles = true;
      }
    });
  });

  // Validate schema directory exists
  if (!fs.existsSync("docker/build/temporal/schema")) {
    console.error("Error: Missing docker/build/temporal/schema directory");
    missingFiles = true;
  }

  if (missingFiles) {
    throw new Error("❌ Binary validation failed");
  }

  console.log("✓ All required binaries present for available architectures");

  // Export available architectures for Docker build
  core.setOutput("available-archs", availableArchs.join(","));
};
