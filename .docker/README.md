# Temporal Docker Build System

This directory contains everything needed to build Temporal's Docker images using goreleaser and GHCR.

## Overview

The build system creates two main Docker images:
- **server**: Contains temporal-server and temporal CLI
- **admin-tools**: Contains temporal CLI, tdbg, and database tools (cassandra-tool, sql-tool, elasticsearch-tool)

## Local Development

### Prerequisites
- Docker with buildx support
- Go 1.21+
- goreleaser (`brew install --cask goreleaser`)
- make

### Building Images Locally

```bash
cd .docker

# Build native architecture images (fast)
make build-native

# Build multi-arch images (amd64 + arm64)
make build

# Test images
make test

# Clean build artifacts
make clean
```

### Manual Build Steps

```bash
# 1. Build binaries with goreleaser
make goreleaser-bins

# 2. Download temporal CLI from GitHub releases
make download-cli

# 3. Organize binaries for Docker
make organize-bins

# 4. Build Docker images
make build-native
```

## CI/CD Workflows

### Automatic Builds

The `.github/workflows/docker-build.yml` workflow automatically builds and pushes images to GHCR on:
- Push to `main` branch
- Push to `test/*`, `cloud/*`, `feature/*`, `release/*` branches
- Pull requests (build only, no push)

**Images are tagged with:**
- `sha-<commit>` (e.g., `sha-abc1234`)
- `branch-<branch-name>` (e.g., `branch-main`)
- `latest` (only for main branch)

### Manual Releases

Use `.github/workflows/release-docker-images.yml` to promote images to production:

```bash
# Promote a pre-built image to release registry
# Input: commit SHA (first 7 chars), version tag, flags for latest/major
```

### Base Image Updates

Use `.github/workflows/release-base-images.yml` to build new base images when dependencies change.

## Directory Structure

```
.docker/
├── base-images/          # Base image Dockerfiles
│   ├── base-server.Dockerfile
│   └── base-admin-tools.Dockerfile
├── server.Dockerfile     # Server image
├── admin-tools.Dockerfile # Admin tools image
├── docker-bake.hcl       # Docker buildx bake configuration
├── scripts/              # Runtime scripts
│   ├── entrypoint.sh
│   ├── start-temporal.sh
│   └── test.sh
├── dist/                 # Build output (gitignored)
│   ├── amd64/           # Linux amd64 binaries
│   └── arm64/           # Linux arm64 binaries
├── .cli-version          # Temporal CLI version to download
├── docker-compose.yml    # Testing compose file
├── Makefile             # Build automation
└── README.md            # This file
```

## Configuration

### Temporal CLI Version

Update `.cli-version` to change which temporal CLI version is downloaded:

```bash
echo "v1.6.0" > .cli-version
```

### Image Registry

The default registry is `ghcr.io/chaptersix` for testing. To change:

**In docker-bake.hcl:**
```hcl
variable "IMAGE_REPO" {
  default = "ghcr.io/your-org"  # or "dockerhub-username"
}
```

**In workflows:**
Update the `IMAGE_REPO` environment variable or registry login steps.

## Testing

### Local Testing

```bash
# Build and test
make build-native test

# Run specific tests
docker run --rm ghcr.io/chaptersix/server:sha-test temporal-server --version
docker run --rm ghcr.io/chaptersix/admin-tools:sha-test temporal --version
```

### Integration Testing

Use docker-compose for integration tests:

```bash
IMAGE_SHA_TAG=sha-test docker compose up -d
docker compose exec admin-tools temporal operator cluster health
docker compose down
```

## Goreleaser Configuration

The build uses `../.goreleaser.yml` to build temporal binaries:
- temporal-server
- temporal-cassandra-tool
- temporal-sql-tool
- temporal-elasticsearch-tool
- tdbg

**Note:** The goreleaser config was updated to v2 format. Key changes:
- Added `version: 2` at the top
- Changed `changelog.skip` to `changelog.disable`

## Troubleshooting

### Goreleaser build fails
```bash
# Check goreleaser version
goreleaser --version

# Validate config
goreleaser check
```

### Binaries not found in Docker build
```bash
# Check organized binaries
ls -la dist/amd64/ dist/arm64/

# Manually run organize-bins
make organize-bins
```

### CLI download fails
```bash
# Check version in .cli-version
cat .cli-version

# Verify release exists
curl -I https://github.com/temporalio/cli/releases/download/$(cat .cli-version)/temporal_cli_$(cat .cli-version | sed 's/^v//')_linux_amd64.tar.gz
```

### Docker build context errors
The Dockerfiles expect to be run from the temporal repo root, not from `.docker/`.
The Makefile handles this automatically with `cd ..`.

## Migration Notes

This docker build system replaces the docker-builds repository approach:

**Old approach:**
- Separate docker-builds repo with submodules
- Built CLI and tctl from source
- Cross-repo workflow triggers

**New approach:**
- Everything in temporal repo
- Downloads CLI from releases
- No tctl (deprecated)
- Uses goreleaser for all temporal binaries
- Simpler, single-repo workflow

## Contributing

When making changes:
1. Test locally with `make build-native test`
2. Verify goreleaser config still works
3. Update this README if adding new features
4. Check that both amd64 and arm64 builds work
