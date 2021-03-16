## Release artifacts

Release binaries are created using [GoReleaser](https://goreleaser.com/)

GoReleaser github action is configured to attach release binaries on a Github release event [Release action](.github/workflows/goreleaser.yml)

GoReleaser configuration is at [.goreleaser.yml](.goreleaser.yml)


Locally build snapshot binaries
```bash
./develop/scripts/goreleaser.sh --snapshot --rm-dist
```

Locally build release binaries
```bash
./develop/scripts/goreleaser.sh
```
