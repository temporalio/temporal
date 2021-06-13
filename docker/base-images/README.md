### Build and push base docker images using `docker build`

To build a new version of base docker image run:

```bash
make <base_image_name> DOCKER_IMAGE_TAG=<new_base_image_version>
```

Check [MakeFile](Makefile) for all possible `base_image_name` options.

For example:

```bash
make base-builder DOCKER_IMAGE_TAG=1.3.0
```

### Build and push base docker images for `linux/arm64` using `docker buildx`

Learn more about `docker buildx` from the official [doc](https://docs.docker.com/buildx/working-with-buildx/).

Create builder container once:
```bash
make buildx-docker-container
```

Add `-x` suffix to build target names tu build using `buildx` for both `linux/amd64` and `linux/arm64`.

For example:

```bash
make base-builder-x DOCKER_IMAGE_TAG=1.3.0
```

### Test base docker images for `linux/arm64` using on `linux/amd64`

Run [qemu-user-static](https://github.com/multiarch/qemu-user-static) first:
```bash
docker run --rm --privileged multiarch/qemu-user-static --reset -p yes
```

Then run base image. For example:
```bash
docker run --rm -it --platform linux/arm64 temporalio/base-server:1.1.0 uname -m
```