### Build and push base docker images using `docker build`

To build a new version of base docker image run:
```bash
make <base_image_name> DOCKER_IMAGE_TAG=<new_base_image_version>
```

Check [Makefile](Makefile) for all possible `base_image_name` options.

For example:
```bash
make base-builder DOCKER_IMAGE_TAG=1.3.0
```

### Build base docker images for `linux/arm64` using `docker buildx`

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

Pass `DOCKER_BUILDX_OUTPUT=registry` to automatically push image to the docker hub:
```bash
make base-builder-x DOCKER_IMAGE_TAG=1.3.0 DOCKER_BUILDX_OUTPUT=registry
```

Run:
```bash
docker manifest inspect temporalio/base-builder:1.3.0
```
to verify that published base image was built for two architectures.

### Run base docker images for `linux/arm64` on `linux/amd64`

Run [qemu-user-static](https://github.com/multiarch/qemu-user-static) first:
```bash
docker run --rm --privileged multiarch/qemu-user-static --reset -p yes
```

Then run base image. For example:
```bash
docker run --rm -it --platform linux/arm64 temporalio/base-server:1.1.0 uname -m
```

### Troubleshooting
1. If there is an error during build for docker-x like this:
   ```text
   error: failed to solve: process "/dev/.buildkit_qemu_emulator /bin/sh -c apk add --update --no-cache     ca-certificates     tzdata     bash     curl     vim     jq     mysql-client     postgresql-client     python2     && curl https://bootstrap.pypa.io/pip/2.7/get-pip.py | python     && pip install cqlsh" did not complete successfully: exit code: 1
   ```
   run `docker run --rm --privileged linuxkit/binfmt:v0.8` first.
2. In case of this error:
   ```text
   #5 2.181 (4/31) Installing bash (5.1.0-r0)
   #5 2.370 Executing bash-5.1.0-r0.post-install
   #5 2.380 ERROR: bash-5.1.0-r0.post-install: script exited with error 1
   ```
   run:
   ```bash
   docker run --rm --privileged multiarch/qemu-user-static --reset -p yes
   docker buildx rm builder-x
   make docker-buildx-container
   docker buildx inspect --bootstrap
   ```
