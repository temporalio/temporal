Create new base docker images
=============================

To build a new version of base docker images run:

```bash
make all-images DOCKER_IMAGE_TAG=<new_base_image_version>
```

For example:

```bash
make all-images DOCKER_IMAGE_TAG=1.0.0
```
