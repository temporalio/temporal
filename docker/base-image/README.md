Create new base docker images
=============================

To build new version of base docker images run:

```bash
$ make base-images DOCKER_IMAGE_TAG=<current_base_image_tag>
```

For example:

```bash
$ make base-images DOCKER_IMAGE_TAG=1.0.0
```
