Create new base docker images
=============================

To build a new version of base docker image run:

```bash
make <base_image_name> DOCKER_IMAGE_TAG=<new_base_image_version>
```

Check [MakeFile](Makefile) for all possible `base_image_name` options.

For example:

```bash
make base-builder DOCKER_IMAGE_TAG=1.3.0
```
