#!/bin/bash

set -ex

dockerize -template /etc/temporal/config/config_template.yaml:/etc/temporal/config/docker.yaml

exec temporal-server --root $TEMPORAL_HOME --env docker start --services=$SERVICES
