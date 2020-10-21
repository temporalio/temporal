#!/bin/bash

set -ex

dockerize -template /etc/temporal/config/config_template.yaml:/etc/temporal/config/docker.yaml

# Convert semicolon (or comma for backward compatibility) separated string (i.e. "history:matching")
# to valid flags list (i.e. "--services=history --services=matching").
IFS=':,' read -ra SERVICES_LIST <<< "$SERVICES"
SERVICES_ARGS=$(printf -- "--services=%s " "${SERVICES_LIST[@]}")

exec temporal-server --root $TEMPORAL_HOME --env docker start $SERVICES_ARGS
