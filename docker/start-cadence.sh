#!/bin/bash

dockerize -template /etc/cadence/config/config_template.yaml:/etc/cadence/config/docker.yaml cadence-server --root $CADENCE_HOME --env docker start --services=$SERVICES
