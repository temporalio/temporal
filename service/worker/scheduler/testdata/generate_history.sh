#!/usr/bin/env bash
#
# Consider running this script to generate a new history for TestReplays
# whenever there's some change to the scheduler workflow.
# To use it, run a local server (any backend) and run this script.

set -x

id=sched1

# shellcheck disable=SC2064
trap "temporal schedule delete -s '$id'" EXIT

temporal schedule create -s "$id" \
  --overlap-policy bufferall \
  --interval 10s \
  --jitter 8s \
  -w mywf \
  -t mytq \
  --workflow-type mywf \
  --execution-timeout 5

sleep 50 # ~5 normal actions, some may be buffered

# backfill 3 actions
temporal schedule backfill -s "$id"  \
  --overlap-policy allowall  \
  --start-time 2022-05-09T11:22:22Z  \
  --end-time   2022-05-09T11:22:55Z

sleep 22 # another 2-3 normal actions

# trigger a couple (will definitely buffer)
temporal schedule trigger -s sched1
sleep 3
temporal schedule trigger -s sched1

# pause
temporal schedule toggle -s sched1 --pause --reason testing
sleep 21
temporal schedule toggle -s sched1 --unpause --reason testing

# update
temporal schedule update -s "$id"  \
  --calendar '{"hour":"*","minute":"*","second":"*/5"}' \
  --remaining-actions 1 \
  -w mywf \
  -t mytq \
  --workflow-type mywf \
  --execution-timeout 3

sleep 12
# should have used one action and be idle now

# capture history
now=$(date +%s)
temporal workflow show -w "temporal-sys-scheduler:$id" -o json | gzip -9c > "replay_$now.json.gz"
