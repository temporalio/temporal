#!/usr/bin/env bash
#
# Consider running this script to generate a new history for TestReplays
# whenever there's some change to the worker-deployment or worker-deployment-version workflow.
# To use it, run a local server (any backend) with the dynamic config for worker-versioning 3
# (system.enableDeploymentVersions=true) enabled and run this script.
#
# Note: this requires temporal cli >= 0.12 and sdk >= v1.33.0

deploymentName="foo"
version="1.0"

# Create the default namespace
temporal operator namespace create default

# Run the worker which shall start the deployment entity workflows....
echo "Running the Go program..."
timeout 10s go run "$(dirname "$0")/worker/worker.go"

# Download the history for the worker deployment workflow...
now=$(date +%s)
temporal workflow show -w "temporal-sys-worker-deployment:$deploymentName" --output json | gzip -9c > "$(dirname "$0")/testdata/replay_worker_deployment_wf_$now.json.gz"

# Download the history for the worker deployment version workflow...
temporal workflow show -w "temporal-sys-worker-deployment-version:$deploymentName.$version" --output json | gzip -9c > "$(dirname "$0")/testdata/replay_worker_deployment_version_wf_$now.json.gz"


