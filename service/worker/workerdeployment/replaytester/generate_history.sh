#!/usr/bin/env bash
#
# Consider running this script to generate a new history for TestReplays
# whenever there's some change to the worker-deployment or worker-deployment-version workflow.
# To use it, run a local server (any backend) and ensure the following dynamic configs are enabled 
# in the dynamic config file (config/dynamicconfig/development-sql.yaml):
#
# matching.deploymentWorkflowVersion={WORKFLOW_VERSION}
# matching.PollerHistoryTTL=1s
# matching.wv.VersionDrainageStatusVisibilityGracePeriod=5s
# matching.wv.VersionDrainageStatusRefreshInterval=5s
# workercontroller.enabled=true
#
# workercontroller.enabled is required by the compute-config section of worker/worker.go
# (exerciseComputeConfig)
#
# Make sure you set deploymentWorkflowVersion correctly. It should be an integer instead of {WORKFLOW_VERSION} above.
# If deploymentWorkflowVersion is set to i, run this script with v{i} as the argument. The argument instructs the script
# where to put the history files under `testdata` folder. The workflow version that server uses and the testdata folder
# must match in order for the tests to run against correct workflow versions.

# Example: set `matching.deploymentWorkflowVersion=1` and run this script as below:
# ./generate_history.sh v1
#
# With above config and command, the replay histories will be generated using workflow version
# 1 (AsyncSetCurrentAndRamping) and saved in testdata/v1 folder.
#
# Note: this requires temporal cli >= 0.12 and sdk >= v1.33.0

deploymentName="foo"
version="1.0"
# The compute-config-only version (created by exerciseComputeConfig in worker/worker.go)
computeConfigVersion="2.0"

# Expected workflow counts - users can override these if their changes are expected to generate more workflows which will be true when a breaking change to 
# these workflows is introduced.
# These values are used by the replay tester to validate that your workflow changes haven't accidentally created additional executions.
EXPECTED_DEPLOYMENT_WORKFLOWS=${EXPECTED_DEPLOYMENT_WORKFLOWS:-13}
EXPECTED_VERSION_WORKFLOWS=${EXPECTED_VERSION_WORKFLOWS:-14}

echo "📋 Expected workflow counts:"
echo "   Deployment workflows: $EXPECTED_DEPLOYMENT_WORKFLOWS"
echo "   Version workflows: $EXPECTED_VERSION_WORKFLOWS"
echo "   (Override with EXPECTED_DEPLOYMENT_WORKFLOWS=X EXPECTED_VERSION_WORKFLOWS=Y if you expect different counts)"
echo ""

# Create the default namespace
temporal operator namespace create default

# Run the worker which shall start the deployment entity workflows....
echo "Running the Go program..."

if ! go run "$(dirname "$0")/worker/worker.go"; then
  echo "Go program exited with an error. Exiting bash script." >&2
  exit 1
fi

echo "Go program completed successfully."

echo "Waiting 5 seconds for all workflows to show up in visibility..."
sleep 5

# Function to download all workflow runs in CAN chain
download_workflow_chain() {
    local workflow_id=$1
    local workflow_name=$2
    local workflow_type=$3
    local run_dir=$4
    
    echo "📥 Downloading all executions for: $workflow_id"

    # Use the working query method with TemporalNamespaceDivision
    echo "   Getting the chain of CAN runs for this workflow using the TemporalNamespaceDivision query..."
    run_ids=$(temporal workflow list \
        --query "TemporalNamespaceDivision = \"TemporalWorkerDeployment\" AND WorkflowType = \"$workflow_type\" AND WorkflowId = \"$workflow_id\"" \
        --output json | \
        jq -r '.[] | .execution.runId')

    # Count how many we found
    if [ -z "$run_ids" ]; then
        run_count=0
    else
        run_count=$(echo "$run_ids" | wc -l | tr -d ' ')
    fi
    echo "   Found $run_count executions"
    
    if [ "$run_count" -eq 0 ]; then
        echo "   No executions found for $workflow_id"
        return
    fi
    
    # Download each execution
    run_index=0
    for run_id in $run_ids; do
        if [ -n "$run_id" ]; then
            echo "   Downloading run $((run_index + 1))/$run_count: $run_id"
            
            # Write to a plain file first so the exit status is `workflow show`'s rather than
            # gzip's, then check it is non-empty.
            json_file="$run_dir/replay_${workflow_name}_run_${run_id}.json"
            if ! temporal workflow show -w "$workflow_id" -r "$run_id" --output json > "$json_file"; then
                echo "   Failed to download history for $workflow_id run $run_id" >&2
                exit 1
            fi
            if [ ! -s "$json_file" ]; then
                echo "   Empty history downloaded for $workflow_id run $run_id" >&2
                exit 1
            fi
            gzip -9c < "$json_file" > "$json_file.gz"
            rm -f "$json_file"

            ((run_index++))
        fi
    done
    
    # # Save run IDs for reference
    # echo "$run_ids" > "$run_dir/${workflow_name}_all_runs.txt"
    
    echo "   ✅ Downloaded $run_index executions for $workflow_name"
}

# Create timestamped run directory 
now=$(date +%s)
run_dir="$(dirname "$0")/testdata/$1/run_$now"
mkdir -p "$run_dir"

echo "📁 Creating run directory: $run_dir"

# Download all executions for both workflow types
download_workflow_chain "temporal-sys-worker-deployment:$deploymentName" "worker_deployment_wf" "temporal-sys-worker-deployment-workflow" "$run_dir"
download_workflow_chain "temporal-sys-worker-deployment-version:$deploymentName:$version" "worker_deployment_version_wf" "temporal-sys-worker-deployment-version-workflow" "$run_dir"
download_workflow_chain "temporal-sys-worker-deployment-version:$deploymentName:$computeConfigVersion" "worker_deployment_version_wf" "temporal-sys-worker-deployment-version-workflow" "$run_dir"

echo ""
echo "🎉 Complete! All workflow execution histories downloaded to $run_dir"
echo ""
echo "📊 Summary for this run:"
echo "   📂 Run directory: $run_dir"

# Count files by workflow type
deployment_files=$(find "$run_dir" -name "replay_worker_deployment_wf_*.json.gz" 2>/dev/null | wc -l | tr -d ' ')
version_files=$(find "$run_dir" -name "replay_worker_deployment_version_wf_*.json.gz" 2>/dev/null | wc -l | tr -d ' ')

echo "   Worker Deployment workflows: $deployment_files executions"
echo "   Worker Version workflows: $version_files executions"

# Save expected counts to a file for the replay tester to read
cat > "$run_dir/expected_counts.txt" << EOF
# Expected workflow counts for replay testing
# Generated by generate_history.sh on $(date)
EXPECTED_DEPLOYMENT_WORKFLOWS=$EXPECTED_DEPLOYMENT_WORKFLOWS
EXPECTED_VERSION_WORKFLOWS=$EXPECTED_VERSION_WORKFLOWS
ACTUAL_DEPLOYMENT_WORKFLOWS=$deployment_files
ACTUAL_VERSION_WORKFLOWS=$version_files
EOF

echo "   📝 Expected counts saved to: $run_dir/expected_counts.txt"

# A run whose actual counts don't match the expected ones would be checked in with an
# expected_counts.txt that fails TestReplays immediately, so fail here instead.
if [ "$deployment_files" -ne "$EXPECTED_DEPLOYMENT_WORKFLOWS" ] || [ "$version_files" -ne "$EXPECTED_VERSION_WORKFLOWS" ]; then
  echo "" >&2
  echo "❌ Workflow counts do not match the expected values." >&2
  echo "   Expected: Deployment=$EXPECTED_DEPLOYMENT_WORKFLOWS, Version=$EXPECTED_VERSION_WORKFLOWS" >&2
  echo "   Actual:   Deployment=$deployment_files, Version=$version_files" >&2
  echo "" >&2
  echo "   If the new counts are correct for your change, re-run with:" >&2
  echo "     EXPECTED_DEPLOYMENT_WORKFLOWS=$deployment_files EXPECTED_VERSION_WORKFLOWS=$version_files $0 $1" >&2
  echo "   Otherwise your change created extra workflow executions; investigate before checking in." >&2
  exit 1
fi
