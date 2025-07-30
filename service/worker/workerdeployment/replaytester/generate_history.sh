#!/bin/bash

deploymentName="foo"
version="1.0"

# Create the default namespace
temporal operator namespace create default

# Run the worker which shall start the deployment entity workflows....
echo "Running the Go program..."
go run "$(dirname "$0")/worker/worker.go"

echo "Waiting 5 seconds for all workflows to show up in visibility..."
sleep 5

# Function to download all workflow runs in CAN chain
download_workflow_chain() {
    local workflow_id=$1
    local workflow_name=$2
    local workflow_type=$3
    local timestamp=$4
    local run_dir=$5
    
    echo "📥 Downloading all executions for: $workflow_id"
    
    # Use the working query method with TemporalNamespaceDivision
    echo "   Getting the chain of CAN runs for this workflow using the TemporalNamespaceDivision query..."
    run_ids=$(temporal workflow list \
        --query "TemporalNamespaceDivision = \"TemporalWorkerDeployment\" AND WorkflowType = \"$workflow_type\"" \
        --output json | \
        jq -r '.[] | .execution.runId')
    
    # Count how many we found
    run_count=$(echo "$run_ids" | wc -l | tr -d ' ')
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
            
            temporal workflow show \
                -w "$workflow_id" \
                -r "$run_id" \
                --output json | \
                tee "$run_dir/replay_${workflow_name}_run_${run_id}.json" | \
                gzip -9c > "$run_dir/replay_${workflow_name}_run_${run_id}.json.gz"
            
            ((run_index++))
        fi
    done
    
    # # Save run IDs for reference
    # echo "$run_ids" > "$run_dir/${workflow_name}_all_runs.txt"
    
    echo "   ✅ Downloaded $run_index executions for $workflow_name"
}

# Create timestamped run directory 
now=$(date +%s)
run_dir="$(dirname "$0")/testdata/run_$now"
mkdir -p "$run_dir"

echo "📁 Creating run directory: $run_dir"

# Download all executions for both workflow types
download_workflow_chain "temporal-sys-worker-deployment:$deploymentName" "worker_deployment_wf" "temporal-sys-worker-deployment-workflow" "$now" "$run_dir"
download_workflow_chain "temporal-sys-worker-deployment-version:$deploymentName:$version" "worker_deployment_version_wf" "temporal-sys-worker-deployment-version-workflow" "$now" "$run_dir"

echo ""
echo "🎉 Complete! All workflow execution histories downloaded to $run_dir"
echo ""
echo "📊 Summary for this run:"
echo "   📂 Run directory: $run_dir"

# Count files by workflow type
deployment_files=$(ls -1 "$run_dir"/replay_worker_deployment_wf_*.json.gz 2>/dev/null | wc -l | tr -d ' ')
version_files=$(ls -1 "$run_dir"/replay_worker_deployment_version_wf_*.json.gz 2>/dev/null | wc -l | tr -d ' ')
total_files=$(ls -1 "$run_dir"/replay_*.json.gz 2>/dev/null | wc -l | tr -d ' ')

echo "   Worker Deployment workflows: $deployment_files executions"
echo "   Worker Version workflows: $version_files executions"



