#!/bin/bash
set -euo pipefail

# =============================================================================
# AWS ECS Test Resources Teardown Script for Paradime SDK
# =============================================================================
# Cleans up resources created by setup_aws_ecs_test_resources.sh.
# Does NOT touch the default VPC/subnet/security group (those are shared).
#
# Usage:
#   ./tests/scripts/teardown_aws_ecs_test_resources.sh [REGION]
#
# Example:
#   ./tests/scripts/teardown_aws_ecs_test_resources.sh eu-west-2
# =============================================================================

REGION="${1:-eu-west-2}"
PREFIX="paradime-test"
CLUSTER_NAME="${PREFIX}-ecs-cluster"
TASK_DEF_SUCCESS="${PREFIX}-ecs-success"
TASK_DEF_FAIL="${PREFIX}-ecs-fail"
EXECUTION_ROLE_NAME="${PREFIX}-ecs-execution-role"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ENV_FILE="${REPO_ROOT}/.env.aws-ecs-test"

echo "============================================================"
echo "  Paradime SDK - AWS ECS Test Resources Teardown"
echo "============================================================"
echo "  Region: ${REGION}"
echo "============================================================"
echo ""
echo "  This will DELETE:"
echo "    - ECS cluster: ${CLUSTER_NAME}"
echo "    - Task definitions: ${TASK_DEF_SUCCESS}, ${TASK_DEF_FAIL}"
echo "    - IAM role: ${EXECUTION_ROLE_NAME}"
echo "    - Local .env file: ${ENV_FILE}"
echo ""
echo "  Default VPC, subnets, and security groups are NOT touched."
echo ""
read -p "  Are you sure? (y/N) " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "  Aborted."
    exit 0
fi
echo ""

# ---------------------------------------------------------
# Stop any running tasks in the cluster
# ---------------------------------------------------------
echo ">>> Stopping running tasks in cluster..."
RUNNING_TASKS=$(aws ecs list-tasks \
    --cluster "${CLUSTER_NAME}" \
    --desired-status RUNNING \
    --region "${REGION}" \
    --query 'taskArns' \
    --output text 2>/dev/null || echo "")

if [ -n "${RUNNING_TASKS}" ] && [ "${RUNNING_TASKS}" != "None" ]; then
    for TASK_ARN in ${RUNNING_TASKS}; do
        echo "    Stopping task: ${TASK_ARN}"
        aws ecs stop-task \
            --cluster "${CLUSTER_NAME}" \
            --task "${TASK_ARN}" \
            --region "${REGION}" \
            --output text >/dev/null 2>&1 || true
    done
    echo "    Waiting 10s for tasks to stop..."
    sleep 10
else
    echo "    No running tasks found."
fi

# ---------------------------------------------------------
# Delete ECS cluster
# ---------------------------------------------------------
echo ">>> Deleting ECS cluster..."
aws ecs delete-cluster \
    --cluster "${CLUSTER_NAME}" \
    --region "${REGION}" \
    --output text >/dev/null 2>&1 \
    && echo "    ✅ Deleted: ${CLUSTER_NAME}" \
    || echo "    ⚠️ Not found or already deleted."

# ---------------------------------------------------------
# Deregister and delete task definitions
# ---------------------------------------------------------
echo ">>> Deregistering task definitions..."

for FAMILY in "${TASK_DEF_SUCCESS}" "${TASK_DEF_FAIL}"; do
    REVISIONS=$(aws ecs list-task-definitions \
        --family-prefix "${FAMILY}" \
        --region "${REGION}" \
        --query 'taskDefinitionArns' \
        --output text 2>/dev/null || echo "")

    if [ -n "${REVISIONS}" ] && [ "${REVISIONS}" != "None" ]; then
        for REV_ARN in ${REVISIONS}; do
            aws ecs deregister-task-definition \
                --task-definition "${REV_ARN}" \
                --region "${REGION}" \
                --output text >/dev/null 2>&1 || true
            aws ecs delete-task-definitions \
                --task-definitions "${REV_ARN}" \
                --region "${REGION}" \
                --output text >/dev/null 2>&1 || true
        done
        echo "    ✅ Removed: ${FAMILY}"
    else
        echo "    ⚠️ No revisions found for: ${FAMILY}"
    fi
done

# ---------------------------------------------------------
# Delete IAM role
# ---------------------------------------------------------
echo ">>> Deleting IAM execution role..."

aws iam detach-role-policy \
    --role-name "${EXECUTION_ROLE_NAME}" \
    --policy-arn "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy" \
    2>/dev/null || true

aws iam delete-role \
    --role-name "${EXECUTION_ROLE_NAME}" \
    2>/dev/null \
    && echo "    ✅ Deleted: ${EXECUTION_ROLE_NAME}" \
    || echo "    ⚠️ Not found or already deleted."

# ---------------------------------------------------------
# Clean up local files
# ---------------------------------------------------------
echo ">>> Cleaning up local files..."
if [ -f "${ENV_FILE}" ]; then
    rm "${ENV_FILE}"
    echo "    ✅ Deleted ${ENV_FILE}"
else
    echo "    ⚠️ ${ENV_FILE} not found."
fi

echo ""
echo "============================================================"
echo "  TEARDOWN COMPLETE"
echo "============================================================"
