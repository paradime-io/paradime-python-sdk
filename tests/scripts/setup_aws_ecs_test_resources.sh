#!/bin/bash
set -euo pipefail

# =============================================================================
# AWS ECS Test Resources Setup Script for Paradime SDK
# =============================================================================
# Creates the minimal AWS resources to test the ECS integration commands:
#   1. ECS cluster
#   2. IAM execution role for Fargate
#   3. Two task definitions (one succeeds, one fails)
#
# Networking uses the account's default VPC and first available subnet.
#
# Prerequisites:
#   - AWS CLI v2 installed and configured
#   - IAM permissions: see aws_ecs_test_policy.json (paste as inline policy)
#
# Usage:
#   ./tests/scripts/setup_aws_ecs_test_resources.sh [REGION]
#
# Example:
#   ./tests/scripts/setup_aws_ecs_test_resources.sh us-east-1
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
echo "  Paradime SDK - AWS ECS Test Resources Setup"
echo "============================================================"
echo "  Region:          ${REGION}"
echo "  Cluster:         ${CLUSTER_NAME}"
echo "  Task Defs:       ${TASK_DEF_SUCCESS}, ${TASK_DEF_FAIL}"
echo "  Execution Role:  ${EXECUTION_ROLE_NAME}"
echo "============================================================"
echo ""

# ---------------------------------------------------------
# Step 1: Discover default VPC networking
# ---------------------------------------------------------
echo ">>> Step 1: Discovering default VPC networking..."

VPC_ID=$(aws ec2 describe-vpcs \
    --region "${REGION}" \
    --filters "Name=isDefault,Values=true" \
    --query 'Vpcs[0].VpcId' \
    --output text)

if [ -z "${VPC_ID}" ] || [ "${VPC_ID}" = "None" ]; then
    echo "    ERROR: No default VPC found in ${REGION}."
    echo "    Create one with: aws ec2 create-default-vpc --region ${REGION}"
    exit 1
fi
echo "    Default VPC: ${VPC_ID}"

SUBNET_ID=$(aws ec2 describe-subnets \
    --region "${REGION}" \
    --filters "Name=vpc-id,Values=${VPC_ID}" "Name=default-for-az,Values=true" \
    --query 'Subnets[0].SubnetId' \
    --output text)

if [ -z "${SUBNET_ID}" ] || [ "${SUBNET_ID}" = "None" ]; then
    echo "    ERROR: No default subnet found in default VPC."
    exit 1
fi
echo "    Default Subnet: ${SUBNET_ID}"

# Use the default security group
SG_ID=$(aws ec2 describe-security-groups \
    --region "${REGION}" \
    --filters "Name=vpc-id,Values=${VPC_ID}" "Name=group-name,Values=default" \
    --query 'SecurityGroups[0].GroupId' \
    --output text)

echo "    Default Security Group: ${SG_ID}"
echo "    ✅ Networking discovered."
echo ""

# ---------------------------------------------------------
# Step 2: Create ECS cluster
# ---------------------------------------------------------
echo ">>> Step 2: Creating ECS cluster..."

aws ecs create-cluster \
    --cluster-name "${CLUSTER_NAME}" \
    --region "${REGION}" \
    --tags key=paradime-test,value=true \
    --output text >/dev/null 2>&1 \
    || echo "    Cluster may already exist."

echo "    ✅ ECS cluster: ${CLUSTER_NAME}"
echo ""

# ---------------------------------------------------------
# Step 3: Create IAM execution role
# ---------------------------------------------------------
echo ">>> Step 3: Creating ECS task execution role..."

TRUST_POLICY='{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Service": "ecs-tasks.amazonaws.com" },
    "Action": "sts:AssumeRole"
  }]
}'

ROLE_ARN=$(aws iam create-role \
    --role-name "${EXECUTION_ROLE_NAME}" \
    --assume-role-policy-document "${TRUST_POLICY}" \
    --tags Key=paradime-test,Value=true \
    --query 'Role.Arn' \
    --output text 2>/dev/null) \
    || { ROLE_ARN=$(aws iam get-role \
            --role-name "${EXECUTION_ROLE_NAME}" \
            --query 'Role.Arn' \
            --output text); echo "    Role already exists: ${ROLE_ARN}"; }

aws iam attach-role-policy \
    --role-name "${EXECUTION_ROLE_NAME}" \
    --policy-arn "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy" \
    2>/dev/null || true

echo "    Execution Role ARN: ${ROLE_ARN}"
echo "    ✅ IAM role configured."
echo ""

# ---------------------------------------------------------
# Step 4: Register task definitions
# ---------------------------------------------------------
echo ">>> Step 4: Registering task definitions..."

# Task definition that succeeds (exit code 0)
cat > /tmp/paradime-ecs-success.json << JSONEOF
{
  "family": "${TASK_DEF_SUCCESS}",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "256",
  "memory": "512",
  "executionRoleArn": "${ROLE_ARN}",
  "containerDefinitions": [{
    "name": "main",
    "image": "alpine:latest",
    "command": ["sh", "-c", "echo 'Paradime ECS test succeeded!' && sleep 5 && exit 0"],
    "essential": true
  }]
}
JSONEOF

aws ecs register-task-definition \
    --cli-input-json file:///tmp/paradime-ecs-success.json \
    --region "${REGION}" \
    --tags key=paradime-test,value=true \
    --output text >/dev/null

echo "    ✅ Registered: ${TASK_DEF_SUCCESS}"

# Task definition that fails (exit code 1)
cat > /tmp/paradime-ecs-fail.json << JSONEOF
{
  "family": "${TASK_DEF_FAIL}",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "256",
  "memory": "512",
  "executionRoleArn": "${ROLE_ARN}",
  "containerDefinitions": [{
    "name": "main",
    "image": "alpine:latest",
    "command": ["sh", "-c", "echo 'Paradime ECS test — intentional failure' && exit 1"],
    "essential": true
  }]
}
JSONEOF

aws ecs register-task-definition \
    --cli-input-json file:///tmp/paradime-ecs-fail.json \
    --region "${REGION}" \
    --tags key=paradime-test,value=true \
    --output text >/dev/null

echo "    ✅ Registered: ${TASK_DEF_FAIL}"

rm -f /tmp/paradime-ecs-success.json /tmp/paradime-ecs-fail.json
echo ""

# ---------------------------------------------------------
# Step 5: Generate .env file
# ---------------------------------------------------------
echo ">>> Step 5: Generating .env file..."

cat > "${ENV_FILE}" << ENVEOF
# AWS ECS Test Environment Variables for Paradime SDK
# Generated on $(date -u +"%Y-%m-%dT%H:%M:%SZ")

AWS_REGION=${REGION}
ECS_CLUSTER=${CLUSTER_NAME}
ECS_TASK_DEF_SUCCESS=${TASK_DEF_SUCCESS}
ECS_TASK_DEF_FAIL=${TASK_DEF_FAIL}
ECS_SUBNET=${SUBNET_ID}
ECS_SECURITY_GROUP=${SG_ID}
ENVEOF

echo "    ✅ .env file saved to ${ENV_FILE}"
echo ""

# ---------------------------------------------------------
# Summary
# ---------------------------------------------------------
echo "============================================================"
echo "  SETUP COMPLETE"
echo "============================================================"
echo ""
echo "  Environment file: ${ENV_FILE}"
echo ""
echo "  Test commands:"
echo ""
echo "  # Load env vars"
echo "  export \$(cat ${ENV_FILE} | grep -v '^#' | xargs)"
echo ""
echo "  # List task definitions"
echo "  paradime run aws-ecs-list"
echo "  paradime run aws-ecs-list --json"
echo ""
echo "  # Trigger successful task"
echo "  paradime run aws-ecs-trigger \\"
echo "    --cluster \${ECS_CLUSTER} \\"
echo "    --task-definitions \${ECS_TASK_DEF_SUCCESS} \\"
echo "    --subnets \${ECS_SUBNET} \\"
echo "    --security-groups \${ECS_SECURITY_GROUP} \\"
echo "    --assign-public-ip"
echo ""
echo "  # Trigger failing task (should exit 1)"
echo "  paradime run aws-ecs-trigger \\"
echo "    --cluster \${ECS_CLUSTER} \\"
echo "    --task-definitions \${ECS_TASK_DEF_FAIL} \\"
echo "    --subnets \${ECS_SUBNET} \\"
echo "    --security-groups \${ECS_SECURITY_GROUP} \\"
echo "    --assign-public-ip"
echo ""
echo "  # Trigger multiple tasks concurrently"
echo "  paradime run aws-ecs-trigger \\"
echo "    --cluster \${ECS_CLUSTER} \\"
echo "    --task-definitions \${ECS_TASK_DEF_SUCCESS},\${ECS_TASK_DEF_FAIL} \\"
echo "    --subnets \${ECS_SUBNET} \\"
echo "    --security-groups \${ECS_SECURITY_GROUP} \\"
echo "    --assign-public-ip"
echo ""
echo "  # Fire-and-forget (no wait)"
echo "  paradime run aws-ecs-trigger \\"
echo "    --cluster \${ECS_CLUSTER} \\"
echo "    --task-definitions \${ECS_TASK_DEF_SUCCESS} \\"
echo "    --subnets \${ECS_SUBNET} \\"
echo "    --security-groups \${ECS_SECURITY_GROUP} \\"
echo "    --assign-public-ip --no-wait"
echo ""
echo "  # JSON output"
echo "  paradime run aws-ecs-trigger \\"
echo "    --cluster \${ECS_CLUSTER} \\"
echo "    --task-definitions \${ECS_TASK_DEF_SUCCESS} \\"
echo "    --subnets \${ECS_SUBNET} \\"
echo "    --security-groups \${ECS_SECURITY_GROUP} \\"
echo "    --assign-public-ip --json"
echo ""
echo "============================================================"
echo "  COST WARNING: Fargate tasks incur costs while running."
echo "  Run the teardown script when done:"
echo "    ./tests/scripts/teardown_aws_ecs_test_resources.sh ${REGION}"
echo "============================================================"
