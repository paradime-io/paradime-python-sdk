# AWS Triggers Documentation

This package provides CLI commands to trigger AWS Lambda functions, SageMaker Pipelines, and Step Functions state machines, following the same pattern as the existing Fivetran and Airbyte integrations.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Authentication](#authentication)
- [AWS Lambda](#aws-lambda)
- [AWS SageMaker](#aws-sagemaker)
- [AWS Step Functions](#aws-step-functions)
- [Usage Examples](#usage-examples)
- [Error Handling](#error-handling)

## Prerequisites

Install the required AWS SDK:

```bash
pip install boto3
```

## Authentication

All AWS commands support authentication through environment variables. You can set these in your environment or pass them as command-line options:

### Required Environment Variables

```bash
export AWS_ACCESS_KEY_ID="your-access-key-id"
export AWS_SECRET_ACCESS_KEY="your-secret-access-key"
export AWS_DEFAULT_REGION="us-east-1"  # Optional, defaults to us-east-1
```

### Optional Environment Variables

```bash
export AWS_SESSION_TOKEN="your-session-token"  # For temporary credentials
```

### IAM Permissions

Ensure your AWS credentials have the appropriate permissions:

**Lambda:**
- `lambda:InvokeFunction`
- `lambda:ListFunctions`
- `lambda:GetFunction`

**SageMaker:**
- `sagemaker:StartPipelineExecution`
- `sagemaker:DescribePipelineExecution`
- `sagemaker:ListPipelines`

**Step Functions:**
- `states:StartExecution`
- `states:DescribeExecution`
- `states:ListStateMachines`

## AWS Lambda

### Invoke Lambda Functions

Trigger one or more Lambda functions with support for synchronous, asynchronous, and dry-run invocations.

#### Command

```bash
paradime run aws-lambda-invoke [OPTIONS]
```

#### Options

| Option | Environment Variable | Description | Required | Default |
|--------|---------------------|-------------|----------|---------|
| `--aws-access-key-id` | `AWS_ACCESS_KEY_ID` | AWS access key ID | No | - |
| `--aws-secret-access-key` | `AWS_SECRET_ACCESS_KEY` | AWS secret access key | No | - |
| `--aws-session-token` | `AWS_SESSION_TOKEN` | AWS session token (temporary credentials) | No | - |
| `--aws-region` | `AWS_DEFAULT_REGION` | AWS region name | No | us-east-1 |
| `--function-name` | - | Lambda function name or ARN (can specify multiple) | Yes | - |
| `--invocation-type` | - | Invocation type: RequestResponse, Event, or DryRun | No | RequestResponse |
| `--wait-for-completion` | - | Wait for async invocations to complete | No | True |
| `--timeout-minutes` | - | Max time to wait for completion (minutes) | No | 15 |

#### Invocation Types

- **RequestResponse** (Synchronous): Invoke function and wait for response
- **Event** (Asynchronous): Invoke function without waiting for response
- **DryRun**: Validate invocation without executing the function

#### Example Usage

```bash
# Invoke a single Lambda function (synchronous)
paradime run aws-lambda-invoke \
  --function-name my-lambda-function

# Invoke multiple Lambda functions
paradime run aws-lambda-invoke \
  --function-name function-1 \
  --function-name function-2 \
  --function-name function-3

# Asynchronous invocation
paradime run aws-lambda-invoke \
  --function-name my-lambda-function \
  --invocation-type Event

# Dry run (validation only)
paradime run aws-lambda-invoke \
  --function-name my-lambda-function \
  --invocation-type DryRun

# With explicit credentials
paradime run aws-lambda-invoke \
  --aws-access-key-id AKIAIOSFODNN7EXAMPLE \
  --aws-secret-access-key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
  --aws-region us-west-2 \
  --function-name my-lambda-function
```

### List Lambda Functions

List all Lambda functions in your AWS account.

#### Command

```bash
paradime run aws-lambda-list-functions [OPTIONS]
```

#### Example Usage

```bash
# List all Lambda functions
paradime run aws-lambda-list-functions

# List Lambda functions in a specific region
paradime run aws-lambda-list-functions --aws-region us-west-2
```

## AWS SageMaker

### Start SageMaker Pipeline

Trigger one or more SageMaker Pipeline executions.

#### Command

```bash
paradime run aws-sagemaker-start-pipeline [OPTIONS]
```

#### Options

| Option | Environment Variable | Description | Required | Default |
|--------|---------------------|-------------|----------|---------|
| `--aws-access-key-id` | `AWS_ACCESS_KEY_ID` | AWS access key ID | No | - |
| `--aws-secret-access-key` | `AWS_SECRET_ACCESS_KEY` | AWS secret access key | No | - |
| `--aws-session-token` | `AWS_SESSION_TOKEN` | AWS session token (temporary credentials) | No | - |
| `--aws-region` | `AWS_DEFAULT_REGION` | AWS region name | No | us-east-1 |
| `--pipeline-name` | - | SageMaker Pipeline name (can specify multiple) | Yes | - |
| `--wait-for-completion` | - | Wait for pipeline executions to complete | No | True |
| `--timeout-minutes` | - | Max time to wait for completion (minutes) | No | 1440 |

#### Example Usage

```bash
# Start a single SageMaker Pipeline
paradime run aws-sagemaker-start-pipeline \
  --pipeline-name my-ml-pipeline

# Start multiple pipelines
paradime run aws-sagemaker-start-pipeline \
  --pipeline-name training-pipeline \
  --pipeline-name inference-pipeline

# Start pipeline without waiting for completion
paradime run aws-sagemaker-start-pipeline \
  --pipeline-name my-ml-pipeline \
  --no-wait-for-completion

# With custom timeout
paradime run aws-sagemaker-start-pipeline \
  --pipeline-name my-ml-pipeline \
  --timeout-minutes 60
```

### List SageMaker Pipelines

List all SageMaker Pipelines in your AWS account.

#### Command

```bash
paradime run aws-sagemaker-list-pipelines [OPTIONS]
```

#### Example Usage

```bash
# List all SageMaker Pipelines
paradime run aws-sagemaker-list-pipelines

# List pipelines in a specific region
paradime run aws-sagemaker-list-pipelines --aws-region us-west-2
```

## AWS Step Functions

### Start Step Functions Execution

Trigger one or more AWS Step Functions state machine executions.

#### Command

```bash
paradime run aws-stepfunctions-start-execution [OPTIONS]
```

#### Options

| Option | Environment Variable | Description | Required | Default |
|--------|---------------------|-------------|----------|---------|
| `--aws-access-key-id` | `AWS_ACCESS_KEY_ID` | AWS access key ID | No | - |
| `--aws-secret-access-key` | `AWS_SECRET_ACCESS_KEY` | AWS secret access key | No | - |
| `--aws-session-token` | `AWS_SESSION_TOKEN` | AWS session token (temporary credentials) | No | - |
| `--aws-region` | `AWS_DEFAULT_REGION` | AWS region name | No | us-east-1 |
| `--state-machine-arn` | - | Step Functions state machine ARN (can specify multiple) | Yes | - |
| `--wait-for-completion` | - | Wait for executions to complete | No | True |
| `--timeout-minutes` | - | Max time to wait for completion (minutes) | No | 1440 |

#### Example Usage

```bash
# Start a single Step Functions execution
paradime run aws-stepfunctions-start-execution \
  --state-machine-arn arn:aws:states:us-east-1:123456789012:stateMachine:MyStateMachine

# Start multiple executions
paradime run aws-stepfunctions-start-execution \
  --state-machine-arn arn:aws:states:us-east-1:123456789012:stateMachine:StateMachine1 \
  --state-machine-arn arn:aws:states:us-east-1:123456789012:stateMachine:StateMachine2

# Start execution without waiting for completion
paradime run aws-stepfunctions-start-execution \
  --state-machine-arn arn:aws:states:us-east-1:123456789012:stateMachine:MyStateMachine \
  --no-wait-for-completion
```

### List Step Functions State Machines

List all Step Functions state machines in your AWS account.

#### Command

```bash
paradime run aws-stepfunctions-list-state-machines [OPTIONS]
```

#### Example Usage

```bash
# List all Step Functions state machines
paradime run aws-stepfunctions-list-state-machines

# List state machines in a specific region
paradime run aws-stepfunctions-list-state-machines --aws-region us-west-2
```

## Usage Examples

### Integration with Paradime Bolt Schedules

You can integrate these AWS triggers into your Paradime Bolt schedules:

```yaml
# .paradime/schedules/aws-workflow.yml
name: AWS ML Pipeline Workflow
schedule: "0 2 * * *"  # Daily at 2 AM
commands:
  - name: Trigger Lambda Data Prep
    command: |
      paradime run aws-lambda-invoke \
        --function-name data-preprocessing \
        --wait-for-completion

  - name: Start SageMaker Training
    command: |
      paradime run aws-sagemaker-start-pipeline \
        --pipeline-name ml-training-pipeline \
        --timeout-minutes 120

  - name: Execute Deployment Step Function
    command: |
      paradime run aws-stepfunctions-start-execution \
        --state-machine-arn arn:aws:states:us-east-1:123456789012:stateMachine:DeploymentWorkflow
```

### Parallel Execution

Trigger multiple services in parallel:

```bash
# In separate terminal windows or using a process manager
paradime run aws-lambda-invoke --function-name prep-function &
paradime run aws-sagemaker-start-pipeline --pipeline-name training-pipeline &
paradime run aws-stepfunctions-start-execution --state-machine-arn arn:aws:states:...:DeployWorkflow &
wait  # Wait for all background jobs to complete
```

### Using with Environment Variables

Create a `.env` file:

```bash
# .env
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
AWS_DEFAULT_REGION=us-east-1
```

Then run commands without explicit credentials:

```bash
source .env
paradime run aws-lambda-invoke --function-name my-function
```

## Error Handling

### Exit Codes

All commands follow the standard exit code convention:

- **0**: Success - All operations completed successfully
- **1**: Failure - One or more operations failed

### Common Error Scenarios

#### Authentication Errors

```
❌ AWS Error: InvalidClientTokenId
```

**Solution**: Verify your AWS credentials are correct and have not expired.

#### Permission Errors

```
❌ AWS Error: AccessDeniedException
```

**Solution**: Ensure your IAM user/role has the required permissions listed in the [IAM Permissions](#iam-permissions) section.

#### Resource Not Found

```
❌ AWS Error: ResourceNotFoundException
```

**Solution**: Verify the function name, pipeline name, or state machine ARN is correct and exists in the specified region.

#### Throttling

```
⚠️ THROTTLED (TooManyRequestsException)
```

**Solution**: AWS is rate-limiting your requests. Consider:
- Adding delays between invocations
- Requesting a service quota increase
- Reducing the number of concurrent executions

#### Timeout

```
Timeout waiting for execution to complete after X minutes
```

**Solution**: Increase the `--timeout-minutes` value or use `--no-wait-for-completion` if you don't need to wait for results.

## Progress Monitoring

All commands provide real-time progress updates with emojis and timestamps:

```
============================================================
🚀 TRIGGERING AWS LAMBDA FUNCTIONS
============================================================

[1/2] ⚡ data-preprocessing
----------------------------------------
09:15:32 🚀 [data-preprocessing] Invoking Lambda function...
09:15:32 📝 [data-preprocessing] Invocation type: RequestResponse
09:15:35 ✅ [data-preprocessing] Invocation successful

============================================================
⚡ LIVE PROGRESS
============================================================

09:15:36 🔄 [ml-training] Running... (0m 30s elapsed)
09:16:06 🔄 [ml-training] Running... (1m 0s elapsed)
09:16:45 ✅ [ml-training] Completed successfully (1m 39s)

============================================================
📊 INVOCATION RESULTS
============================================================
FUNCTION NAME                            STATUS
---------------------------------------- ---------------
data-preprocessing                       ✅ SUCCESS
ml-training                             ✅ SUCCESS
============================================================
```

## Logging

The commands use Python's logging module with INFO level by default. Logs include:

- Timestamps for all operations
- Status updates during long-running operations
- Detailed error messages with AWS error codes
- Resource ARNs and execution IDs for tracking

## Best Practices

1. **Use Environment Variables**: Store credentials in environment variables rather than passing them as command-line arguments
2. **Set Appropriate Timeouts**: Choose timeout values based on expected execution duration
3. **Monitor Costs**: Be aware that triggering AWS services incurs costs
4. **Use IAM Roles**: When running in AWS (EC2, ECS, Lambda), use IAM roles instead of access keys
5. **Enable CloudWatch**: Enable CloudWatch logging for better debugging and monitoring
6. **Tag Resources**: Tag your AWS resources for better organization and cost tracking
7. **Test with DryRun**: Use `--invocation-type DryRun` to validate Lambda invocations before execution

## Troubleshooting

### Debug Mode

Set the Python logging level to DEBUG for more detailed output:

```bash
export PYTHONLOGLEVEL=DEBUG
paradime run aws-lambda-invoke --function-name my-function
```

### Verify AWS Configuration

```bash
# Check AWS CLI configuration
aws sts get-caller-identity

# Test Lambda function exists
aws lambda get-function --function-name my-function

# Test SageMaker pipeline exists
aws sagemaker describe-pipeline --pipeline-name my-pipeline

# Test Step Functions state machine exists
aws stepfunctions describe-state-machine \
  --state-machine-arn arn:aws:states:us-east-1:123456789012:stateMachine:MyStateMachine
```

## Support

For issues or questions:
- Check the [AWS Documentation](https://docs.aws.amazon.com/)
- Review [Boto3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
- Contact your Paradime support team
