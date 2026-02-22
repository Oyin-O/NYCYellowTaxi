#!/bin/bash
set -e

echo "Deploying NYC Taxi Pipeline..."

# Get account ID
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
STACK_NAME="${1:-sam-l}"

echo "Building..."
sam build

echo "Deploying stack: $STACK_NAME..."
sam deploy \
  --stack-name $STACK_NAME \
  --capabilities CAPABILITY_NAMED_IAM \
  --resolve-s3 \
  --no-confirm-changeset

echo "Uploading Glue script..."
aws s3 cp glue_scripts/bronze_to_silver_transformation.py s3://nyc-taxi-glue-scripts-${ACCOUNT_ID}/scripts/

aws s3 cp glue_scripts/silver_to_gold_finance.py \
  s3://nyc-taxi-glue-scripts-${ACCOUNT_ID}/scripts/

aws s3 cp glue_scripts/silver_to_gold_operations.py \
  s3://nyc-taxi-glue-scripts-${ACCOUNT_ID}/scripts/

echo "✅ Deployment complete!"
echo ""
echo "Test with: aws lambda invoke --function-name ${STACK_NAME}-bronze-ingest response.json"

echo "Test the pipeline"
aws stepfunctions start-execution \
  --state-machine-arn $(aws cloudformation describe-stacks \
    --stack-name nyc-taxi-pipeline \
    --query 'Stacks[0].Outputs[?OutputKey==`StateMachineArn`].OutputValue' \
    --output text)