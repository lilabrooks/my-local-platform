#!/usr/bin/env bash
# Seed local AWS resources in floci and topics in Kafka.
#
# Idempotent: safe to re-run. Everything it creates is namespaced `mlp-`.
set -euo pipefail

ENDPOINT="${AWS_ENDPOINT_URL:-http://localhost:4566}"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"

# Local creds only. These never touch a real AWS account -- floci accepts any
# key. AWS_PROFILE is cleared so a real SSO profile can't be picked up here.
unset AWS_PROFILE || true
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="$REGION"

aws_local() { aws --endpoint-url "$ENDPOINT" "$@"; }

say() { printf '\033[1;34m==>\033[0m %s\n' "$*"; }

say "waiting for floci at $ENDPOINT"
for i in $(seq 1 30); do
  if curl -sf "$ENDPOINT/_floci/health" >/dev/null 2>&1; then break; fi
  [ "$i" = 30 ] && { echo "floci did not become healthy" >&2; exit 1; }
  sleep 1
done

# --- S3 ---------------------------------------------------------------------
say "S3: bucket mlp-artifacts"
aws_local s3api create-bucket --bucket mlp-artifacts >/dev/null 2>&1 || true
aws_local s3api put-bucket-versioning \
  --bucket mlp-artifacts --versioning-configuration Status=Enabled >/dev/null

# --- SNS + SQS fanout -------------------------------------------------------
say "SNS: topic mlp-events"
TOPIC_ARN=$(aws_local sns create-topic --name mlp-events --query TopicArn --output text)

say "SQS: queue mlp-events-q subscribed to mlp-events"
QUEUE_URL=$(aws_local sqs create-queue --queue-name mlp-events-q --query QueueUrl --output text)
QUEUE_ARN=$(aws_local sqs get-queue-attributes --queue-url "$QUEUE_URL" \
  --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)

# Subscribing twice would create a duplicate subscription, so check first.
EXISTING=$(aws_local sns list-subscriptions-by-topic --topic-arn "$TOPIC_ARN" \
  --query "Subscriptions[?Endpoint=='$QUEUE_ARN'].SubscriptionArn" --output text 2>/dev/null || true)
if [ -z "$EXISTING" ] || [ "$EXISTING" = "None" ]; then
  aws_local sns subscribe --topic-arn "$TOPIC_ARN" --protocol sqs \
    --notification-endpoint "$QUEUE_ARN" --attributes RawMessageDelivery=true >/dev/null
fi

# --- SES --------------------------------------------------------------------
say "SES: verify platform@localhost.test"
aws_local ses verify-email-identity --email-address platform@localhost.test >/dev/null 2>&1 || true

# --- Secrets Manager --------------------------------------------------------
say "SecretsManager: mlp/db"
aws_local secretsmanager create-secret --name mlp/db \
  --secret-string '{"username":"platform","password":"platform"}' >/dev/null 2>&1 || true

say "AWS surface ready"
echo "    topic : $TOPIC_ARN"
echo "    queue : $QUEUE_URL"
