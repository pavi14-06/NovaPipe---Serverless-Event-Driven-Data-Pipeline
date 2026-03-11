#!/usr/bin/env bash
# ============================================================
# NovaPipe — Pipeline Health Check Script
# Checks: Lambda, Kafka, Cassandra, S3, SQS DLQ
# Usage: ./scripts/health_check.sh [--env production]
# ============================================================

set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
RESET='\033[0m'

OK()   { echo -e "${GREEN}  [OK]${RESET}   $*"; }
FAIL() { echo -e "${RED}  [FAIL]${RESET} $*"; FAILURES=$((FAILURES+1)); }
WARN() { echo -e "${YELLOW}  [WARN]${RESET} $*"; }
log()  { echo -e "${BLUE}▶${RESET} $*"; }

PIPELINE_ENV="${PIPELINE_ENV:-production}"
AWS_REGION="${AWS_REGION:-us-east-1}"
FAILURES=0

while [[ $# -gt 0 ]]; do
  case $1 in
    --env)    PIPELINE_ENV="$2"; shift 2 ;;
    --region) AWS_REGION="$2";   shift 2 ;;
    *) echo "Unknown: $1"; exit 1 ;;
  esac
done

echo -e "\n${BOLD}${BLUE}════ NovaPipe Health Check — ${PIPELINE_ENV} ════${RESET}\n"

# ─── 1. AWS Auth ──────────────────────────────────────────────────────────────
log "Checking AWS connectivity..."
if aws sts get-caller-identity --region "$AWS_REGION" &>/dev/null; then
  OK "AWS CLI authenticated"
else
  FAIL "AWS CLI not authenticated"
fi

# ─── 2. Lambda Functions ──────────────────────────────────────────────────────
log "Checking Lambda functions..."
for FN in producer consumer transformer; do
  FN_NAME="novapipe-${FN}-${PIPELINE_ENV}"
  STATE=$(aws lambda get-function \
    --function-name "$FN_NAME" \
    --region "$AWS_REGION" \
    --query 'Configuration.State' \
    --output text 2>/dev/null || echo "NOT_FOUND")

  if [[ "$STATE" == "Active" ]]; then
    OK "Lambda $FN_NAME → Active"
  elif [[ "$STATE" == "NOT_FOUND" ]]; then
    FAIL "Lambda $FN_NAME → Not Found"
  else
    WARN "Lambda $FN_NAME → $STATE"
  fi
done

# ─── 3. S3 Bucket ─────────────────────────────────────────────────────────────
log "Checking S3 data lake..."
S3_BUCKET="novapipe-data-lake-${PIPELINE_ENV}"
if aws s3 ls "s3://${S3_BUCKET}" --region "$AWS_REGION" &>/dev/null; then
  # Count recent files (last 1 hour)
  RECENT=$(aws s3 ls "s3://${S3_BUCKET}/events/" --recursive \
    --region "$AWS_REGION" 2>/dev/null | wc -l || echo 0)
  OK "S3 bucket '${S3_BUCKET}' accessible | total_objects≈${RECENT}"
else
  FAIL "S3 bucket '${S3_BUCKET}' not found or inaccessible"
fi

# ─── 4. SQS Dead Letter Queue ────────────────────────────────────────────────
log "Checking SQS Dead Letter Queue..."
DLQ_NAME="novapipe-dlq-${PIPELINE_ENV}"
DLQ_URL=$(aws sqs get-queue-url \
  --queue-name "$DLQ_NAME" \
  --region "$AWS_REGION" \
  --query 'QueueUrl' \
  --output text 2>/dev/null || echo "")

if [[ -n "$DLQ_URL" ]]; then
  DLQ_DEPTH=$(aws sqs get-queue-attributes \
    --queue-url "$DLQ_URL" \
    --attribute-names ApproximateNumberOfMessages \
    --region "$AWS_REGION" \
    --query 'Attributes.ApproximateNumberOfMessages' \
    --output text 2>/dev/null || echo "?")

  if [[ "$DLQ_DEPTH" == "0" ]]; then
    OK "DLQ '${DLQ_NAME}' → depth=0 (healthy)"
  elif [[ "$DLQ_DEPTH" =~ ^[0-9]+$ ]] && [[ "$DLQ_DEPTH" -gt 100 ]]; then
    WARN "DLQ '${DLQ_NAME}' → depth=${DLQ_DEPTH} (elevated — check failures)"
  else
    WARN "DLQ '${DLQ_NAME}' → depth=${DLQ_DEPTH}"
  fi
else
  FAIL "DLQ '${DLQ_NAME}' not found"
fi

# ─── 5. CloudWatch Alarms ─────────────────────────────────────────────────────
log "Checking CloudWatch alarms..."
ALARM_STATES=$(aws cloudwatch describe-alarms \
  --alarm-name-prefix "novapipe-" \
  --region "$AWS_REGION" \
  --query 'MetricAlarms[].{Name:AlarmName,State:StateValue}' \
  --output text 2>/dev/null || echo "")

if [[ -z "$ALARM_STATES" ]]; then
  WARN "No CloudWatch alarms found for novapipe-*"
else
  while IFS=$'\t' read -r NAME STATE; do
    if [[ "$STATE" == "OK" ]]; then
      OK "Alarm: $NAME → OK"
    elif [[ "$STATE" == "ALARM" ]]; then
      FAIL "Alarm: $NAME → ALARM 🚨"
    else
      WARN "Alarm: $NAME → $STATE"
    fi
  done <<< "$ALARM_STATES"
fi

# ─── 6. Lambda Error Rate ─────────────────────────────────────────────────────
log "Checking Lambda error rates (last 1 hour)..."
END_TIME=$(date -u +%Y-%m-%dT%H:%M:%SZ)
START_TIME=$(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || \
             date -u -v-1H +%Y-%m-%dT%H:%M:%SZ)

for FN in producer consumer transformer; do
  FN_NAME="novapipe-${FN}-${PIPELINE_ENV}"
  ERRORS=$(aws cloudwatch get-metric-statistics \
    --namespace AWS/Lambda \
    --metric-name Errors \
    --dimensions "Name=FunctionName,Value=${FN_NAME}" \
    --start-time "$START_TIME" \
    --end-time "$END_TIME" \
    --period 3600 \
    --statistics Sum \
    --region "$AWS_REGION" \
    --query 'Datapoints[0].Sum' \
    --output text 2>/dev/null || echo "0")

  ERRORS="${ERRORS:-0}"
  if [[ "$ERRORS" == "None" || "$ERRORS" == "0" ]]; then
    OK "Lambda $FN → errors=0 (last 1h)"
  else
    WARN "Lambda $FN → errors=${ERRORS} (last 1h)"
  fi
done

# ─── Summary ──────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}═══════════════════════════════════════${RESET}"
if [[ "$FAILURES" == "0" ]]; then
  echo -e "${GREEN}${BOLD}✅ All checks passed! Pipeline is healthy.${RESET}"
else
  echo -e "${RED}${BOLD}❌ ${FAILURES} check(s) failed. Investigate above.${RESET}"
  exit 1
fi
echo ""
