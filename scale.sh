#!/usr/bin/env bash
# ============================================================
# NovaPipe — Lambda Scaling Script
# Adjusts provisioned concurrency and reserved concurrency
# Usage: ./scripts/scale.sh --concurrency 50 [--env production]
# ============================================================

set -euo pipefail

BLUE='\033[0;34m'; GREEN='\033[0;32m'; RESET='\033[0m'; BOLD='\033[1m'
log()     { echo -e "${BLUE}[$(date '+%H:%M:%S')]${RESET} $*"; }
success() { echo -e "${GREEN}✅ $*${RESET}"; }

PIPELINE_ENV="${PIPELINE_ENV:-production}"
AWS_REGION="${AWS_REGION:-us-east-1}"
RESERVED_CONCURRENCY=100
PROVISIONED_CONCURRENCY=10

while [[ $# -gt 0 ]]; do
  case $1 in
    --env)           PIPELINE_ENV="$2";            shift 2 ;;
    --concurrency)   RESERVED_CONCURRENCY="$2";    shift 2 ;;
    --provisioned)   PROVISIONED_CONCURRENCY="$2"; shift 2 ;;
    --region)        AWS_REGION="$2";              shift 2 ;;
    *) echo "Unknown: $1"; exit 1 ;;
  esac
done

echo -e "\n${BOLD}${BLUE}════ NovaPipe Scaling — ${PIPELINE_ENV} ════${RESET}"
echo -e "Reserved concurrency:    ${RESERVED_CONCURRENCY}"
echo -e "Provisioned concurrency: ${PROVISIONED_CONCURRENCY}\n"

for FN in producer consumer transformer; do
  FN_NAME="novapipe-${FN}-${PIPELINE_ENV}"

  log "Scaling $FN_NAME..."

  # Set reserved concurrency
  aws lambda put-function-concurrency \
    --function-name "$FN_NAME" \
    --reserved-concurrent-executions "$RESERVED_CONCURRENCY" \
    --region "$AWS_REGION" \
    --output table

  # Get latest published version for provisioned concurrency
  LATEST_VERSION=$(aws lambda list-versions-by-function \
    --function-name "$FN_NAME" \
    --region "$AWS_REGION" \
    --query 'Versions[-1].Version' \
    --output text 2>/dev/null || echo "")

  if [[ -n "$LATEST_VERSION" && "$LATEST_VERSION" != "\$LATEST" ]]; then
    aws lambda put-provisioned-concurrency-config \
      --function-name "$FN_NAME" \
      --qualifier "$LATEST_VERSION" \
      --provisioned-concurrent-executions "$PROVISIONED_CONCURRENCY" \
      --region "$AWS_REGION" \
      --output table \
      2>/dev/null || log "Provisioned concurrency set via alias (if configured)"
  fi

  success "$FN_NAME scaled"
done

success "All Lambda functions scaled ⚡"
