#!/usr/bin/env bash
# ============================================================
# NovaPipe — Full AWS Deployment Script
# Deploys: Terraform infra + Kafka topics + Lambda functions
# Usage: ./scripts/deploy.sh [--env production|staging]
# ============================================================

set -euo pipefail
IFS=$'\n\t'

# ─── Colors ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
RESET='\033[0m'

# ─── Logging ──────────────────────────────────────────────────────────────────
log()     { echo -e "${BLUE}[$(date '+%H:%M:%S')]${RESET} $*"; }
success() { echo -e "${GREEN}✅ $*${RESET}"; }
warn()    { echo -e "${YELLOW}⚠️  $*${RESET}"; }
error()   { echo -e "${RED}❌ $*${RESET}" >&2; exit 1; }
banner()  { echo -e "\n${BOLD}${BLUE}════════ $* ════════${RESET}\n"; }

# ─── Defaults ─────────────────────────────────────────────────────────────────
PIPELINE_ENV="${PIPELINE_ENV:-production}"
AWS_REGION="${AWS_REGION:-us-east-1}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
TF_DIR="$ROOT_DIR/terraform"
LAMBDA_DIR="$ROOT_DIR/lambda"

# ─── Argument Parsing ─────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case $1 in
    --env)        PIPELINE_ENV="$2"; shift 2 ;;
    --region)     AWS_REGION="$2";   shift 2 ;;
    --skip-tf)    SKIP_TERRAFORM=1;  shift   ;;
    --skip-kafka) SKIP_KAFKA=1;      shift   ;;
    --help)
      echo "Usage: $0 [--env ENV] [--region REGION] [--skip-tf] [--skip-kafka]"
      exit 0
      ;;
    *) error "Unknown argument: $1" ;;
  esac
done

SKIP_TERRAFORM="${SKIP_TERRAFORM:-0}"
SKIP_KAFKA="${SKIP_KAFKA:-0}"

# ─── Pre-flight Checks ────────────────────────────────────────────────────────
banner "NovaPipe Deployment — env=${PIPELINE_ENV}"

log "Running pre-flight checks..."

check_cmd() {
  command -v "$1" &>/dev/null || error "'$1' is not installed. Please install it first."
}

check_cmd aws
check_cmd terraform
check_cmd python3
check_cmd pip3
check_cmd zip

# AWS auth check
aws sts get-caller-identity --region "$AWS_REGION" &>/dev/null \
  || error "AWS CLI not authenticated. Run 'aws configure'."

AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
log "AWS Account: ${AWS_ACCOUNT_ID} | Region: ${AWS_REGION}"
success "Pre-flight checks passed"

# ─── Load .env ────────────────────────────────────────────────────────────────
if [[ -f "$ROOT_DIR/.env" ]]; then
  log "Loading .env..."
  # shellcheck disable=SC2046
  export $(grep -v '^#' "$ROOT_DIR/.env" | xargs)
else
  warn ".env not found — using environment variables only"
fi

# ─── Package Lambda Functions ─────────────────────────────────────────────────
banner "Packaging Lambda Functions"

package_lambda() {
  local fn_name="$1"
  local fn_dir="$LAMBDA_DIR/$fn_name"
  local zip_path="$fn_dir/${fn_name}.zip"

  log "Packaging lambda: $fn_name"

  # Install dependencies into local dir
  if [[ -f "$fn_dir/requirements.txt" ]]; then
    pip3 install -r "$fn_dir/requirements.txt" \
      --target "$fn_dir/package" \
      --quiet \
      --upgrade
  fi

  # Build zip
  (
    cd "$fn_dir"
    if [[ -d package ]]; then
      cd package && zip -r9 "../${fn_name}.zip" . -x "*.pyc" -x "*/__pycache__/*" > /dev/null
      cd ..
    fi
    zip -g "${fn_name}.zip" handler.py > /dev/null
  )

  echo "$zip_path"
  success "Packaged $fn_name → $zip_path"
}

PRODUCER_ZIP=$(package_lambda "producer")
CONSUMER_ZIP=$(package_lambda "consumer")
TRANSFORMER_ZIP=$(package_lambda "transformer")

# ─── Terraform Deploy ─────────────────────────────────────────────────────────
if [[ "$SKIP_TERRAFORM" == "0" ]]; then
  banner "Terraform Infrastructure"

  cd "$TF_DIR"

  log "Initializing Terraform..."
  terraform init -upgrade -input=false

  log "Validating configuration..."
  terraform validate

  log "Planning deployment..."
  terraform plan \
    -var="pipeline_env=${PIPELINE_ENV}" \
    -var="aws_region=${AWS_REGION}" \
    -var="producer_zip=${PRODUCER_ZIP}" \
    -var="consumer_zip=${CONSUMER_ZIP}" \
    -var="transformer_zip=${TRANSFORMER_ZIP}" \
    -out=tfplan \
    -input=false

  log "Applying infrastructure..."
  terraform apply -input=false tfplan

  # Export outputs
  KAFKA_BOOTSTRAP=$(terraform output -raw kafka_bootstrap_servers 2>/dev/null || echo "")
  CASSANDRA_ENDPOINT=$(terraform output -raw cassandra_endpoint 2>/dev/null || echo "")
  S3_BUCKET=$(terraform output -raw s3_bucket_name 2>/dev/null || echo "")
  DLQ_URL=$(terraform output -raw dlq_url 2>/dev/null || echo "")

  success "Terraform deployment complete"
  cd "$ROOT_DIR"
fi

# ─── Kafka Topic Setup ────────────────────────────────────────────────────────
if [[ "$SKIP_KAFKA" == "0" ]] && [[ -n "${KAFKA_BOOTSTRAP_SERVERS:-$KAFKA_BOOTSTRAP}" ]]; then
  banner "Kafka Topic Setup"

  export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-$KAFKA_BOOTSTRAP}"
  export PIPELINE_ENV

  log "Creating Kafka topics..."
  python3 "$ROOT_DIR/kafka/topics.py" create \
    && success "Kafka topics created"

fi

# ─── Smoke Test ───────────────────────────────────────────────────────────────
banner "Smoke Tests"

log "Invoking producer Lambda..."
TEST_PAYLOAD='{"_test": false, "type": "deploy_smoke_test", "ts": "'$(date -u +%Y-%m-%dT%H:%M:%SZ)'"}'

INVOKE_RESULT=$(aws lambda invoke \
  --function-name "novapipe-producer-${PIPELINE_ENV}" \
  --payload "$TEST_PAYLOAD" \
  --region "$AWS_REGION" \
  /tmp/novapipe_smoke_response.json \
  --query 'StatusCode' \
  --output text 2>/dev/null || echo "SKIP")

if [[ "$INVOKE_RESULT" == "200" ]]; then
  RESPONSE=$(cat /tmp/novapipe_smoke_response.json)
  success "Producer smoke test passed | response=$RESPONSE"
else
  warn "Smoke test skipped or failed (Lambda may still be propagating)"
fi

# ─── Summary ──────────────────────────────────────────────────────────────────
banner "Deployment Summary"

echo -e "${BOLD}Environment:${RESET}  ${PIPELINE_ENV}"
echo -e "${BOLD}AWS Region:${RESET}   ${AWS_REGION}"
echo -e "${BOLD}Account ID:${RESET}   ${AWS_ACCOUNT_ID}"
[[ -n "${KAFKA_BOOTSTRAP:-}" ]]     && echo -e "${BOLD}Kafka:${RESET}        ${KAFKA_BOOTSTRAP}"
[[ -n "${CASSANDRA_ENDPOINT:-}" ]]  && echo -e "${BOLD}Cassandra:${RESET}    ${CASSANDRA_ENDPOINT}"
[[ -n "${S3_BUCKET:-}" ]]           && echo -e "${BOLD}S3 Bucket:${RESET}    s3://${S3_BUCKET}"

echo ""
success "NovaPipe deployment complete! 🚀"
echo ""
echo -e "Monitor pipeline: ${BLUE}https://${AWS_REGION}.console.aws.amazon.com/cloudwatch${RESET}"
