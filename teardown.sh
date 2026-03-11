#!/usr/bin/env bash
# ============================================================
# NovaPipe — Teardown Script
# Destroys all AWS resources created by deploy.sh
# Usage: ./scripts/teardown.sh [--env staging] [--force]
# ============================================================

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
RESET='\033[0m'

log()     { echo -e "${BLUE}[$(date '+%H:%M:%S')]${RESET} $*"; }
success() { echo -e "${GREEN}✅ $*${RESET}"; }
warn()    { echo -e "${YELLOW}⚠️  $*${RESET}"; }
error()   { echo -e "${RED}❌ $*${RESET}" >&2; exit 1; }

PIPELINE_ENV="${PIPELINE_ENV:-production}"
FORCE=0
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TF_DIR="$(dirname "$SCRIPT_DIR")/terraform"

while [[ $# -gt 0 ]]; do
  case $1 in
    --env)   PIPELINE_ENV="$2"; shift 2 ;;
    --force) FORCE=1; shift ;;
    *) error "Unknown argument: $1" ;;
  esac
done

echo -e "\n${BOLD}${RED}════ NovaPipe TEARDOWN — env=${PIPELINE_ENV} ════${RESET}\n"
warn "This will DESTROY all NovaPipe infrastructure in '${PIPELINE_ENV}'!"
warn "Resources: Lambda, MSK Kafka, Keyspaces (Cassandra), S3, SQS, CloudWatch"
echo ""

if [[ "$FORCE" == "0" ]]; then
  read -rp "Type 'destroy-novapipe' to confirm: " CONFIRM
  [[ "$CONFIRM" == "destroy-novapipe" ]] || error "Teardown cancelled."
fi

# ─── Empty S3 Buckets First ───────────────────────────────────────────────────
log "Emptying S3 buckets..."
BUCKET_NAME=$(cd "$TF_DIR" && terraform output -raw s3_bucket_name 2>/dev/null || echo "")

if [[ -n "$BUCKET_NAME" ]]; then
  aws s3 rm "s3://${BUCKET_NAME}" --recursive --quiet && \
    log "Emptied s3://${BUCKET_NAME}"
fi

# ─── Terraform Destroy ────────────────────────────────────────────────────────
log "Running terraform destroy..."
cd "$TF_DIR"
terraform destroy \
  -var="pipeline_env=${PIPELINE_ENV}" \
  -auto-approve \
  -input=false

# ─── Clean Local Artifacts ───────────────────────────────────────────────────
log "Cleaning local build artifacts..."
find "$(dirname "$TF_DIR")/lambda" -name "*.zip" -delete 2>/dev/null || true
find "$(dirname "$TF_DIR")/lambda" -name "package" -type d -exec rm -rf {} + 2>/dev/null || true
rm -f "$TF_DIR/tfplan" 2>/dev/null || true

success "NovaPipe infrastructure destroyed 🧹"
