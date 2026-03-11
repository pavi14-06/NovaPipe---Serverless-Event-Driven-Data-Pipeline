#!/usr/bin/env bash
# ============================================================
# NovaPipe — Local Development Environment Setup
# Starts Docker services + applies schema + creates Kafka topics
# Usage: ./scripts/setup_local.sh
# ============================================================

set -euo pipefail

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
RESET='\033[0m'

log()     { echo -e "${BLUE}[$(date '+%H:%M:%S')]${RESET} $*"; }
success() { echo -e "${GREEN}✅ $*${RESET}"; }
warn()    { echo -e "${YELLOW}⚠️  $*${RESET}"; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

echo -e "\n${BOLD}${BLUE}════ NovaPipe Local Dev Setup ════${RESET}\n"

# ─── Prerequisites ────────────────────────────────────────────────────────────
command -v docker      &>/dev/null || { echo "Docker not found"; exit 1; }
command -v python3     &>/dev/null || { echo "Python3 not found"; exit 1; }

# ─── .env Setup ───────────────────────────────────────────────────────────────
if [[ ! -f "$ROOT_DIR/.env" ]]; then
  log "Creating .env from template..."
  cp "$ROOT_DIR/.env.example" "$ROOT_DIR/.env"
  warn ".env created — edit it with your config before deploying to AWS"
fi

# ─── Python Dependencies ──────────────────────────────────────────────────────
log "Installing Python dependencies..."
pip3 install \
  kafka-python \
  cassandra-driver \
  boto3 \
  pytest \
  pytest-mock \
  --quiet

success "Python packages installed"

# ─── Start Docker Services ────────────────────────────────────────────────────
log "Starting Docker services (Kafka + Cassandra + LocalStack)..."
cd "$ROOT_DIR/docker"
docker compose up -d --remove-orphans

# ─── Wait for Kafka ───────────────────────────────────────────────────────────
log "Waiting for Kafka to be ready..."
MAX_WAIT=60
ELAPSED=0
until docker exec novapipe-kafka \
  kafka-broker-api-versions --bootstrap-server localhost:9092 &>/dev/null; do
  sleep 3
  ELAPSED=$((ELAPSED+3))
  [[ $ELAPSED -ge $MAX_WAIT ]] && { echo "Kafka timeout"; exit 1; }
  echo -n "."
done
echo ""
success "Kafka is ready"

# ─── Create Kafka Topics ──────────────────────────────────────────────────────
log "Creating Kafka topics..."
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export PIPELINE_ENV="development"
python3 "$ROOT_DIR/kafka/topics.py" create
success "Kafka topics created"

# ─── Wait for Cassandra ───────────────────────────────────────────────────────
log "Waiting for Cassandra to be ready..."
ELAPSED=0
until docker exec novapipe-cassandra cqlsh -e "describe keyspaces" &>/dev/null; do
  sleep 5
  ELAPSED=$((ELAPSED+5))
  [[ $ELAPSED -ge 120 ]] && { echo "Cassandra timeout"; exit 1; }
  echo -n "."
done
echo ""
success "Cassandra is ready"

# ─── Apply Schema ─────────────────────────────────────────────────────────────
log "Applying Cassandra schema..."
docker exec -i novapipe-cassandra \
  cqlsh < "$ROOT_DIR/cassandra/schema.cql" \
  && success "Cassandra schema applied"

# ─── Summary ──────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}════ Local Services Ready ════${RESET}"
echo -e "  Kafka:       ${GREEN}localhost:9092${RESET}"
echo -e "  Kafka UI:    ${GREEN}http://localhost:8080${RESET}"
echo -e "  Cassandra:   ${GREEN}localhost:9042${RESET}"
echo -e "  LocalStack:  ${GREEN}http://localhost:4566${RESET}"
echo ""
success "NovaPipe local environment is ready! 🎉"
echo ""
echo "  Run tests:      python -m pytest tests/ -v"
echo "  Stop services:  docker compose -f docker/docker-compose.yml down"
