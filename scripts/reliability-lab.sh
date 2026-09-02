#!/usr/bin/env bash
set -euo pipefail

# All PostgreSQL state belongs to this new container; Redis tests create their
# own loopback processes and temporary directories. No existing service is reset.
cd "$(dirname "$0")/.."
command -v docker >/dev/null
command -v "${LAB_REDIS_SERVER:-redis-server}" >/dev/null
lab_container="im-reliability-$(date +%s)-$$"
cleanup() { docker rm -f -v "$lab_container" >/dev/null 2>&1 || true; }
trap cleanup EXIT
docker run -d --name "$lab_container" -e POSTGRES_HOST_AUTH_METHOD=trust \
    -e POSTGRES_DB=reliability_lab -p 127.0.0.1::5432 postgres:17-alpine >/dev/null
for ((i=0; i<60; i++)); do
    if docker exec "$lab_container" pg_isready -U postgres -d reliability_lab >/dev/null 2>&1; then break; fi
    sleep 0.2
done
docker exec "$lab_container" pg_isready -U postgres -d reliability_lab
lab_port=$(docker port "$lab_container" 5432/tcp | awk -F: '{print $NF}')
export LAB_DATABASE_URL="postgres://postgres@127.0.0.1:${lab_port}/reliability_lab?sslmode=disable"
export RUN_RELIABILITY_LAB=1
"${LAB_REDIS_SERVER:-redis-server}" --version
docker exec "$lab_container" postgres --version
go test -race -count=1 -timeout=5m -v ./internal/reliabilitylab "$@"
DATABASE_URL="$LAB_DATABASE_URL" go run ./cmd/im-migrate up
TEST_DATABASE_URL="$LAB_DATABASE_URL" go test -race -count=1 -timeout=2m -v . \
    -run '^(TestOutboxRealProcessExpiredOwner|TestTwoOutboxWorkersClaimDisjointBatches|TestExpiredLeaseCanBeReclaimedAndOldOwnerCannotComplete)$'
